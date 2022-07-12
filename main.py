import asyncio
import logging
import os
import pickle
import uuid
import time
import aiofiles
import aiofiles.os
from queue import Empty, Full, Queue
from typing import IO, AsyncGenerator, Iterable

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

import diskcache

app = FastAPI()


class ValueSizeLimitExceeded(Exception):
    pass


# Define the filter
class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.args and len(record.args) >= 3 and record.args[2] != "/-/healthcheck/"  # type: ignore


# Add filter to the logger
logging.getLogger("uvicorn.access").addFilter(EndpointFilter())


class CustomDisk(diskcache.Disk):
    def fetch(self, mode, filename, value, read):
        return aiofiles.open(os.path.join(self._directory, filename), "rb")

    async def astore(self, value, read, key=diskcache.UNKNOWN):
        assert read

        filename, full_path = self.filename(key, value)
        await aiofiles.os.makedirs(os.path.split(full_path)[0], exist_ok=True)

        size = 0
        async with aiofiles.open(full_path, "xb") as f:
            async for chunk in value:
                if not chunk:
                    break
                size += len(chunk)
                await f.write(chunk)
        return size, diskcache.core.MODE_BINARY, filename, None


class CustomCache(diskcache.Cache):
    _disk: CustomDisk

    async def aset(self, key, value, expire=None, read=False, tag=None, retry=False):
        assert read
        size, mode, filename, db_value = await self._disk.astore(value, read, key=key)
        await asyncio.to_thread(
            self._aset_transaction,
            key=key,
            expire=expire,
            tag=tag,
            retry=retry,
            size=size,
            mode=mode,
            filename=filename,
            db_value=db_value,
        )

    def _aset_transaction(self, *, key, expire, tag, retry, size, mode, filename, db_value):
        now = time.time()
        expire_time = None if expire is None else now + expire
        db_key, raw = self._disk.put(key)
        columns = (expire_time, tag, size, mode, filename, db_value)
        with self._transact(retry, filename) as (sql, cleanup):
            rows = sql(
                "SELECT rowid, filename FROM Cache" " WHERE key = ? AND raw = ?",
                (db_key, raw),
            ).fetchall()

            if rows:
                ((rowid, old_filename),) = rows
                cleanup(old_filename)
                self._row_update(rowid, now, columns)
            else:
                self._row_insert(db_key, raw, now, columns)

            self._cull(now, sql, cleanup)

            return True


_cache = CustomCache(
    directory=os.environ.get("CACHE_DIRECTORY", "/tmp"),
    timeout=60,
    disk=CustomDisk,
    statistics=0,  # False
    tag_index=0,  # False
    # least-recently-stored, least-recently-used, least-frequently-used
    eviction_policy=os.environ.get("EVICTION_POLICY", "least-recently-stored"),
    size_limit=eval(os.environ.get("CACHE_SIZE_LIMIT", "8 << 30")),
    cull_limit=10,
    sqlite_auto_vacuum=1,  # FULL
    sqlite_cache_size=2 << 13,  # 8,192 pages
    sqlite_journal_mode="wal",
    sqlite_mmap_size=2 << 26,  # 64mb
    sqlite_synchronous=1,  # NORMAL
    disk_min_file_size=2 << 15,  # 32kb
    disk_pickle_protocol=pickle.HIGHEST_PROTOCOL,
)

VALUE_SIZE_LIMIT = eval(os.environ.get("VALUE_SIZE_LIMIT", "300 << 20"))
DEFAULT_EXPIRE = eval(os.environ.get("DEFAULT_EXPIRE", "24 * 60 * 60"))  # 1day
RESPONSE_CHUNK_SIZE = 4 << 20
PUT_TIMEOUT = eval(os.environ.get("REQUEST_TIMEOUT", "3 * 60"))  # 3min


def file_iterator(fp: IO[bytes]) -> Iterable[bytes]:
    while data := fp.read(RESPONSE_CHUNK_SIZE):
        yield data
    if hasattr(fp, "close"):
        fp.close()


@app.get("/{name}")
async def get_raw(name: str) -> Response:
    # reader = _cache.get(name, read=True)
    # if reader is None:
    #     raise HTTPException(status_code=404)
    # return StreamingResponse(file_iterator(reader))
    return StreamingResponse(await _cache.get(name, read=True))


class AsyncGeneratorReader:
    _queue: Queue[bytes]

    def __init__(self, loop: asyncio.AbstractEventLoop, generator: AsyncGenerator[bytes, None]):
        self._loop = loop
        self._generator = generator
        self._event = asyncio.Event()
        self._queue = Queue(maxsize=10)

    async def _async_queue_put(self, data: bytes, max_try_cnt: int = 100000, tick_tock: float = 0.01):
        for i in range(max_try_cnt):
            if self._event.is_set():
                raise Exception("request already exitted")
            try:
                # awaitしないとthreadを占有してしまう
                print(">>> waiting put", self._queue)
                await asyncio.to_thread(self._queue.put, data, timeout=tick_tock)
            except Full:
                print(">>> put failed", self._queue)
                pass
            else:
                print(">>> put complete", self._queue)
                break

    async def read_request(self):
        print("read request start", self._queue)
        size = 0
        try:
            async for chunk in self._generator:
                print(">>> reading chunk", len(chunk), self._queue)
                size += len(chunk)
                if size > VALUE_SIZE_LIMIT:
                    await self._async_queue_put(b"")
                    raise ValueSizeLimitExceeded()

                await self._async_queue_put(chunk)
        finally:
            await asyncio.to_thread(self._queue.join)
        print("read request end", self._queue)

    def read(self, bytes: int) -> bytes:
        print("reading content", self._queue)
        try:
            while not self._event.is_set():
                try:
                    print("<<< wait queue", self._queue)
                    return self._queue.get(timeout=1)
                except Empty:
                    print("<<< queue get failed", self._queue)
                    pass
        finally:
            print("queue task done", self._queue)
            self._queue.task_done()
        return b""


@app.put("/{name}")
async def put_raw(name: str, request: Request) -> Response:
    # event_loop = asyncio.get_event_loop()
    # reader = AsyncGeneratorReader(event_loop, request.stream())
    # try:
    #     await asyncio.wait_for(
    #         asyncio.gather(
    #             reader.read_request(),
    #             asyncio.to_thread(lambda: _cache.set(name, reader, expire=DEFAULT_EXPIRE, read=True)),
    #         ),
    #         timeout=PUT_TIMEOUT,
    #     )
    # except asyncio.exceptions.TimeoutError:
    #     raise HTTPException(status_code=503, detail="process timeout")
    # except ValueSizeLimitExceeded:
    #     raise HTTPException(status_code=403, detail="size limit exceeded")
    # finally:
    #     reader._event.set()
    await _cache.aset(name, request.stream(), expire=DEFAULT_EXPIRE, read=True)
    return Response(status_code=200)


@app.delete("/{name}")
def delete_content(name: str) -> Response:
    if not _cache.delete(name):
        return Response(status_code=404)
    return Response(status_code=200)


@app.post("/-/flushall/")
def clear_all_content():
    return Response(status_code=200, content=f"clear: {_cache.clear()}")


@app.get("/-/healthcheck/")
def healthcheck():
    key = uuid.uuid4().hex
    _cache.set(key, key)
    _cache.get(key)
    _cache.delete(key)
    return Response(status_code=200)
