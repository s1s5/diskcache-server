import asyncio
import os
import pickle
import threading
from queue import Queue
from typing import IO, AsyncGenerator, Iterable

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse

import diskcache

app = FastAPI()


class ValueSizeLimitExceeded(Exception):
    pass


# class CustomCache(diskcache.Cache):
#     def __init__(self, directory=None, timeout=60, disk=diskcache.Disk, **settings):
#         self._sql_retry("CREATE TABLE IF NOT EXISTS Settings ( key TEXT NOT NULL UNIQUE, value LONG)")
#         super().__init__(directory, timeout, disk, **settings)


_cache = diskcache.Cache(
    directory=os.environ.get("CACHE_DIRECTORY", "/tmp"),
    timeout=60,
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
def get_raw(name: str) -> Response:
    reader = _cache.get(name, read=True)
    if reader is None:
        raise HTTPException(status_code=404)
    return StreamingResponse(file_iterator(reader))


class AsyncGeneratorReader:
    _queue: Queue[bytes]

    def __init__(self, loop: asyncio.AbstractEventLoop, generator: AsyncGenerator[bytes, None]):
        self._loop = loop
        self._generator = generator
        self._event = threading.Event()
        self._queue = Queue(maxsize=10)

    async def read_request(self):
        size = 0
        try:
            async for chunk in self._generator:
                size += len(chunk)
                if size > VALUE_SIZE_LIMIT:
                    await asyncio.to_thread(self._queue.put, b"")
                    raise ValueSizeLimitExceeded()
                await asyncio.to_thread(self._queue.put, chunk)
        finally:
            await asyncio.to_thread(self._queue.join)

    def read(self, bytes: int) -> bytes:
        try:
            return self._queue.get()
        finally:
            self._queue.task_done()


@app.put("/{name}")
async def put_raw(name: str, request: Request) -> Response:
    event_loop = asyncio.get_event_loop()
    reader = AsyncGeneratorReader(event_loop, request.stream())
    try:
        await asyncio.wait_for(
            asyncio.gather(
                reader.read_request(),
                asyncio.to_thread(lambda: _cache.set(name, reader, expire=DEFAULT_EXPIRE, read=True)),
            ),
            timeout=PUT_TIMEOUT,
        )
    except ValueSizeLimitExceeded:
        raise HTTPException(status_code=403, detail="size limit exceeded")
    return Response(status_code=200)


@app.delete("/{name}")
def delete_content(name: str) -> Response:
    if not _cache.delete(name):
        return Response(status_code=404)
    return Response(status_code=200)
