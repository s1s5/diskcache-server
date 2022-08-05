import asyncio
import gzip
import io
import logging
import os
import pickle
import re
import time
import uuid

import aiofiles
import aiofiles.os
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, Counter, Gauge, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator as PrometheusInstrumentator

import diskcache

app = FastAPI()


class ValueSizeLimitExceeded(Exception):
    pass


# Define the filter
class EndpointFilter(logging.Filter):
    xp = re.compile("/-/.*/")

    def filter(self, record: logging.LogRecord) -> bool:
        return record.args and len(record.args) >= 3 and (not self.xp.match(record.args[2]))  # type: ignore


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
                if size > VALUE_SIZE_LIMIT:
                    raise ValueSizeLimitExceeded()
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
_cache.stats(enable=True)

_cache_hits = Counter("diskcache_cache_hits", "num cache hits")
_cache_misses = Counter("diskcache_cache_misses", "num cache misses")
_cache_len = Gauge("diskcache_cache_len", "Count of items in cache including expired items")
_cache_volume = Gauge("diskcache_cache_volume", "total size of cache on disk")

VALUE_SIZE_LIMIT = eval(os.environ.get("VALUE_SIZE_LIMIT", "300 << 20"))
DEFAULT_EXPIRE = eval(os.environ.get("DEFAULT_EXPIRE", "24 * 60 * 60"))  # 1day
RESPONSE_CHUNK_SIZE = 4 << 20
PUT_TIMEOUT = eval(os.environ.get("REQUEST_TIMEOUT", "3 * 60"))  # 3min

EXPIRE_HEADER = "x-diskcache-expire"


async def read_all(opened_file):
    async with opened_file as fp:
        while chunk := await fp.read(1 << 18):
            yield chunk


@app.get("/{name}")
async def get_raw(name: str) -> Response:
    value = _cache.get(name, read=True)
    if value is None:
        raise HTTPException(status_code=404)
    return StreamingResponse(read_all(value))


@app.put("/{name}")
async def put_raw(name: str, request: Request) -> Response:
    expire = int(request.headers.get(EXPIRE_HEADER, DEFAULT_EXPIRE))
    await _cache.aset(name, request.stream(), expire=expire, read=True)
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
    _cache.set(key, io.BytesIO(key.encode("ascii")), read=True)
    _cache.get(key)
    _cache.delete(key)
    return Response(status_code=200)


@app.get("/-/metrics/")
def metrics(request: Request):
    hits, misses = _cache.stats(enable=True, reset=True)
    _cache_hits.inc(hits)
    _cache_misses.inc(misses)
    _cache_len.set(len(_cache))
    _cache_volume.set(_cache.volume())

    if "gzip" in request.headers.get("Accept-Encoding", ""):
        resp = Response(content=gzip.compress(generate_latest(REGISTRY)))
        resp.headers["Content-Type"] = CONTENT_TYPE_LATEST
        resp.headers["Content-Encoding"] = "gzip"
        return resp
    else:
        resp = Response(content=generate_latest(REGISTRY))
        resp.headers["Content-Type"] = CONTENT_TYPE_LATEST
    return resp


PrometheusInstrumentator().instrument(app)
