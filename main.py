import asyncio
import datetime
import gzip
import hashlib
import io
import logging
import logging.config
import os
import pickle
import re
import sqlite3
import time
import uuid
from typing import Dict, Optional

import aiofiles
import aiofiles.os
import yaml
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, Counter, Gauge, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator as PrometheusInstrumentator

import diskcache

app = FastAPI()


class SizeDifferentException(Exception):
    pass


class ValueSizeLimitExceeded(Exception):
    pass


# Define the filter
class EndpointFilter(logging.Filter):
    xp = re.compile("/-/.*/")

    def filter(self, record: logging.LogRecord) -> bool:
        return record.args and len(record.args) >= 3 and (not self.xp.match(record.args[2]))  # type: ignore


# Add filter to the logger
logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

DATETIME_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"


def create_tag_from_request(request: Request):
    tag: Dict[str, str] = {
        "Last-Modified": datetime.datetime.now(tz=datetime.timezone.utc).strftime(DATETIME_FORMAT),
    }

    if request.headers.get("x-set-cache-control"):
        tag["Cache-Control"] = request.headers["x-set-cache-control"]

    if request.headers.get("content-type"):
        tag["Content-Type"] = request.headers["content-type"]

    if request.headers.get("content-encoding"):
        tag["Content-Encoding"] = request.headers["content-encoding"]
    return tag


class CustomDisk(diskcache.Disk):
    def fetch(self, mode, filename, value, read):
        if mode == diskcache.core.MODE_RAW:
            return bytes(value) if type(value) is sqlite3.Binary else value

        return aiofiles.open(os.path.join(self._directory, filename), "rb")

    async def _astore_raw(self, value, content_length: int):
        byte_data, size = b"", 0
        async for chunk in value:
            if not chunk:
                break
            size += len(chunk)
            byte_data += chunk
            if size > content_length:
                raise SizeDifferentException()
        if size != content_length:
            raise SizeDifferentException()
        return (
            size,
            diskcache.core.MODE_RAW,
            None,
            sqlite3.Binary(byte_data),
            hashlib.sha256(byte_data).hexdigest(),
        )

    async def astore(self, value, read, key=diskcache.UNKNOWN, content_length: Optional[int] = None):
        assert read

        # 小さいものはDBに格納
        if (content_length is not None) and content_length < self.min_file_size:
            return await self._astore_raw(value=value, content_length=content_length)

        filename, full_path = self.filename(key, value)
        await aiofiles.os.makedirs(os.path.split(full_path)[0], exist_ok=True)
        hasher = hashlib.sha256()

        size = 0
        async with aiofiles.open(full_path, "xb") as f:
            async for chunk in value:
                if not chunk:
                    break
                size += len(chunk)
                if size > VALUE_SIZE_LIMIT:
                    raise ValueSizeLimitExceeded()
                hasher.update(chunk)
                await f.write(chunk)

        if (content_length is not None) and size != content_length:
            raise SizeDifferentException()
        return size, diskcache.core.MODE_BINARY, filename, None, hasher.hexdigest()


class CustomCache(diskcache.Cache):
    _disk: CustomDisk

    async def aset(
        self,
        key,
        value,
        *,
        expire=None,
        read=False,
        tag: Dict[str, str],
        retry=False,
        content_length: Optional[int] = None,
    ):
        assert read
        size, mode, filename, db_value, digest = await self._disk.astore(
            value, read, key=key, content_length=content_length
        )
        tag["Content-Length"] = str(size)
        tag["Etag"] = digest
        await asyncio.to_thread(
            self._aset_transaction,
            key=key,
            expire=expire,
            tag=pickle.dumps(tag),
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
    sqlite_mmap_size=1 << 27,  # 128mb
    sqlite_synchronous=1,  # NORMAL
    disk_min_file_size=1 << 15,  # 32kb
    disk_pickle_protocol=pickle.HIGHEST_PROTOCOL,
)
_cache.stats(enable=True)

_cache_hits = Counter("diskcache_cache_hits", "num cache hits")
_cache_misses = Counter("diskcache_cache_misses", "num cache misses")
_cache_len = Gauge("diskcache_cache_len", "Count of items in cache including expired items")
_cache_volume = Gauge("diskcache_cache_volume", "total size of cache on disk")

DEBUG = eval(os.environ.get("DEBUG", "False"))
VALUE_SIZE_LIMIT = eval(os.environ.get("VALUE_SIZE_LIMIT", "300 << 20"))
DEFAULT_EXPIRE = eval(os.environ.get("DEFAULT_EXPIRE", "24 * 60 * 60"))  # 1day
RESPONSE_CHUNK_SIZE = 1 << 18
PUT_TIMEOUT = eval(os.environ.get("REQUEST_TIMEOUT", "3 * 60"))  # 3min

EXPIRE_HEADER = "x-diskcache-expire"


async def read_all(opened_file):
    if isinstance(opened_file, bytes):
        yield opened_file
    else:
        async with opened_file as fp:
            while chunk := await fp.read(RESPONSE_CHUNK_SIZE):
                yield chunk


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


@app.get("/{name:path}")
async def get_raw(name: str, request: Request) -> Response:
    value, _expire, _tag = await asyncio.to_thread(_cache.get, name, read=True, expire_time=True, tag=True)

    expire = datetime.datetime.fromtimestamp(_expire, datetime.timezone.utc) if _expire else None
    tag = pickle.loads(_tag) if _tag else {}
    headers = dict(**tag)
    if expire:
        headers["Expire"] = expire.strftime(DATETIME_FORMAT)

    if value is None:
        raise HTTPException(status_code=404)
    elif tag.get("Etag", "no-etag-found") == request.headers.get("If-None-Match"):
        return Response(status_code=304, headers=headers)

    return StreamingResponse(read_all(value), headers=headers)


@app.put("/{name:path}")
async def put_raw(name: str, request: Request) -> Response:
    if name.startswith("-/"):
        return Response(content="path must not start with '-/'", status_code=400)

    expire = int(request.headers.get(EXPIRE_HEADER, DEFAULT_EXPIRE))
    try:
        if "content-length" in request.headers:
            content_length = int(request.headers["content-length"])
        else:
            content_length = None
    except ValueError:
        return Response(content="invalid Content-Length value", status_code=400)

    try:
        await _cache.aset(
            name,
            request.stream(),
            expire=expire,
            read=True,
            tag=create_tag_from_request(request),
            content_length=content_length,
        )
    except ValueSizeLimitExceeded:
        return Response(content="size limit exceeded", status_code=400)
    except SizeDifferentException:
        return Response(content="content-length different", status_code=400)

    return Response(status_code=200)


@app.delete("/{name:path}")
def delete_content(name: str) -> Response:
    if name.startswith("-/"):
        return Response(content="path must not start with '-/'", status_code=400)

    if not _cache.delete(name):
        return Response(status_code=404)
    return Response(status_code=200)


PrometheusInstrumentator().instrument(app)

logging_config = yaml.safe_load(os.environ.get("LOGGING_CONFIG", ""))
if logging_config:
    logging.config.dictConfig(logging_config)

if DEBUG:
    import tracemalloc

    tracemalloc.start()

    @app.get("/-/tracemalloc/")
    def get_tracemalloc():
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")
        sio = io.StringIO()
        for stat in top_stats[:50]:
            print(stat, file=sio)

        return Response(sio.getvalue())


@app.get("/-/gc/")
def run_gc():
    import gc

    sio = io.StringIO()
    print(f"gc.isenabled()={gc.isenabled()}", file=sio)
    print(f"gc.collect()={gc.collect()}", file=sio)
    return Response(sio.getvalue())


@app.get("/-/closecon/")
def close_db_connection():
    _cache.close()
    return Response("close db")
