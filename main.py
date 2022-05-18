import asyncio
import os
import pickle

from fastapi import FastAPI, Request, Response

import diskcache

app = FastAPI()


_cache = diskcache.Cache(
    directory=os.environ.get("CACHE_DIRECTORY", "/tmp"),
    timeout=60,
    statistics=0,  # False
    tag_index=0,  # False
    eviction_policy="least-recently-stored",
    size_limit=int(os.environ.get("CACHE_SIZE_LIMIT", 4 << 30)),  # 最大4gb
    cull_limit=10,
    sqlite_auto_vacuum=1,  # FULL
    sqlite_cache_size=2 << 13,  # 8,192 pages
    sqlite_journal_mode="wal",
    sqlite_mmap_size=2 << 26,  # 64mb
    sqlite_synchronous=1,  # NORMAL
    disk_min_file_size=2 << 15,  # 32kb
    disk_pickle_protocol=pickle.HIGHEST_PROTOCOL,
)

VALUE_SIZE_LIMIT = int(os.environ.get("VALUE_SIZE_LIMIT", 100 << 20))
DEFAULT_EXPIRE = int(os.environ.get("DEFAULT_EXPIRE", 24 * 60 * 60))  # 1day


@app.get("/{name}")
def get_raw(name: str) -> Response:
    try:
        return Response(content=_cache[name], status_code=200)
    except KeyError:
        return Response(content="Not Found", status_code=404)


@app.put("/{name}")
async def put_raw(name: str, request: Request) -> Response:
    data = await request.body()
    if len(data) > VALUE_SIZE_LIMIT:
        return Response(content="size exceeded", status_code=400)
    await asyncio.to_thread(lambda: _cache.set(name, data, expire=DEFAULT_EXPIRE))
    return Response(status_code=200)
