# import asyncio
# import random
import uuid
from concurrent.futures import ThreadPoolExecutor
from random import randbytes
from time import time_ns

import httpx
import requests
from requests.adapters import HTTPAdapter, Retry


def get_put(session, base_url: str, num_operation: int, byte_size: int):
    put_total, get_total, del_total = 0, 0, 0
    data = randbytes(byte_size)
    url = ""
    for i in range(num_operation):
        key = uuid.uuid4().hex
        url = base_url + "/" + key

        put_st = time_ns()
        response = session.put(url, data=data)
        if response.status_code != 200:
            print("Error")
            break
        put_ed = time_ns()

        get_st = time_ns()
        response = session.get(url)
        get_ed = time_ns()

        if response.status_code != 200:
            print("Error")
            break

        if response.content != data:
            print("content error")
            break

        del_st = time_ns()
        session.delete(url)
        del_ed = time_ns()

        put_total += put_ed - put_st
        get_total += get_ed - get_st
        del_total += del_ed - del_st

    print(
        f"put:{(put_total / num_operation) * 1.0e-6}[msec], "
        f"get:{(get_total / num_operation) * 1.0e-6}[msec], "
        f"del:{(del_total / num_operation) * 1.0e-6}[msec]"
    )


async def a_get_put(client: httpx.AsyncClient, base_url: str, num_operation: int, byte_size: int):
    put_total, get_total, del_total = 0, 0, 0
    data = randbytes(byte_size)
    for i in range(num_operation):
        key = uuid.uuid4().hex
        url = base_url + "/" + key

        put_st = time_ns()
        await client.put(url, data=data)
        put_ed = time_ns()

        get_st = time_ns()
        response = await client.get(url)
        get_ed = time_ns()
        print(response.http_version)
        print(response.headers)
        assert response.status_code == 200
        assert response.content == data

        del_st = time_ns()
        await client.delete(url)
        del_ed = time_ns()

        put_total += put_ed - put_st
        get_total += get_ed - get_st
        del_total += del_ed - del_st

    print(
        f"put:{(put_total / num_operation) * 1.0e-6}[msec], "
        f"get:{(get_total / num_operation) * 1.0e-6}[msec], "
        f"del:{(del_total / num_operation) * 1.0e-6}[msec]"
    )


async def amain():
    base_url = "http://localhost:8000"
    async with httpx.AsyncClient(http2=True) as client:
        await a_get_put(client, base_url, 10, (1 << 14) - 1)


def main():
    session = requests.Session()
    adapter = HTTPAdapter(
        max_retries=Retry(total=10, backoff_factor=0.5, status_forcelist=[502, 503, 504]),
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    base_url = "http://localhost:8000"
    with ThreadPoolExecutor(max_workers=32) as pool:
        for _ in range(1):
            # pool.submit(get_put, base_url, 100, random.randint(1<<10, 10<<20))
            pool.submit(get_put, session, base_url, 1000, 10 << 20)


if __name__ == "__main__":
    main()
    # asyncio.run(amain())
