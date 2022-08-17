# import asyncio
import uuid
from random import randbytes
from time import time_ns

import httpx
import requests


def get_put(base_url: str, num_operation: int, byte_size: int):
    put_total, get_total, del_total = 0, 0, 0
    data = randbytes(byte_size)
    for i in range(num_operation):
        key = uuid.uuid4().hex
        url = base_url + "/" + key

        put_st = time_ns()
        requests.put(url, data=data)
        put_ed = time_ns()

        get_st = time_ns()
        response = requests.get(url)
        get_ed = time_ns()

        assert response.status_code == 200
        assert response.content == data

        del_st = time_ns()
        requests.delete(url)
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
    base_url = "http://localhost:8108"
    async with httpx.AsyncClient(http2=True) as client:
        await a_get_put(client, base_url, 10, (1 << 15) - 1)


def main():
    base_url = "http://localhost:8108"
    get_put(base_url, 10, 2 << 15)


if __name__ == "__main__":
    main()
    # asyncio.run(amain())
