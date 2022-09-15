import gzip
import hashlib
import tempfile
from unittest import mock

import pytest
from fastapi.testclient import TestClient

from main import CustomCache, CustomDisk, app

client = TestClient(app)


@pytest.fixture
def patch_cache():
    with tempfile.TemporaryDirectory() as td, mock.patch(
        "main._cache", CustomCache(directory=td, disk=CustomDisk, disk_min_file_size=1)
    ) as _cache:
        yield _cache


@pytest.fixture
def patch_cache_raw():
    with tempfile.TemporaryDirectory() as td, mock.patch(
        "main._cache", CustomCache(directory=td, disk=CustomDisk, disk_min_file_size=1 << 30)
    ) as _cache:
        yield _cache


def test_get_404(patch_cache):
    response = client.get("/data")
    assert response.status_code == 404

    response = client.delete("/data")
    assert response.status_code == 404

    patch_cache.set("data", b"hello world")
    response = client.get("/data")
    assert response.status_code == 200


def test_put_get(patch_cache):
    response = client.put("/data", data=b"hello world")
    assert response.status_code == 200

    response = client.get("/data")
    assert response.status_code == 200
    assert response.content == b"hello world"


def test_put_get_raw(patch_cache_raw):
    response = client.put("/data", data=b"hello world")
    assert response.status_code == 200

    response = client.get("/data")
    assert response.status_code == 200
    assert response.content == b"hello world"


def test_put_delete_get(patch_cache):
    response = client.put("/data", data=b"hello world")
    assert response.status_code == 200

    response = client.delete("/data")
    assert response.status_code == 200

    response = client.get("/data")
    assert response.status_code == 404


def test_get_keys(patch_cache):  # noqa: R701
    client.put("/a", data=b"a")
    client.put("/b", data=b"b")
    client.put("/c", data=b"c")
    client.put("/b2", data=b"b2")

    r = client.post("/-/keys/", json={"max_num": 100}).json()
    assert [x["key"] for x in r] == ["a", "b", "c", "b2"]

    r = client.post("/-/keys/", json={"max_num": 1}).json()
    assert [x["key"] for x in r] == ["a"]

    r = client.post(
        "/-/keys/", json={"max_num": 1, "key": r[0]["key"], "store_time": r[0]["store_time"]}
    ).json()
    assert [x["key"] for x in r] == ["b"]

    r = client.post("/-/keys/", json={"max_num": 100, "prefix": "b"}).json()
    assert [x["key"] for x in r] == ["b", "b2"]

    r = client.post(
        "/-/keys/", json={"max_num": 100, "key": r[0]["key"], "store_time": r[0]["store_time"], "prefix": "b"}
    ).json()
    assert [x["key"] for x in r] == ["b2"]

    for i in [1, 2, 3, 4, 100]:
        r = client.post("/-/keys/", json={"max_num": 100, "load_limit": i}).json()
        assert [x["key"] for x in r] == ["a", "b", "c", "b2"]

        r = client.post("/-/keys/", json={"max_num": 1, "load_limit": i}).json()
        assert [x["key"] for x in r] == ["a"]

        r = client.post(
            "/-/keys/",
            json={"max_num": 1, "key": r[0]["key"], "store_time": r[0]["store_time"], "load_limit": i},
        ).json()
        assert [x["key"] for x in r] == ["b"]

        r = client.post("/-/keys/", json={"max_num": 100, "prefix": "b", "load_limit": i}).json()
        assert [x["key"] for x in r] == ["b", "b2"]

        r = client.post(
            "/-/keys/",
            json={
                "max_num": 100,
                "key": r[0]["key"],
                "store_time": r[0]["store_time"],
                "prefix": "b",
                "load_limit": i,
            },
        ).json()
        assert [x["key"] for x in r] == ["b2"]


def test_skip_download(patch_cache):
    response = client.put("/data", data=b"hello world")
    assert response.status_code == 200

    response = client.get("/data", headers={"If-None-Match": hashlib.sha256(b"hello world").hexdigest()})
    assert response.status_code == 304


def test_headers(patch_cache):
    response = client.put(
        "/data",
        data=gzip.compress(b"hello world"),
        headers={
            "x-set-cache-control": "public, must-revalidate, proxy-revalidate",
            "Content-Type": "text/plain",
            "Content-Encoding": "gzip",
        },
    )
    assert response.status_code == 200

    response = client.get("/data")
    assert response.status_code == 200
    assert int(response.headers["content-length"]) == len(gzip.compress(b"hello world"))
    assert response.headers["cache-control"] == "public, must-revalidate, proxy-revalidate"
    assert response.headers["content-type"] == "text/plain"
    assert response.headers["content-encoding"] == "gzip"
    assert response.content == b"hello world"


# def test_stream(patch_cache):
#     response = client.put("/data", data=b"hello world", headers={"content-length": None})
#     assert response.status_code == 200

#     response = client.get("/data")
#     assert response.status_code == 200
#     assert response.content == b"hello world"


def test_get_flushall(patch_cache):
    response = client.post("/-/flushall/")
    assert response.status_code == 200


def test_get_healthcheck(patch_cache):
    response = client.get("/-/healthcheck/")
    assert response.status_code == 200


def test_get_metrics(patch_cache):
    response = client.get("/-/metrics/")
    assert response.status_code == 200
