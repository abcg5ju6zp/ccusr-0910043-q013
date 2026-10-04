"""项目内部接口说明。"""

import json

import pytest
from tornado.httpclient import HTTPClientError


@pytest.fixture()
def jp_server_config(jp_server_config):
    return {"KernelManager": {"shutdown_wait_time": 0}}


async def test_restart_success(jp_fetch):
    """项目内部接口说明。"""
    r = await jp_fetch("api", "kernels", method="POST", body="{}")
    kernel = json.loads(r.body.decode())
    kernel_id = kernel["id"]

    r = await jp_fetch(
        "api",
        "kernels",
        kernel_id,
        "restart",
        method="POST",
        allow_nonstandard_methods=True,
    )
    assert r.code == 200
    model = json.loads(r.body.decode())
    assert model["id"] == kernel_id


async def test_restart_failure_returns_500_json(jp_fetch, jp_serverapp):
    """项目内部接口说明。"""
    r = await jp_fetch("api", "kernels", method="POST", body="{}")
    kernel = json.loads(r.body.decode())
    kernel_id = kernel["id"]

    # Make restart_kernel fail
    km = jp_serverapp.kernel_manager
    original_restart = km.restart_kernel

    async def failing_restart(*args, **kwargs):
        raise RuntimeError("kernel process died")

    km.restart_kernel = failing_restart
    try:
        with pytest.raises(HTTPClientError) as exc_info:
            await jp_fetch(
                "api",
                "kernels",
                kernel_id,
                "restart",
                method="POST",
                allow_nonstandard_methods=True,
            )

        response = exc_info.value.response
        assert response.code == 500

        # Verify it's a proper JSON response from write_error()
        assert "application/json" in response.headers.get("Content-Type", "")

        body = json.loads(response.body.decode())
        assert "message" in body
        assert "Exception restarting kernel" in body["message"]
        # write_error() includes "reason" field; the old manual write didn't
        assert "reason" in body
    finally:
        km.restart_kernel = original_restart


async def test_interrupt_unaffected(jp_fetch):
    """项目内部接口说明。"""
    r = await jp_fetch("api", "kernels", method="POST", body="{}")
    kernel = json.loads(r.body.decode())
    kernel_id = kernel["id"]

    r = await jp_fetch(
        "api",
        "kernels",
        kernel_id,
        "interrupt",
        method="POST",
        allow_nonstandard_methods=True,
    )
    assert r.code == 204
