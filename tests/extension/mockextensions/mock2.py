"""项目内部接口说明。"""
# by the test functions.


def _jupyter_server_extension_paths():
    return [{"module": "tests.extension.mockextensions.mock2"}]


def _load_jupyter_server_extension(serverapp):
    serverapp.mockII = True
    serverapp.mock_shared = "II"
