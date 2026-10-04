"""项目内部接口说明。"""
# Function that makes these extensions discoverable
# by the test functions.


def _jupyter_server_extension_paths():
    return [{"module": "tests.extension.mockextensions.mockext_deprecated"}]


def load_jupyter_server_extension(serverapp):
    pass
