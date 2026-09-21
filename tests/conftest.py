"""pytest 公共夹具。

日志通过 pyproject.toml 的 log_cli 配置输出（任务标识、位点、判定依据）。
可用 ``pytest -o log_cli=false`` 关闭。
"""

import logging

import pytest


@pytest.fixture(autouse=True)
def _case_banner(request):
    logging.getLogger("test.case").info("==== 开始用例 %s ====", request.node.name)
    yield
