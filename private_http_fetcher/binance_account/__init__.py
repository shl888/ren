"""
币安账户资产获取模块
定时获取币安账户资产数据（私人HTTP获取器）
"""

from .fetcher import PrivateHTTPFetcher
from .starter import start_account_task

__all__ = [
    'PrivateHTTPFetcher',
    'start_account_task'
]

import logging
logging.getLogger(__name__).debug("币安账户资产模块已加载")