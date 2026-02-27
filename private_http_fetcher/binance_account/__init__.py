"""
币安账户资产获取模块
定时获取币安账户资产数据（私人HTTP获取器）
"""

from .fetcher import PrivateHTTPFetcher

__all__ = [
    'PrivateHTTPFetcher',
]