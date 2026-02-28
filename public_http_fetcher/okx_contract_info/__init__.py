"""
OKX合约面值获取模块
获取所有USDT本位永续合约的面值信息
"""
from .fetcher import OKXContractFetcher
from .cleaner import OKXContractCleaner

__all__ = ['OKXContractFetcher', 'OKXContractCleaner']