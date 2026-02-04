"""
私人HTTP数据获取器模块
与private_ws_pool同级的独立模块，负责通过HTTP主动获取币安私人数据
"""

from .fetcher import PrivateHTTPFetcher

__version__ = "1.0.0"
__all__ = ['PrivateHTTPFetcher']

# 可选：添加模块初始化日志
import logging
logging.getLogger(__name__).debug(f"私人HTTP获取器模块 v{__version__} 已加载")
