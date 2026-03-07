"""
数据完成部门
接收、存储、提供数据查询接口

使用方式：
    from data_completion_department import receive_data, get_receiver, setup_data_completion_routes
    
    # 接收数据
    await receive_data(data)
    
    # 获取接收器实例
    receiver = get_receiver()
    
    # 注册路由（由 HTTP 服务器调用）
    setup_data_completion_routes(app)
"""

from .receiver import receive_data, get_receiver
from .data_completion import setup_data_completion_routes

__all__ = [
    'receive_data',
    'get_receiver',
    'setup_data_completion_routes'
]

__version__ = '1.0.0'