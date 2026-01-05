[file name]: config.py
[file content begin]
"""
WebSocket连接池配置 - 自管理模式
"""
from typing import Dict, Any

# 交易所配置
EXCHANGE_CONFIGS = {
    "binance": {
        "ws_public_url": "wss://fstream.binance.com/ws",
        "ws_private_url": "wss://fstream.binance.com/ws",
        "rest_url": "https://fapi.binance.com",
        
        # 连接配置
        "active_connections": 2,        # 主连接数
        "total_connections": 5,         # 总连接数（主+备）
        "symbols_per_connection": 300,  # 每个连接负责的合约数
        
        # 兼容性配置
        "masters_count": 2,
        "warm_standbys_count": 3,
        "symbols_per_master": 300,
        
        # 其他配置
        "reconnect_interval": 3,
        "ping_interval": 10,
    },
    "okx": {
        "ws_public_url": "wss://ws.okx.com:8443/ws/v5/public",
        "ws_private_url": "wss://ws.okx.com:8443/ws/v5/private",
        "rest_url": "https://www.okx.com",
        
        # 连接配置
        "active_connections": 1,
        "total_connections": 3,
        "symbols_per_connection": 300,
        
        # 兼容性配置
        "masters_count": 1,
        "warm_standbys_count": 2,
        "symbols_per_master": 300,
        
        # 其他配置
        "reconnect_interval": 3,
        "ping_interval": 3,  # OKX必须3秒
    }
}

# 订阅的数据类型
SUBSCRIPTION_TYPES = {
    "funding_rate": True,
    "mark_price": True,
    "orderbook": "books5",
    "ticker": True,
}

# 合约筛选
SYMBOL_FILTERS = {
    "binance": {
        "quote": "USDT",
    },
    "okx": {
        "quote": "USDT",
    }
}
[file content end]