"""
WebSocket连接池配置 - 角色互换版
"""
from typing import Dict, Any

# 交易所配置
EXCHANGE_CONFIGS = {
    "binance": {
        "ws_public_url": "wss://fstream.binance.com/ws",
        "ws_private_url": "wss://fstream.binance.com/ws",
        "rest_url": "https://fapi.binance.com",
        
        # 角色互换配置
        "active_connections": 3,        # 活跃连接数（原主连接）
        "total_connections": 6,         # 总连接数（活跃+备用）
        "symbols_per_connection": 300,  # 每个连接负责的合约数
        
        # 兼容性配置（可选）
        "masters_count": 3,              # 兼容旧代码
        "warm_standbys_count": 3,        # 兼容旧代码
        "symbols_per_master": 300,       # 兼容旧代码
        
        # 其他配置
        "rotation_interval": 1800,      # 轮转间隔(秒) - 30分钟
        "monitor_enabled": True,        # 启用监控
        "reconnect_interval": 3,        # 重连间隔(秒)
        "ping_interval": 30,            # 心跳间隔(秒)
        
        # 在 binance 和 okx 的配置字典里添加这些键
        "failover_cooldown_seconds": 30,        # 故障转移冷却期
        "auto_reconnect_max_attempts": 3,       # 自动重连最大尝试次数
        "auto_reconnect_delay": 5,              # 自动重连延迟基数
        "warm_standby_delay_base": 15,          # 温备延迟订阅基数
        "monitor_check_interval": 10,           # 监控检查间隔
        
    },
    "okx": {
        "ws_public_url": "wss://ws.okx.com:8443/ws/v5/public",
        "ws_private_url": "wss://ws.okx.com:8443/ws/v5/private",
        "rest_url": "https://www.okx.com",
        
        # 角色互换配置
        "active_connections": 2,
        "total_connections": 4,
        "symbols_per_connection": 300,
        
        # 兼容性配置（可选）
        "masters_count": 2,
        "warm_standbys_count": 2,
        "symbols_per_master": 300,
        
        # 其他配置
        "rotation_interval": 1800,
        "monitor_enabled": True,
        "reconnect_interval": 3,
        "ping_interval": 30,
                
        # 在 binance 和 okx 的配置字典里添加这些键
        "failover_cooldown_seconds": 30,        # 故障转移冷却期
        "auto_reconnect_max_attempts": 3,       # 自动重连最大尝试次数
        "auto_reconnect_delay": 5,              # 自动重连延迟基数
        "warm_standby_delay_base": 15,          # 温备延迟订阅基数
        "monitor_check_interval": 10,           # 监控检查间隔
    }
}

# 订阅的数据类型
SUBSCRIPTION_TYPES = {
    "funding_rate": True,      # 资金费率
    "mark_price": True,        # 标记价格
    "orderbook": "books5",     # 5档深度(只取第一档)
    "ticker": True,            # 24小时涨跌幅和成交量
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