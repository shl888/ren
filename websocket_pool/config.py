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
        
        # 角色互换配置 - ✅ 需要与 DEFAULT_WORKER_CONFIGS 保持一致
        "data_worker_count": 2,           # 数据工作者数量（原主连接）
        "backup_worker_count": 2,         # 备份工作者数量
        "max_symbols_per_worker": 300,    # 每个工作者最大合约数
        
        # 兼容性配置（可选）
        "masters_count": 2,               # 兼容旧代码
        "warm_standbys_count": 2,         # 兼容旧代码
        "symbols_per_master": 300,        # 兼容旧代码
        
        # 其他配置
        "rotation_interval": 1800,        # 轮转间隔(秒) - 30分钟
        "monitor_enabled": True,          # 启用监控
        "reconnect_interval": 3,          # 重连间隔(秒)
        "ping_interval": 30,              # 心跳间隔(秒)
        
        # ✅ 添加订阅配置
        "subscription_streams": [
            "ticker",     # 24hrTicker
            "markPrice"   # 标记价格
        ],
        "channels": {                     # 币安的channel映射
            "ticker": "@ticker",
            "markPrice": "@markPrice"
        }
    },
    "okx": {
        "ws_public_url": "wss://ws.okx.com:8443/ws/v5/public",
        "ws_private_url": "wss://ws.okx.com:8443/ws/v5/private",
        "rest_url": "https://www.okx.com",
        
        # 角色互换配置 - ✅ 需要与 DEFAULT_WORKER_CONFIGS 保持一致
        "data_worker_count": 1,
        "backup_worker_count": 1,
        "max_symbols_per_worker": 300,
        
        # 兼容性配置（可选）
        "masters_count": 1,
        "warm_standbys_count": 1,
        "symbols_per_master": 300,
        
        # 其他配置
        "rotation_interval": 1800,
        "monitor_enabled": True,
        "reconnect_interval": 3,
        "ping_interval": 30,
        
        # ✅ 添加订阅配置
        "subscription_channels": [
            "tickers",        # ticker数据
            "funding-rate"    # 资金费率
        ],
        "default_instType": "SWAP"
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
        "contract_type": ["linear", "swap"],  # USDT永续合约
        "active_only": True                    # 只获取活跃合约
    },
    "okx": {
        "quote": "USDT",
        "inst_type": "SWAP",                   # SWAP合约
        "active_only": True
    }
}

# ✅ 全局配置常量
DEFAULT_WORKER_CONFIGS = {
    "binance": {
        "data_worker_count": 2,
        "backup_worker_count": 2,
        "max_symbols_per_worker": 300,
        "batch_size": 50,                      # 批量订阅数量
        "ping_interval": 30                    # 心跳间隔
    },
    "okx": {
        "data_worker_count": 1,
        "backup_worker_count": 1,
        "max_symbols_per_worker": 300,
        "batch_size": 30,                      # OKX限制更严格
        "ping_interval": 30
    }
}

# ✅ 订阅消息格式配置
SUBSCRIPTION_FORMATS = {
    "binance": {
        "stream_separator": "@",
        "subscribe_method": "SUBSCRIBE",
        "unsubscribe_method": "UNSUBSCRIBE",
        "ping_message": {"method": "PING"},
        "pong_message": {"method": "PONG"}
    },
    "okx": {
        "channel_separator": ":",
        "subscribe_op": "subscribe",
        "unsubscribe_op": "unsubscribe",
        "login_op": "login",
        "ping_message": {"op": "ping"},
        "pong_message": {"op": "pong"}
    }
}

# ✅ 调试配置
DEBUG_CONFIG = {
    "log_raw_messages": False,     # 是否记录原始消息
    "log_subscriptions": True,     # 是否记录订阅信息
    "log_heartbeats": False,       # 是否记录心跳
    "log_errors": True,            # 是否记录错误
    "verbose_connection": False    # 详细连接日志
}