# trading/templates.py
"""
交易参数模板 - 静态只读模板
固定值已写好，变化值用 None 占位
谁需要谁拷贝，拷贝后在自己的缓存里填充
"""
"""
币安的止损止盈参数，使用示例：

# 按索引填充触发价（索引0=止损，索引1=止盈）
oco_cache["orders"][0]["triggerPrice"] = "50000"   # 止损价
oco_cache["orders"][1]["triggerPrice"] = "70000"   # 止盈价

"""


# ==================== 设置杠杆 ====================

SET_LEVERAGE_OKX = {
    "exchange": "okx",
    "type": "set_leverage",
    "params": {
        "instId": None,      # 合约名，如 "BTC-USDT-SWAP"
        "lever": None,       # 杠杆倍数，如 "10"（字符串）
        "mgnMode": "cross"   # 固定：全仓
    }
}

SET_LEVERAGE_BINANCE = {
    "exchange": "binance",
    "type": "set_leverage",
    "params": {
        "symbol": None,      # 合约名，如 "BTCUSDT"
        "leverage": None     # 杠杆倍数，如 10（数字，不带引号）
    }
}

# ==================== 市价开仓 ====================

OPEN_MARKET_OKX = {
    "exchange": "okx",
    "type": "open_market",
    "params": {
        "instId": None,      # 合约名，如 "BTC-USDT-SWAP"
        "tdMode": "cross",   # 固定：全仓
        "side": None,        # 方向，"buy" 或 "sell"
        "ordType": "market", # 固定：市价
        "sz": None,          # 开仓张数，如 "3.33"（字符串）
        "posSide": None      # 持仓方向，"long" 或 "short"
    }
}

OPEN_MARKET_BINANCE = {
    "exchange": "binance",
    "type": "open_market",
    "params": {
        "symbol": None,      # 合约名，如 "BTCUSDT"
        "side": None,        # 方向，"BUY" 或 "SELL"（大写）
        "type": "MARKET",    # 固定：市价
        "quantity": None,    # 开仓币数，如 "0.01"（字符串）
        "positionSide": None # 持仓方向，"LONG" 或 "SHORT"（大写）
    }
}

# ==================== 止损止盈 ====================

# 注意：止盈止损的订单方向（side）和现仓位持仓方向（posSide）相反。
# 多单时：posSide="long"   side="sell"，
# 空单时：posSide="short"  side="buy"，

OCO_OKX = {
    "exchange": "okx",
    "type": "oco",
    "params": {
        "instId": None,              # 合约名，如 "BTC-USDT-SWAP"
        "ordType": "oco",            # 固定
        "tdMode": "cross",           # 固定：全仓
        "side": None,                # 订单方向，"sell" 或 "buy"
#        "sz": None,                  # 持仓张数（与开仓张数相同）
        "posSide": None,             # 现仓位持仓方向，"long" 或 "short"
        "tpTriggerPx": None,         # 止盈触发价
        "tpOrdPx": "-1",             # 固定：市价
        "slTriggerPx": None,         # 止损触发价
        "slOrdPx": "-1",             # 固定：市价
        "tpTriggerPxType": "last",   # 固定：最新价触发
        "slTriggerPxType": "last",    # 固定：最新价触发
        "closeFraction": "1",        # ← 关键：平全部仓位，不是开仓
        "reduceOnly": "true",        # ← 必须：只减仓
        "cxlOnClosePos": "true"      # ← 与仓位绑定
    }
}

OCO_BINANCE = {
    "exchange": "binance",
    "type": "oco",
    "orders": [
        {
            # ============================================================
            # 索引 0：止损单（STOP_MARKET）
            # 填充方式：oco_cache["orders"][0]["stopPrice"] = "止损价"
            # ============================================================
          
            "symbol": None,              # 合约名
            "side": None,                # 平仓方向，"SELL" 或 "BUY"
            "type": "STOP_MARKET",       # 止损单类型
            "positionSide": None,        # 持仓方向
            "quantity": None,            # 平仓数量（从私人数据读取持仓币数）
            "stopPrice": None,           # 止损触发价（注意参数名是 stopPrice）
            "reduceOnly": "true",        # 只减仓
            "workingType": "CONTRACT_PRICE",
            "priceProtect": "true"       # 防插针误触发
        },
        {
            # ============================================================
            # 索引 1：止盈单（TAKE_PROFIT_MARKET）
            # 填充方式：oco_cache["orders"][1]["stopPrice"] = "止盈价"
            # ============================================================
         
            "symbol": None,              # 合约名
            "side": None,                # 平仓方向
            "type": "TAKE_PROFIT_MARKET", # 止盈单类型
            "positionSide": None,        # 持仓方向
            "quantity": None,            # 平仓数量（从私人数据读取持仓币数）
            "stopPrice": None,           # 止盈触发价
            "reduceOnly": "true",        # 只减仓
            "workingType": "CONTRACT_PRICE",
            "priceProtect": "true"       # 防插针误触发
        }
    ]
}

# ==================== 市价平仓 ====================

CLOSE_POSITION_OKX = {
    "exchange": "okx",
    "type": "close_position",
    "params": {
        "instId": None,      # 合约名，如 "BTC-USDT-SWAP"
        "mgnMode": "cross",  # 固定：全仓
        "posSide": None      # 持仓方向，"long" 或 "short"
    }
}

CLOSE_POSITION_BINANCE = {
    "exchange": "binance",
    "type": "close_position",        # 和开仓同一个接口
    "params": {
        "symbol": None,
        "side": None,              # 开仓方向为"LONG" 时，"side":为 "SELL"，开仓方向为"SHORT"时，"side":为 "BUY" 
        "positionSide": None,        # 持仓方向
        "quantity": None,          # 从私人数据读取持仓币数
        "type": "MARKET",   # 固定：市价
#         "reduceOnly": "true"       # 关键：只减仓不反向开仓
    }
}