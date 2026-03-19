"""
数据完成部门 - 模块根入口
==================================================
【模块职责】
这个模块是整个数据完成系统的核心，负责：
1. 接收外部数据（私人数据、行情数据）
2. 检测数据状态并打标签
3. 根据标签调度数据流向
4. 修复不完整的数据（币安半成品、持仓缺失）
5. 将数据存储到数据库

【模块结构】
data_completion_department/
├── __init__.py              # 模块根入口（这个文件）
├── receiver.py              # 接收存储区
├── detector.py              # 检测区
├── scheduler.py             # 调度区
├── database.py              # 数据库存储区
├── constants.py             # 标签常量
└── repair/                  # 修复区
    ├── __init__.py
    ├── binance/
    │   ├── __init__.py
    │   ├── semi_repair.py
    │   └── missing_repair.py
    └── okx/
        ├── __init__.py
        └── missing_repair.py

【对外暴露接口】
- receive_private_data()  - 接收私人数据
- receive_market_data()   - 接收行情数据
- get_receiver()          - 获取接收器实例
- DataDetector            - 检测区类
- Scheduler               - 调度区类
- Database                - 数据库类
- BinanceRepairArea       - 币安修复区入口
- BinanceSemiRepair       - 币安半成品修复
- BinanceMissingRepair    - 币安持仓缺失修复
- OkxMissingRepair        - 欧意持仓缺失修复
==================================================
"""

from .receiver import receive_private_data, receive_market_data, get_receiver
from .detector import DataDetector
from .scheduler import Scheduler
from .database import Database

# 从 repair 包导入所有修复类
from .repair import (
    BinanceRepairArea,
    BinanceSemiRepair,
    BinanceMissingRepair,
    OkxMissingRepair
)

# 导入所有常量
from .constants import (
    # 数据标签
    TAG_EMPTY,
    TAG_COMPLETE,
    TAG_CLOSED_COMPLETE,
    
    # 信息标签
    INFO_BINANCE_SEMI,
    INFO_BINANCE_MISSING,
    INFO_BINANCE_EMPTY,
    INFO_OKX_MISSING,
    INFO_OKX_EMPTY,
    
    # 交易所常量
    EXCHANGE_BINANCE,
    EXCHANGE_OKX,
    
    # ----- 基础信息 -----
    FIELD_EXCHANGE,              # 交易所名称
    
    # ----- 开仓信息 -----
    FIELD_OPEN_CONTRACT,         # 开仓合约名
    FIELD_OPEN_PRICE,            # 开仓价格
    FIELD_OPEN_DIRECTION,        # 开仓方向
    FIELD_POSITION_SIZE,         # 持仓币数
    FIELD_POSITION_CONTRACTS,    # 持仓张数
    FIELD_CONTRACT_VALUE,        # 合约面值
    FIELD_LEVERAGE,              # 杠杆倍数
    FIELD_OPEN_POSITION_VALUE,   # 开仓价仓位价值
    FIELD_OPEN_MARGIN,           # 开仓保证金
    
    # ----- 标记价相关 -----
    FIELD_MARK_PRICE,            # 当前标记价格
    FIELD_MARK_POSITION_VALUE,   # 标记价仓位价值
    FIELD_MARK_MARGIN,           # 标记价保证金
    FIELD_MARK_PNL,              # 标记价浮盈
    FIELD_MARK_PNL_PERCENT_OF_MARGIN,  # 标记价浮盈百分比
    
    # ----- 最新价相关 -----
    FIELD_LATEST_PRICE,          # 当前最新成交价
    FIELD_LATEST_POSITION_VALUE, # 最新价仓位价值
    FIELD_LATEST_MARGIN,         # 最新价保证金
    FIELD_LATEST_PNL,            # 最新价浮盈
    FIELD_LATEST_PNL_PERCENT_OF_MARGIN,  # 最新价浮盈百分比
    
    # ----- 涨跌盈亏幅（相对开仓价）-----
    FIELD_MARK_PNL_PERCENT,      # 标记价涨跌盈亏幅
    FIELD_LATEST_PNL_PERCENT,    # 最新价涨跌盈亏幅
    
    # ----- 资金费相关 -----
    FIELD_FUNDING_THIS,          # 本次资金费
    FIELD_FUNDING_TOTAL,         # 累计资金费
    FIELD_FUNDING_COUNT,         # 资金费结算次数
    FIELD_FUNDING_TIME,          # 本次资金费结算时间
    FIELD_AVG_FUNDING_RATE,      # 平均资金费率
    
    # ----- 平仓信息（通用）-----
    FIELD_CLOSE_TIME,            # 平仓时间
    FIELD_CLOSE_PRICE,           # 平仓价
    FIELD_CLOSE_POSITION_VALUE,  # 平仓价仓位价值
    FIELD_CLOSE_PNL_PERCENT,     # 平仓价涨跌盈亏幅
    FIELD_CLOSE_PNL,             # 平仓收益
    FIELD_CLOSE_PNL_PERCENT_OF_MARGIN,  # 平仓收益率
    
    # ----- 币安平仓特有字段 -----
    FIELD_CLOSE_EXEC_TYPE,       # 平仓执行方式
    FIELD_CLOSE_FEE,             # 平仓手续费
    FIELD_CLOSE_FEE_CURRENCY,    # 平仓手续费币种
)

# ===== 版本信息 =====
__version__ = '1.0.0'
__author__ = 'DataCompletion Team'
__description__ = '数据完成模块 - 接收、检测、修复、存储'

# ===== 对外暴露的接口 =====
__all__ = [
    # 核心接口
    'receive_private_data',
    'receive_market_data',
    'get_receiver',
    
    # 主要类
    'DataDetector',
    'Scheduler',
    'Database',
    'BinanceRepairArea',      # 币安修复区入口
    'BinanceSemiRepair',      # 币安半成品修复
    'BinanceMissingRepair',   # 币安持仓缺失修复
    'OkxMissingRepair',       # 欧意持仓缺失修复
    
    # 数据标签常量
    'TAG_EMPTY',              # 空仓
    'TAG_COMPLETE',           # 持仓完整
    'TAG_CLOSED_COMPLETE',    # 平仓完整
    
    # 信息标签常量 - 币安
    'INFO_BINANCE_SEMI',       # 币安半成品
    'INFO_BINANCE_MISSING',    # 币安持仓缺失
    'INFO_BINANCE_EMPTY',      # 币安空仓
    
    # 信息标签常量 - 欧意
    'INFO_OKX_MISSING',        # 欧意持仓缺失
    'INFO_OKX_EMPTY',          # 欧意空仓
    
    # 交易所常量
    'EXCHANGE_BINANCE',
    'EXCHANGE_OKX',
    
    # ----- 基础信息 -----
    'FIELD_EXCHANGE',
    
    # ----- 开仓信息 -----
    'FIELD_OPEN_CONTRACT',
    'FIELD_OPEN_PRICE',
    'FIELD_OPEN_DIRECTION',
    'FIELD_POSITION_SIZE',
    'FIELD_POSITION_CONTRACTS',
    'FIELD_CONTRACT_VALUE',
    'FIELD_LEVERAGE',
    'FIELD_OPEN_POSITION_VALUE',
    'FIELD_OPEN_MARGIN',
    
    # ----- 标记价相关 -----
    'FIELD_MARK_PRICE',
    'FIELD_MARK_POSITION_VALUE',
    'FIELD_MARK_MARGIN',
    'FIELD_MARK_PNL',
    'FIELD_MARK_PNL_PERCENT_OF_MARGIN',
    
    # ----- 最新价相关 -----
    'FIELD_LATEST_PRICE',
    'FIELD_LATEST_POSITION_VALUE',
    'FIELD_LATEST_MARGIN',
    'FIELD_LATEST_PNL',
    'FIELD_LATEST_PNL_PERCENT_OF_MARGIN',
    
    # ----- 涨跌盈亏幅 -----
    'FIELD_MARK_PNL_PERCENT',
    'FIELD_LATEST_PNL_PERCENT',
    
    # ----- 资金费相关 -----
    'FIELD_FUNDING_THIS',
    'FIELD_FUNDING_TOTAL',
    'FIELD_FUNDING_COUNT',
    'FIELD_FUNDING_TIME',
    'FIELD_AVG_FUNDING_RATE',
    
    # ----- 平仓信息 -----
    'FIELD_CLOSE_TIME',
    'FIELD_CLOSE_PRICE',
    'FIELD_CLOSE_POSITION_VALUE',
    'FIELD_CLOSE_PNL_PERCENT',
    'FIELD_CLOSE_PNL',
    'FIELD_CLOSE_PNL_PERCENT_OF_MARGIN',
    
    # ----- 币安平仓特有字段 -----
    'FIELD_CLOSE_EXEC_TYPE',
    'FIELD_CLOSE_FEE',
    'FIELD_CLOSE_FEE_CURRENCY',
    
    # 版本信息
    '__version__',
    '__author__',
    '__description__',
]

# ===== 模块初始化日志 =====
import logging
logger = logging.getLogger(__name__)
logger.info(f"✅ 数据完成模块 v{__version__} 已加载")