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

from .constants import (
    TAG_EMPTY,
    TAG_COMPLETE,
    TAG_CLOSED_COMPLETE,
    INFO_BINANCE_SEMI,
    INFO_BINANCE_MISSING,
    INFO_BINANCE_EMPTY,
    INFO_OKX_MISSING,
    INFO_OKX_EMPTY,
    EXCHANGE_BINANCE,
    EXCHANGE_OKX
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
    
    # 版本信息
    '__version__',
    '__author__',
    '__description__',
]

# ===== 模块初始化日志 =====
import logging
logger = logging.getLogger(__name__)
logger.info(f"✅ 数据完成模块 v{__version__} 已加载")