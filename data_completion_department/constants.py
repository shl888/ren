"""
数据完成模块 - 标签常量
==================================================
【文件职责】
这个文件集中定义所有标签和字段常量，是整个模块的"通用语言"。

为什么要集中定义？
    1. 避免手写字符串错误（比如"已平仓"写成"己平仓"）
    2. 一处修改，处处生效
    3. IDE自动补全，提高编码效率
    4. 新同事一看就知道有哪些标签可用

【标签分类】
1. 数据标签（带数据） - 与数据本身一起推送
   - 空仓：无持仓，推大脑+推信息标签
   - 持仓完整：数据完整，直接存持仓区
   - 平仓完整：数据已平仓且完整，需要存历史+清理持仓

2. 信息标签（纯标签） - 独立推送，不带数据
   - 欧易空仓：欧意空仓状态信息
   - 欧意持仓缺失：触发欧意持仓缺失修复
   - 币安空仓：币安空仓状态信息
   - 币安持仓缺失：触发币安持仓缺失修复
   - 币安半成品：触发币安半成品修复

【使用示例】
    from .constants import TAG_EMPTY, INFO_BINANCE_SEMI

    # 推送数据标签
    await scheduler.handle({'tag': TAG_EMPTY, 'data': data})

    # 推送信息标签  
    await scheduler.handle({'info': INFO_BINANCE_SEMI})
==================================================
"""

# ========== 数据标签（带数据） ==========
"""
数据标签必须与数据一起推送，格式：{'tag': TAG_XXX, 'data': data}
"""

TAG_EMPTY = "空仓"           
"""
空仓标签
适用条件：标记价保证金为空
调度动作：推数据库？ + 推大脑（去标签） + 推对应信息标签
"""

TAG_COMPLETE = "持仓完整"    
"""
持仓完整标签
适用条件：开仓合约名有值 AND 平仓时间空
调度动作：推数据库（存持仓区） + 推大脑（去标签）
"""

TAG_CLOSED_COMPLETE = "平仓完整"    
"""
平仓完整标签
适用条件：开仓合约名有值 AND 平仓时间有值
调度动作：推数据库（存历史+清理持仓） + 推大脑（去标签）
"""

# ========== 信息标签（纯标签，不带数据） ==========
"""
信息标签单独推送，不带数据，格式：{'info': INFO_XXX}
这些标签是修复流程的开关，永远只有1个（覆盖更新）
"""

# ----- 欧意相关 -----
INFO_OKX_EMPTY = "欧易空仓"      
"""
欧易空仓标签
触发条件：欧意数据标记价保证金为空
修复动作：记录空仓状态信息
"""

INFO_OKX_MISSING = "欧意持仓缺失"      
"""
欧意持仓缺失标签
触发条件：欧意数据标记价保证金有值 AND 开仓合约名空
修复动作：启动欧意持仓缺失修复流程（循环运行）
"""

# ----- 币安相关 -----
INFO_BINANCE_EMPTY = "币安空仓"      
"""
币安空仓标签
触发条件：币安数据标记价保证金为空
修复动作：记录空仓状态信息
"""

INFO_BINANCE_SEMI = "币安半成品"      
"""
币安半成品标签
触发条件：币安数据标记价保证金有值 AND 开仓合约名有值
修复动作：启动币安半成品修复流程（循环运行）
"""

INFO_BINANCE_MISSING = "币安持仓缺失"  
"""
币安持仓缺失标签
触发条件：币安数据标记价保证金有值 AND 开仓合约名空
修复动作：启动币安持仓缺失修复流程（循环运行）
"""

# ========== 交易所常量 ==========
"""
交易所名称常量，用于统一标识
注意：这些值必须与数据中的"交易所"字段值完全一致
"""

EXCHANGE_BINANCE = "binance"  # 币安交易所
EXCHANGE_OKX = "okx"          # 欧易交易所

# ========== 字段名常量 ==========
"""
字段名常量，对应数据字典中的key（全是中文）
使用这些常量可以避免手写中文字符串，减少错误
"""

# ----- 基础信息 -----
FIELD_EXCHANGE = "交易所"              # 交易所名称

# ----- 开仓信息 -----
FIELD_OPEN_CONTRACT = "开仓合约名"      # 开仓合约名，如 BTCUSDT
FIELD_OPEN_PRICE = "开仓价"             # 开仓价格
FIELD_OPEN_DIRECTION = "开仓方向"       # 开仓方向：LONG/SHORT
FIELD_POSITION_SIZE = "持仓币数"        # 持仓数量（币）
FIELD_POSITION_CONTRACTS = "持仓张数"   # 持仓数量（张）
FIELD_CONTRACT_VALUE = "合约面值"       # 每张合约的面值
FIELD_LEVERAGE = "杠杆"                 # 杠杆倍数
FIELD_OPEN_POSITION_VALUE = "开仓价仓位价值"  # 开仓价 * 持仓币数
FIELD_OPEN_MARGIN = "开仓保证金"        # 开仓保证金

# ----- 标记价相关 -----
FIELD_MARK_PRICE = "标记价"             # 当前标记价格
FIELD_MARK_POSITION_VALUE = "标记价仓位价值"  # 标记价 * 持仓币数
FIELD_MARK_MARGIN = "标记价保证金"       # 基于标记价计算的保证金
FIELD_MARK_PNL = "标记价浮盈"           # 基于标记价的浮动盈亏
FIELD_MARK_PNL_PERCENT_OF_MARGIN = "标记价浮盈百分比"  # 基于保证金的盈亏百分比

# ----- 最新价相关 -----
FIELD_LATEST_PRICE = "最新价"           # 当前最新成交价
FIELD_LATEST_POSITION_VALUE = "最新价仓位价值"  # 最新价 * 持仓币数
FIELD_LATEST_MARGIN = "最新价保证金"    # 基于最新价计算的保证金
FIELD_LATEST_PNL = "最新价浮盈"         # 基于最新价的浮动盈亏
FIELD_LATEST_PNL_PERCENT_OF_MARGIN = "最新价浮盈百分比"  # 基于保证金的盈亏百分比

# ----- 涨跌盈亏幅（相对开仓价）-----
FIELD_MARK_PNL_PERCENT = "标记价涨跌盈亏幅"    # (标记价-开仓价)/开仓价*100
FIELD_LATEST_PNL_PERCENT = "最新价涨跌盈亏幅"  # (最新价-开仓价)/开仓价*100

# ----- 资金费相关 -----
FIELD_FUNDING_THIS = "本次资金费"        # 最近一次资金费
FIELD_FUNDING_TOTAL = "累计资金费"       # 累计资金费总和
FIELD_FUNDING_COUNT = "资金费结算次数"    # 资金费结算次数
FIELD_FUNDING_TIME = "本次资金费结算时间" # 最近一次结算时间
FIELD_AVG_FUNDING_RATE = "平均资金费率"  # 平均资金费率

# ----- 平仓信息（通用）-----
FIELD_CLOSE_TIME = "平仓时间"           # 平仓时间，有值表示已平仓
FIELD_CLOSE_PRICE = "平仓价"            # 平仓价格
FIELD_CLOSE_POSITION_VALUE = "平仓价仓位价值"  # 平仓价 * 持仓币数
FIELD_CLOSE_PNL_PERCENT = "平仓价涨跌盈亏幅"   # (平仓价-开仓价)/开仓价*100 或 (开仓价-平仓价)/开仓价*100
FIELD_CLOSE_PNL = "平仓收益"            # 平仓盈亏金额
FIELD_CLOSE_PNL_PERCENT_OF_MARGIN = "平仓收益率"  # 平仓收益相对于保证金的百分比

# ----- 币安平仓特有字段（新增）-----
FIELD_CLOSE_EXEC_TYPE = "平仓执行方式"   # 平仓执行方式：MARKET/LIMIT等
FIELD_CLOSE_FEE = "平仓手续费"           # 平仓手续费
FIELD_CLOSE_FEE_CURRENCY = "平仓手续费币种"  # 平仓手续费币种

# ========== 扩展字段（可根据需要添加）==========
"""
随着项目发展，可能会需要更多字段常量，可以在这里统一添加
例如：
    FIELD_STOP_LOSS = "止损触发价"
    FIELD_TAKE_PROFIT = "止盈触发价"
    ...
"""