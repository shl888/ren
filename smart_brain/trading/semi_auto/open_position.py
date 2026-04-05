# trading/semi_auto/open_position.py
"""
半自动模式 - 开仓流程

================================================================================
本文件是什么？
================================================================================
本文件是大脑的行动指南（步骤说明书）。
大脑按照本文件写的步骤，一步一步执行开仓流程。
================================================================================
流程结构说明
================================================================================
本文件将开仓流程拆分为 step1 到 step7，每个 step 是一个独立的函数。
大脑按顺序执行每个 step：
    step1 → step2 → step3 → ... → step7

每个 step 的返回值：
    - success = True:  步骤执行成功，继续下一步
    - success = False: 步骤执行失败，直接跳到最后一步 step7，清理缓存后退出

================================================================================
大脑与步骤文件的关系？
================================================================================
- 步骤文件 = 写在纸上的步骤（第1步做什么、第2步做什么...）
- 大脑 = 干活的人，看着步骤文件，一步一步执行

步骤文件里写的每一行代码，都是大脑要执行的动作。
大脑执行完一步，自动接着执行下一步。
================================================================================
大脑与工人的关系
================================================================================
- 大脑和工人是合作伙伴关系
- 大脑负责：读取数据、计算、判断条件、控制流程
- 工人负责：发HTTP请求到交易所

大脑把参数准备好，交给工人，工人去执行。
大脑和工人之间不是“调用”关系，是“合作”关系。
================================================================================
什么时候等待工人结果？
================================================================================
- step3（设置杠杆）：需要等待工人结果（因为下一步开仓要用到杠杆）
- step6（开仓）：不需要等待工人结果（已经是最后一步，发完就结束）

================================================================================
错误处理
================================================================================
- 流程内错误（有持仓、保证金过高、最小下单量不足等）：只打印日志，不推送前端
- 工人执行结果（杠杆成功/失败、开仓成功/失败）：大脑在流程外有推送逻辑，本流程不管

================================================================================
流程步骤概览
================================================================================
step1：检测是否空仓（读取私人数据中的开仓合约名字段）
step2：检测保证金是否够用（保证金 ≤ 账户资产的70%）
step3：设置杠杆（填充参数 → 交给工人 → 等待结果）
step4：读取精度数据（行情、欧易面值、币安精度）
step5：计算下单参数（严格按公式计算张数和币数）
step6：开仓（填充参数 → 交给工人，不等待结果）
step7：清理缓存（最后一步，无论成功失败都要执行）
================================================================================
"""

import copy
import asyncio
import logging
from typing import Dict, Any, Optional, Tuple

from ..templates import (
    SET_LEVERAGE_OKX,
    SET_LEVERAGE_BINANCE,
    OPEN_MARKET_OKX,
    OPEN_MARKET_BINANCE
)

logger = logging.getLogger(__name__)


class OpenPositionFlow:
    """
    半自动开仓流程 - 大脑的行动指南
    
    大脑按照本类中的 step1-step7 方法，一步一步执行开仓流程。
    """
    
    def __init__(self, brain):
        """
        初始化流程
        
        Args:
            brain: 大脑实例，流程中的每一步都由大脑去执行
        """
        # 大脑实例（干活的人）
        self.brain = brain
        self.data_manager = brain.data_manager
        
        # ==================== 缓存区 ====================
        # 存放4个模板的副本，大脑在流程中逐步填充
        self.okx_leverage_cache = None      # 欧易杠杆参数
        self.binance_leverage_cache = None  # 币安杠杆参数
        self.okx_order_cache = None         # 欧易开仓参数
        self.binance_order_cache = None     # 币安开仓参数
        
        # ==================== 流程中需要记住的变量 ====================
        # 这些变量大脑在流程中会逐步填充，供后续步骤使用
        self.raw_symbol: str = ""           # 原始合约名（如 BTCUSDT）
        self.okx_symbol: str = ""           # 欧易合约名（转换后，如 BTC-USDT-SWAP）
        self.binance_symbol: str = ""       # 币安合约名（如 BTCUSDT）
        self.leverage: int = 0              # 杠杆倍数
        self.margin: float = 0.0            # 保证金（USDT）
        self.direction: str = ""            # 方向：long_binance_short_okx 或 long_okx_short_binance
        
        # 行情数据
        self.okx_trade_price: float = 0.0   # 欧易最新成交价
        self.binance_trade_price: float = 0.0  # 币安最新成交价
        
        # 欧易面值数据
        self.ctVal: float = 0.0             # 合约面值
        self.lotSz: float = 0.0             # 下单步长（张数）
        self.minSz: float = 0.0             # 最小下单量（张数）
        
        # 币安精度数据
        self.stepSize: float = 0.0          # 下单步长（币数）
        self.minQty: float = 0.0            # 最小下单量（币数）
        
        # 计算结果
        self.sz: float = 0.0                # 欧易开仓张数
        self.quantity: float = 0.0          # 币安开仓币数
    
    # ==================== 转换规则 ====================
    
    def _convert_okx_symbol(self, symbol: str) -> str:
        """
        转换欧易合约名格式
        
        规则：
        - BTCUSDT → BTC-USDT-SWAP
        - ETHUSDT → ETH-USDT-SWAP
        - 如果已经是目标格式（包含-SWAP），直接返回
        
        Args:
            symbol: 原始合约名，如 "BTCUSDT"
            
        Returns:
            欧易格式的合约名，如 "BTC-USDT-SWAP"
        """
        if not symbol:
            return symbol
        
        # 已经是目标格式，直接返回
        if '-SWAP' in symbol:
            return symbol
        
        # USDT结尾的合约：BTCUSDT → BTC-USDT-SWAP
        if symbol.endswith('USDT'):
            base = symbol[:-4]  # 去掉末尾的 "USDT"
            return f"{base}-USDT-SWAP"
        
        # 其他格式暂不转换
        return symbol
    
    # ==================== 数据读取（大脑去存储区拿数据）====================
    
    async def _check_empty_position(self) -> Tuple[bool, str]:
        """
        检查是否空仓
        
        大脑读取私人数据中的开仓合约名字段。
        如果任一交易所的开仓合约名字段有值，说明有持仓，禁止开仓。
        
        Returns:
            (是否空仓, 错误信息)
            - True: 空仓，可以继续
            - False: 有持仓，禁止开仓
        """
        try:
            # 大脑去存储区拿私人数据
            user_data_result = await self.data_manager.get_private_user_data()
            user_data = user_data_result.get('data', {})
            
            # 检查欧易持仓
            okx_data = user_data.get('okx', {})
            okx_position_symbol = okx_data.get('开仓合约名', '')
            
            # 检查币安持仓
            binance_data = user_data.get('binance', {})
            binance_position_symbol = binance_data.get('开仓合约名', '')
            
            if okx_position_symbol:
                return False, f"欧易已有持仓: {okx_position_symbol}"
            
            if binance_position_symbol:
                return False, f"币安已有持仓: {binance_position_symbol}"
            
            return True, ""
            
        except Exception as e:
            logger.error(f"❌ step1 检测空仓失败: {e}")
            return False, f"检测空仓失败: {e}"
    
    async def _check_margin_enough(self) -> Tuple[bool, str]:
        """
        检查保证金是否够用
        
        条件：开仓保证金 ≤ 账户资产额的70%
        两个交易所都需满足，因为开仓需要同时占用两个交易所的资金
        
        Returns:
            (是否通过, 错误信息)
        """
        try:
            # 大脑去存储区拿私人数据
            user_data_result = await self.data_manager.get_private_user_data()
            user_data = user_data_result.get('data', {})
            
            # 获取欧易账户资产
            okx_data = user_data.get('okx', {})
            okx_asset = okx_data.get('账户资产额', 0)
            
            # 获取币安账户资产
            binance_data = user_data.get('binance', {})
            binance_asset = binance_data.get('账户资产额', 0)
            
            # 计算70%阈值
            okx_threshold = okx_asset * 0.7
            binance_threshold = binance_asset * 0.7
            
            # 检查欧易
            if self.margin > okx_threshold:
                return False, f"保证金 {self.margin} USDT 超过欧易账户资产70% ({okx_threshold:.2f} USDT)"
            
            # 检查币安
            if self.margin > binance_threshold:
                return False, f"保证金 {self.margin} USDT 超过币安账户资产70% ({binance_threshold:.2f} USDT)"
            
            return True, ""
            
        except Exception as e:
            logger.error(f"❌ step2 检测保证金失败: {e}")
            return False, f"检测保证金失败: {e}"
    
    async def _load_market_data(self) -> Tuple[bool, str]:
        """
        读取行情数据
        
        大脑从存储区获取指定合约的最新成交价。
        欧易和币安的价格都需要，用于后续计算。
        
        Returns:
            (是否成功, 错误信息)
        """
        try:
            # 大脑去存储区拿行情数据
            market_result = await self.data_manager.get_public_market_data()
            market_data = market_result.get('data', {})
            
            # 查找指定合约的数据
            symbol_data = market_data.get(self.raw_symbol)
            if not symbol_data:
                return False, f"未找到合约 {self.raw_symbol} 的行情数据"
            
            # 提取价格（字符串转float）
            okx_price_str = symbol_data.get('okx_trade_price', '0')
            binance_price_str = symbol_data.get('binance_trade_price', '0')
            
            self.okx_trade_price = float(okx_price_str)
            self.binance_trade_price = float(binance_price_str)
            
            # 价格有效性检查
            if self.okx_trade_price <= 0:
                return False, f"欧易最新成交价异常: {self.okx_trade_price}"
            
            if self.binance_trade_price <= 0:
                return False, f"币安最新成交价异常: {self.binance_trade_price}"
            
            logger.info(f"   📊 行情数据: 欧易={self.okx_trade_price}, 币安={self.binance_trade_price}")
            return True, ""
            
        except Exception as e:
            logger.error(f"❌ step4 读取行情数据失败: {e}")
            return False, f"读取行情数据失败: {e}"
    
    async def _load_okx_contract_info(self) -> Tuple[bool, str]:
        """
        读取欧易面值数据
        
        大脑从存储区获取合约面值(ctVal)、步长(lotSz)、最小下单量(minSz)。
        这些数据用于计算欧易开仓张数。
        
        Returns:
            (是否成功, 错误信息)
        """
        try:
            # 大脑去存储区拿欧易面值数据
            okx_contracts_result = await self.data_manager.get_okx_contracts_data()
            contracts = okx_contracts_result.get('data', [])
            
            # 查找指定合约
            found = False
            for contract in contracts:
                if contract.get('instId') == self.okx_symbol:
                    self.ctVal = float(contract.get('ctVal', 0))
                    self.lotSz = float(contract.get('lotSz', 0))
                    self.minSz = float(contract.get('minSz', 0))
                    found = True
                    break
            
            if not found:
                return False, f"未找到欧易合约 {self.okx_symbol} 的面值数据"
            
            # 数据有效性检查
            if self.ctVal <= 0:
                return False, f"欧易合约面值异常: {self.ctVal}"
            
            if self.lotSz <= 0:
                return False, f"欧易步长异常: {self.lotSz}"
            
            if self.minSz <= 0:
                return False, f"欧易最小下单量异常: {self.minSz}"
            
            logger.info(f"   📊 欧易面值: ctVal={self.ctVal}, lotSz={self.lotSz}, minSz={self.minSz}")
            return True, ""
            
        except Exception as e:
            logger.error(f"❌ step4 读取欧易面值数据失败: {e}")
            return False, f"读取欧易面值数据失败: {e}"
    
    async def _load_binance_precision(self) -> Tuple[bool, str]:
        """
        读取币安精度数据
        
        大脑从存储区获取步长(stepSize)、最小下单量(minQty)。
        这些数据用于计算币安开仓币数。
        
        Returns:
            (是否成功, 错误信息)
        """
        try:
            # 大脑去存储区拿币安精度数据
            binance_contracts_result = await self.data_manager.get_binance_contracts_data()
            contracts = binance_contracts_result.get('data', [])
            
            # 查找指定合约
            found = False
            for contract in contracts:
                if contract.get('symbol') == self.binance_symbol:
                    self.stepSize = float(contract.get('stepSize', 0))
                    self.minQty = float(contract.get('minQty', 0))
                    found = True
                    break
            
            if not found:
                return False, f"未找到币安合约 {self.binance_symbol} 的精度数据"
            
            # 数据有效性检查
            if self.stepSize <= 0:
                return False, f"币安步长异常: {self.stepSize}"
            
            if self.minQty <= 0:
                return False, f"币安最小下单量异常: {self.minQty}"
            
            logger.info(f"   📊 币安精度: stepSize={self.stepSize}, minQty={self.minQty}")
            return True, ""
            
        except Exception as e:
            logger.error(f"❌ step4 读取币安精度数据失败: {e}")
            return False, f"读取币安精度数据失败: {e}"
    
    # ==================== 计算（大脑自己计算）====================
    
    def _calculate_okx_sz(self) -> Tuple[bool, str]:
        """
        计算欧易开仓张数
        
        公式：sz = floor[(保证金 × 杠杆) / (okx_trade_price × ctVal × lotSz)] × lotSz
        
        公式拆解：
        1. 分子 = 保证金 × 杠杆
        2. 分母 = 欧易最新成交价 × 合约面值 × 步长
        3. 原始张数 = 分子 / 分母
        4. 向下取整 = floor(原始张数)
        5. 最终张数 = 向下取整结果 × 步长
        
        Returns:
            (是否成功, 错误信息)
        """
        try:
            # 第1步：计算分子（保证金 × 杠杆）
            numerator = self.margin * self.leverage
            
            # 第2步：计算分母（价格 × 面值 × 步长）
            denominator = self.okx_trade_price * self.ctVal * self.lotSz
            
            # 第3步：原始张数
            raw_sz = numerator / denominator
            
            # 第4步：向下取整
            floor_sz = int(raw_sz)
            
            # 第5步：乘以步长得到最终张数
            self.sz = floor_sz * self.lotSz
            
            logger.info(f"   📊 欧易计算: 分子={numerator}, 分母={denominator}, 原始={raw_sz}, floor={floor_sz}, 最终sz={self.sz}")
            
            # 校验最小下单量
            if self.sz < self.minSz:
                return False, f"欧易张数 {self.sz} 小于最小下单量 {self.minSz}"
            
            return True, ""
            
        except Exception as e:
            logger.error(f"❌ step5 计算欧易张数失败: {e}")
            return False, f"计算欧易张数失败: {e}"
    
    def _calculate_binance_quantity(self) -> Tuple[bool, str]:
        """
        计算币安开仓币数
        
        公式：
        quantity = floor｛｛floor[(保证金×杠杆）/ (okx_trade_price × ctVal × lotSz)] × lotSz × ctVal × okx_trade_price ｝ / binance_trade_price / stepSize ｝ × stepSize
        
        公式拆解：
        1. 内层floor = floor[(保证金×杠杆) / (价格 × 面值 × 步长)]
        2. 欧易价值 = 内层floor × 步长 × 面值 × 价格
        3. 原始币数 = 欧易价值 / 币安价格
        4. 除以步长 = 原始币数 / 步长
        5. 外层floor = floor(除以步长的结果)
        6. 最终币数 = 外层floor × 步长
        
        Returns:
            (是否成功, 错误信息)
        """
        try:
            # 第1步：内层floor
            inner_value = (self.margin * self.leverage) / (self.okx_trade_price * self.ctVal * self.lotSz)
            inner_floor = int(inner_value)
            
            # 第2步：计算欧易开仓价值
            okx_value = inner_floor * self.lotSz * self.ctVal * self.okx_trade_price
            
            # 第3步：原始币数（欧易价值 / 币安价格）
            raw_quantity = okx_value / self.binance_trade_price
            
            # 第4步：除以步长
            step_quantity = raw_quantity / self.stepSize
            
            # 第5步：外层floor
            outer_floor = int(step_quantity)
            
            # 第6步：乘以步长得到最终币数
            self.quantity = outer_floor * self.stepSize
            
            logger.info(f"   📊 币安计算: 内层floor={inner_floor}, 欧易价值={okx_value}, 原始={raw_quantity}, 最终={self.quantity}")
            
            # 校验最小下单量
            if self.quantity < self.minQty:
                return False, f"币安币数 {self.quantity} 小于最小下单量 {self.minQty}"
            
            return True, ""
            
        except Exception as e:
            logger.error(f"❌ step5 计算币安币数失败: {e}")
            return False, f"计算币安币数失败: {e}"
    
    # ==================== 缓存操作 ====================
    
    def _init_cache(self):
        """创建缓存：大脑拷贝4个模板到缓存区"""
        self.okx_leverage_cache = copy.deepcopy(SET_LEVERAGE_OKX)
        self.binance_leverage_cache = copy.deepcopy(SET_LEVERAGE_BINANCE)
        self.okx_order_cache = copy.deepcopy(OPEN_MARKET_OKX)
        self.binance_order_cache = copy.deepcopy(OPEN_MARKET_BINANCE)
        logger.info("   📦 缓存已初始化，4个模板已拷贝")
    
    def _fill_leverage_params(self):
        """
        填充杠杆参数
        
        大脑把合约名和杠杆倍数填入缓存中的杠杆参数模板。
        
        注意：
        - 欧易的lever字段需要转成字符串
        - 币安的leverage字段保持数字
        """
        # 欧易杠杆参数
        self.okx_leverage_cache["params"]["instId"] = self.okx_symbol
        self.okx_leverage_cache["params"]["lever"] = str(self.leverage)  # 字符串
        # mgnMode 固定为 "cross"，模板里已写
        
        # 币安杠杆参数
        self.binance_leverage_cache["params"]["symbol"] = self.binance_symbol
        self.binance_leverage_cache["params"]["leverage"] = self.leverage  # 数字
        
        logger.info(f"   📝 杠杆参数已填充: 欧易={self.okx_symbol} x{self.leverage}, 币安={self.binance_symbol} x{self.leverage}")
    
    def _fill_order_params_by_direction(self):
        """
        根据交易方向填充开仓参数
        
        大脑根据direction字段，决定欧易和币安的开仓方向（side和positionSide）。
        
        direction取值：
        - "long_binance_short_okx": 做多币安，做空欧易
        - "long_okx_short_binance": 做多欧易，做空币安
        """
        if self.direction == "long_binance_short_okx":
            # 做多币安，做空欧易
            self.okx_order_cache["params"]["side"] = "sell"
            self.okx_order_cache["params"]["posSide"] = "short"
            self.binance_order_cache["params"]["side"] = "BUY"
            self.binance_order_cache["params"]["positionSide"] = "LONG"
            logger.info(f"   📝 方向: 欧易做空, 币安做多")
            
        elif self.direction == "long_okx_short_binance":
            # 做多欧易，做空币安
            self.okx_order_cache["params"]["side"] = "buy"
            self.okx_order_cache["params"]["posSide"] = "long"
            self.binance_order_cache["params"]["side"] = "SELL"
            self.binance_order_cache["params"]["positionSide"] = "SHORT"
            logger.info(f"   📝 方向: 欧易做多, 币安做空")
            
        else:
            logger.error(f"❌ 未知方向: {self.direction}")
    
    def _fill_order_params(self):
        """
        填充开仓参数
        
        大脑把计算结果（张数、币数）填入缓存中的开仓参数模板。
        然后根据方向填充side和positionSide。
        """
        # 欧易开仓参数
        self.okx_order_cache["params"]["instId"] = self.okx_symbol
        self.okx_order_cache["params"]["sz"] = str(self.sz)
        # tdMode 固定为 "cross"，模板里已写
        # ordType 固定为 "market"，模板里已写
        
        # 币安开仓参数
        self.binance_order_cache["params"]["symbol"] = self.binance_symbol
        self.binance_order_cache["params"]["quantity"] = str(self.quantity)
        # type 固定为 "MARKET"，模板里已写
        
        # 根据方向填充side和positionSide
        self._fill_order_params_by_direction()
        
        logger.info(f"   📝 开仓参数已填充: 欧易张数={self.sz}, 币安币数={self.quantity}")
    
    # ==================== 与工人交互 ====================
    
    def _send_to_worker_no_wait(self, params: dict):
        """
        命令大脑：把参数交给工人（不等待结果）
        
        大脑已经把参数准备好了，交给工人去执行。
        这个方法只负责发送，不等待工人返回结果。
        
        Args:
            params: 准备好的参数
        """
        # 大脑把参数发给工人
        self.brain.send_to_worker([params])
        logger.info(f"   📤 交给工人: {params.get('type')} - {params.get('exchange')}")
    
    async def _wait_for_result(self, exchange: str, timeout: float = 30.0) -> Dict[str, Any]:
        """
        等待工人返回指定交易所的结果
        
        大脑把参数交给工人后，在这里挂起等待。
        工人干完活后会把结果推给大脑，大脑收到结果后唤醒，继续执行。
        
        Args:
            exchange: 交易所名称（"okx" 或 "binance"），用于匹配结果
            timeout: 超时时间（秒）
            
        Returns:
            工人返回的结果
        """
        # TODO: 大脑挂起等待的具体实现
        # 需要根据 exchange 匹配工人返回的结果
        logger.info(f"   ⏳ 等待 {exchange} 结果")
        await asyncio.sleep(0.1)  # 占位
        return {"success": True, "exchange": exchange}
    
    # ==================== step3：设置杠杆 ====================
    
    async def step3_set_leverage(self):
        """
        step3：设置杠杆
        
        大脑把填好的杠杆参数交给工人，然后等待工人返回结果。
        必须等杠杆设置成功，才能继续下一步开仓。
        
        Returns:
            {"success": True/False, "error": error_msg}
        """
        logger.info("⚙️ step3：设置杠杆")
        
        # 填充杠杆参数
        self._fill_leverage_params()
        
        logger.info(f"   📤 并发交给工人: 欧易杠杆, 币安杠杆")
        
        # 命令大脑：把参数交给工人（不等待）
        self._send_to_worker_no_wait(self.okx_leverage_cache)
        self._send_to_worker_no_wait(self.binance_leverage_cache)
        
        # 并发等待两个交易所的结果
        okx_task = self._wait_for_result("okx")
        binance_task = self._wait_for_result("binance")
        
        okx_result, binance_result = await asyncio.gather(okx_task, binance_task)
        
        # 检查结果
        if not okx_result.get('success', False):
            error_msg = f"欧易设置杠杆失败: {okx_result.get('error', '未知错误')}"
            logger.error(f"   ❌ {error_msg}")
            return {"success": False, "error": error_msg}
        
        if not binance_result.get('success', False):
            error_msg = f"币安设置杠杆失败: {binance_result.get('error', '未知错误')}"
            logger.error(f"   ❌ {error_msg}")
            return {"success": False, "error": error_msg}
        
        logger.info(f"   ✅ 杠杆设置成功")
        return {"success": True, "error": ""}
    
    # ==================== step6：开仓 ====================
    
    async def step6_send_order(self):
        """
        step6：开仓
        
        大脑把填好的开仓参数交给工人，不等待结果。
        这是开仓流程的最后一步（step7清理缓存除外），不需要等工人执行结果。
        
        Returns:
            {"success": True/False, "error": error_msg}
            - 只表示“交给工人”是否成功，不代表开仓成功
        """
        logger.info("📝 step6：开仓")
        
        # 填充开仓参数
        self._fill_order_params()
        
        logger.info(f"   📤 把开仓参数交给工人: 欧易, 币安")
        
        try:
            # 命令大脑：把参数交给工人
            self._send_to_worker_no_wait(self.okx_order_cache)
            self._send_to_worker_no_wait(self.binance_order_cache)
            
            logger.info(f"   ✅ 开仓参数已交给工人")
            return {"success": True, "error": ""}
            
        except Exception as e:
            error_msg = f"交给工人失败: {e}"
            logger.error(f"   ❌ {error_msg}")
            return {"success": False, "error": error_msg}
    
    # ==================== step7：清理缓存（最后一步）====================
    
    async def step7_clear_cache(self):
        """
        step7：清理缓存（最后一步，门）
        
        无论流程成功还是失败，从哪一步跳过来，都必须执行这一步。
        大脑清空缓存区，释放内存。
        """
        logger.info("🧹 step7：清理缓存（最后一步）")
        self.okx_leverage_cache = None
        self.binance_leverage_cache = None
        self.okx_order_cache = None
        self.binance_order_cache = None
        logger.info("   ✅ 缓存已清理")
    
    # ==================== 主流程 ====================
    
    async def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行开仓流程（大脑按照本方法一步一步执行）
        
        Args:
            params: 开仓指令参数，包含以下字段：
                - symbol: 合约名，如 "BTCUSDT"
                - margin: 保证金，如 36
                - leverage: 杠杆，如 20
                - direction: 方向，"long_binance_short_okx" 或 "long_okx_short_binance"
                
        Returns:
            执行结果字典
        """
        logger.info("=" * 60)
        logger.info("🚀 大脑开始执行开仓流程")
        
        # ==================== 初始化 ====================
        
        # 提取指令参数
        self.raw_symbol = params.get('symbol', '')
        self.margin = float(params.get('margin', 0))
        self.leverage = int(params.get('leverage', 0))
        self.direction = params.get('direction', '')
        
        # 转换欧易合约名
        self.okx_symbol = self._convert_okx_symbol(self.raw_symbol)
        self.binance_symbol = self.raw_symbol
        
        logger.info(f"   📋 指令参数: symbol={self.raw_symbol}, margin={self.margin}, leverage={self.leverage}, direction={self.direction}")
        logger.info(f"   📋 转换后: okx_symbol={self.okx_symbol}, binance_symbol={self.binance_symbol}")
        
        # 创建缓存
        logger.info("📦 创建缓存（拷贝4个参数模板）")
        self._init_cache()
        
        # ==================== step1：检测空仓 ====================
        logger.info("🔍 step1：检测是否空仓")
        is_empty, error_msg = await self._check_empty_position()
        
        if not is_empty:
            logger.warning(f"   ⚠️ 有持仓，禁止开仓: {error_msg}")
            await self.step7_clear_cache()
            return {"success": False, "error": error_msg}
        
        logger.info("   ✅ 空仓检查通过")
        
        # ==================== step2：检测保证金 ====================
        logger.info("💰 step2：检测保证金是否够用（≤账户资产70%）")
        margin_enough, error_msg = await self._check_margin_enough()
        
        if not margin_enough:
            logger.warning(f"   ⚠️ 保证金检查失败: {error_msg}")
            await self.step7_clear_cache()
            return {"success": False, "error": error_msg}
        
        logger.info(f"   ✅ 保证金检查通过: {self.margin} USDT")
        
        # ==================== step3：设置杠杆 ====================
        result = await self.step3_set_leverage()
        if not result["success"]:
            await self.step7_clear_cache()
            return {"success": False, "error": result["error"]}
        
        # ==================== step4：读取精度数据 ====================
        logger.info("📊 step4：读取精度数据")
        
        # 4a. 读取行情数据
        logger.info("   📊 4a. 读取行情数据")
        success, error_msg = await self._load_market_data()
        if not success:
            logger.warning(f"   ⚠️ {error_msg}")
            await self.step7_clear_cache()
            return {"success": False, "error": error_msg}
        
        # 4b. 读取欧易面值数据
        logger.info("   📊 4b. 读取欧易面值数据")
        success, error_msg = await self._load_okx_contract_info()
        if not success:
            logger.warning(f"   ⚠️ {error_msg}")
            await self.step7_clear_cache()
            return {"success": False, "error": error_msg}
        
        # 4c. 读取币安精度数据
        logger.info("   📊 4c. 读取币安精度数据")
        success, error_msg = await self._load_binance_precision()
        if not success:
            logger.warning(f"   ⚠️ {error_msg}")
            await self.step7_clear_cache()
            return {"success": False, "error": error_msg}
        
        # ==================== step5：计算下单参数 ====================
        logger.info("🧮 step5：计算下单参数")
        
        # 5a. 计算欧易张数
        logger.info("   🧮 5a. 计算欧易张数")
        success, error_msg = self._calculate_okx_sz()
        if not success:
            logger.warning(f"   ⚠️ {error_msg}")
            await self.step7_clear_cache()
            return {"success": False, "error": error_msg}
        
        # 5b. 计算币安币数
        logger.info("   🧮 5b. 计算币安币数")
        success, error_msg = self._calculate_binance_quantity()
        if not success:
            logger.warning(f"   ⚠️ {error_msg}")
            await self.step7_clear_cache()
            return {"success": False, "error": error_msg}
        
        # 校验最小下单量（已在计算中完成）
        logger.info("✅ 校验最小下单量（已通过）")
        
        # ==================== step6：开仓 ====================
        result = await self.step6_send_order()
        if not result["success"]:
            await self.step7_clear_cache()
            return {"success": False, "error": result["error"]}
        
        # ==================== step7：清理缓存 ====================
        await self.step7_clear_cache()
        
        # ==================== 成功返回 ====================
        success_msg = f"开仓参数已交给工人！合约={self.raw_symbol}, 保证金={self.margin}USDT, 杠杆={self.leverage}x, 方向={self.direction}"
        logger.info(f"✅ {success_msg}")
        logger.info("=" * 60)
        
        return {
            "success": True,
            "message": success_msg,
            "data": {
                "symbol": self.raw_symbol,
                "okx_symbol": self.okx_symbol,
                "binance_symbol": self.binance_symbol,
                "margin": self.margin,
                "leverage": self.leverage,
                "direction": self.direction,
                "okx_sz": self.sz,
                "binance_quantity": self.quantity,
                "okx_trade_price": self.okx_trade_price,
                "binance_trade_price": self.binance_trade_price
            }
        }