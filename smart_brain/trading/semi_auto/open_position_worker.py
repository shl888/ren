# trading/semi_auto/open_position_worker.py
"""
开仓工人 - 独立负责开仓参数计算和发送

工作流程：
1. 收到开仓指令 → 缓存，启动60秒超时
2. 收到欧易杠杆成功标签 → 缓存
3. 收到币安杠杆成功标签 → 缓存
4. 三个条件都齐了才开始工作（取消超时）：
   - 拷贝开仓模板
   - 读取行情数据（okx_trade_price, binance_trade_price）
   - 读取欧易面值数据（ctVal, lotSz, minSz）
   - 读取币安精度数据（stepSize, minQty）
   - 计算欧易张数 sz
   - 计算币安币数 quantity
   - 根据 direction 填充方向字段
   - 所有字段转字符串
   - 推送给下单工人
   - 清空所有缓存
5. 如果执行过程中出错，立即结束并清空缓存
6. 60秒超时未收到全部条件 → 自动清空缓存，结束流程
"""

import copy
import asyncio
import logging
import math
from typing import Dict, Any

from ..templates import OPEN_MARKET_OKX, OPEN_MARKET_BINANCE

logger = logging.getLogger(__name__)


class OpenPositionWorker:
    def __init__(self, brain):
        self.brain = brain
        self.data_manager = brain.data_manager
        
        # 缓存
        self.pending_params = None           # 开仓指令参数
        self.okx_lever_ok = False            # 欧易杠杆是否成功
        self.binance_lever_ok = False        # 币安杠杆是否成功
        
        self.okx_cache = None                # 欧易开仓参数缓存
        self.binance_cache = None            # 币安开仓参数缓存
        
        # 超时任务
        self.timeout_task = None
        
        # 数据缓存
        self.okx_trade_price = 0.0
        self.binance_trade_price = 0.0
        self.ctVal = 0.0
        self.lotSz = 0.0
        self.minSz = 0.0
        self.stepSize = 0.0
        self.minQty = 0.0
        self.sz = 0.0
        self.quantity = 0.0
        
        logger.info("🔧【开仓工人】初始化完成")
    
    def on_data(self, data: Dict[str, Any]):
        """接收大脑推送的数据"""
        # 开仓指令
        if data.get("command") == "place_order":
            logger.info("📥【开仓工人】收到开仓指令")
            # 新指令覆盖旧的，重置所有状态
            self._cleanup()
            self.pending_params = data.get("params", {})
            # 启动60秒超时
            self._start_timeout()
            asyncio.create_task(self._check_and_execute())
        
        # 信息标签
        elif "info" in data:
            info = data["info"]
            logger.info(f"📥【开仓工人】收到信息标签: {info}")
            
            if info == "欧易杠杆设置成功":
                self.okx_lever_ok = True
            elif info == "币安杠杆设置成功":
                self.binance_lever_ok = True
            
            asyncio.create_task(self._check_and_execute())
    
    def _start_timeout(self):
        """启动60秒超时任务"""
        if self.timeout_task:
            self.timeout_task.cancel()
        self.timeout_task = asyncio.create_task(self._timeout_handler())
        logger.info("⏰【开仓工人】启动60秒超时")
    
    async def _timeout_handler(self):
        """超时处理：60秒内没收到全部条件，清空所有缓存"""
        await asyncio.sleep(60)
        if self.pending_params:
            logger.warning("⏰【开仓工人】等待超时（60秒），自动清空缓存，结束流程")
            self._cleanup()
    
    def _cancel_timeout(self):
        """取消超时任务"""
        if self.timeout_task:
            self.timeout_task.cancel()
            self.timeout_task = None
            logger.info("✅【开仓工人】超时任务已取消")
    
    async def _check_and_execute(self):
        """检查三个条件是否齐了，齐了就执行"""
        if self.pending_params and self.okx_lever_ok and self.binance_lever_ok:
            logger.info("🎯【开仓工人】三个条件已齐，开始执行开仓流程")
            self._cancel_timeout()
            await self._execute()
    
    async def _execute(self):
        """执行开仓流程"""
        if not self.pending_params:
            logger.error("❌【开仓工人】没有开仓参数")
            self._cleanup()
            return
        
        logger.info("🔧【开仓工人】开始执行")
        
        # 1. 拷贝模板
        self.okx_cache = copy.deepcopy(OPEN_MARKET_OKX)
        self.binance_cache = copy.deepcopy(OPEN_MARKET_BINANCE)
        logger.info("📦【开仓工人】模板已拷贝")
        
        # 2. 读取行情数据
        if not await self._load_market_data():
            self._cleanup()
            return
        
        # 3. 读取欧易面值数据
        if not await self._load_okx_contract_info():
            self._cleanup()
            return
        
        # 4. 读取币安精度数据
        if not await self._load_binance_precision():
            self._cleanup()
            return
        
        # 5. 计算参数
        if not self._calculate_params():
            self._cleanup()
            return
        
        # 6. 填充方向字段
        self._fill_direction()
        
        # 7. 所有字段转字符串
        self._convert_to_string()
        
        # 8. 推送给下单工人
        self._send_to_trader()
        
        # 9. 清理
        self._cleanup()
        
        logger.info("✅【开仓工人】完成")
    
    async def _load_market_data(self) -> bool:
        """读取行情数据：okx_trade_price, binance_trade_price"""
        try:
            symbol = self.pending_params.get('symbol', '')
            
            result = await self.data_manager.get_public_market_data()
            market_data = result.get('data', {})
            
            symbol_data = market_data.get(symbol)
            if not symbol_data:
                logger.warning(f"⚠️【开仓工人】未找到合约 {symbol} 的行情数据")
                return False
            
            okx_price = symbol_data.get('okx_trade_price', '0')
            binance_price = symbol_data.get('binance_trade_price', '0')
            
            self.okx_trade_price = float(okx_price)
            self.binance_trade_price = float(binance_price)
            
            if self.okx_trade_price <= 0 or self.binance_trade_price <= 0:
                logger.warning(f"⚠️【开仓工人】价格异常: 欧易={self.okx_trade_price}, 币安={self.binance_trade_price}")
                return False
            
            logger.info(f"✅【开仓工人】行情数据: 欧易={self.okx_trade_price}, 币安={self.binance_trade_price}")
            return True
            
        except Exception as e:
            logger.error(f"❌【开仓工人】读取行情数据失败: {e}")
            return False
    
    async def _load_okx_contract_info(self) -> bool:
        """读取欧易面值数据：ctVal, lotSz, minSz"""
        try:
            symbol = self.pending_params.get('symbol', '')
            okx_symbol = self._convert_okx_symbol(symbol)
            
            result = await self.data_manager.get_okx_contracts_data()
            contracts = result.get('data', [])
            
            for contract in contracts:
                if contract.get('instId') == okx_symbol:
                    self.ctVal = float(contract.get('ctVal', 0))
                    self.lotSz = float(contract.get('lotSz', 0))
                    self.minSz = float(contract.get('minSz', 0))
                    
                    if self.ctVal <= 0 or self.lotSz <= 0 or self.minSz <= 0:
                        logger.warning(f"⚠️【开仓工人】欧易面值异常: ctVal={self.ctVal}, lotSz={self.lotSz}, minSz={self.minSz}")
                        return False
                    
                    logger.info(f"✅【开仓工人】欧易面值: ctVal={self.ctVal}, lotSz={self.lotSz}, minSz={self.minSz}")
                    return True
            
            logger.warning(f"⚠️【开仓工人】未找到欧易合约 {okx_symbol} 的面值数据")
            return False
            
        except Exception as e:
            logger.error(f"❌【开仓工人】读取欧易面值失败: {e}")
            return False
    
    async def _load_binance_precision(self) -> bool:
        """读取币安精度数据：stepSize, minQty"""
        try:
            symbol = self.pending_params.get('symbol', '')
            
            result = await self.data_manager.get_binance_contracts_data()
            contracts = result.get('data', [])
            
            for contract in contracts:
                if contract.get('symbol') == symbol:
                    self.stepSize = float(contract.get('stepSize', 0))
                    self.minQty = float(contract.get('minQty', 0))
                    
                    if self.stepSize <= 0 or self.minQty <= 0:
                        logger.warning(f"⚠️【开仓工人】币安精度异常: stepSize={self.stepSize}, minQty={self.minQty}")
                        return False
                    
                    logger.info(f"✅【开仓工人】币安精度: stepSize={self.stepSize}, minQty={self.minQty}")
                    return True
            
            logger.warning(f"⚠️【开仓工人】未找到币安合约 {symbol} 的精度数据")
            return False
            
        except Exception as e:
            logger.error(f"❌【开仓工人】读取币安精度失败: {e}")
            return False
    
    def _calculate_params(self) -> bool:
        """计算欧易张数和币安币数"""
        try:
            margin = float(self.pending_params.get('margin', 0))
            leverage = float(self.pending_params.get('leverage', 20))
            
            # 计算欧易张数
            # sz = floor[(保证金 * 杠杆) / (okx_trade_price * ctVal * lotSz)] * lotSz
            denominator_okx = self.okx_trade_price * self.ctVal * self.lotSz
            raw_sz = (margin * leverage) / denominator_okx
            floor_sz = math.floor(raw_sz)
            self.sz = floor_sz * self.lotSz
            
            if self.sz < self.minSz:
                logger.warning(f"⚠️【开仓工人】欧易张数 {self.sz} 小于最小下单量 {self.minSz}")
                return False
            
            # 计算币安币数
            # quantity = floor[(sz * ctVal * okx_trade_price / binance_trade_price) / stepSize] * stepSize
            okx_value = self.sz * self.ctVal * self.okx_trade_price
            raw_quantity = okx_value / self.binance_trade_price
            floor_quantity = math.floor(raw_quantity / self.stepSize)
            self.quantity = floor_quantity * self.stepSize
            
            if self.quantity < self.minQty:
                logger.warning(f"⚠️【开仓工人】币安币数 {self.quantity} 小于最小下单量 {self.minQty}")
                return False
            
            logger.info(f"✅【开仓工人】计算完成: sz={self.sz}, quantity={self.quantity}")
            return True
            
        except Exception as e:
            logger.error(f"❌【开仓工人】计算参数失败: {e}")
            return False
    
    def _fill_direction(self):
        """根据方向填充 side 和 posSide"""
        direction = self.pending_params.get('direction', '')
        
        if direction == "long_binance_short_okx":
            # 做多币安，做空欧易
            self.okx_cache['params']['side'] = "sell"
            self.okx_cache['params']['posSide'] = "short"
            self.binance_cache['params']['side'] = "BUY"
            self.binance_cache['params']['positionSide'] = "LONG"
            logger.info("📝【开仓工人】方向: 欧易做空, 币安做多")
            
        elif direction == "long_okx_short_binance":
            # 做多欧易，做空币安
            self.okx_cache['params']['side'] = "buy"
            self.okx_cache['params']['posSide'] = "long"
            self.binance_cache['params']['side'] = "SELL"
            self.binance_cache['params']['positionSide'] = "SHORT"
            logger.info("📝【开仓工人】方向: 欧易做多, 币安做空")
        
        else:
            logger.error(f"❌【开仓工人】未知方向: {direction}")
    
    def _convert_to_string(self):
        """所有字段转字符串"""
        symbol = self.pending_params.get('symbol', '')
        okx_symbol = self._convert_okx_symbol(symbol)
        
        # 欧易参数转字符串
        self.okx_cache['params']['instId'] = okx_symbol
        self.okx_cache['params']['sz'] = f"{self.sz:.8f}".rstrip('0').rstrip('.')
        
        # 币安参数转字符串
        self.binance_cache['params']['symbol'] = symbol
        self.binance_cache['params']['quantity'] = f"{self.quantity:.8f}".rstrip('0').rstrip('.')
    
    def _convert_okx_symbol(self, symbol: str) -> str:
        """转换欧易合约名：BTCUSDT → BTC-USDT-SWAP"""
        if not symbol:
            return symbol
        if '-SWAP' in symbol:
            return symbol
        if symbol.endswith('USDT'):
            base = symbol[:-4]
            return f"{base}-USDT-SWAP"
        return symbol
    
    def _send_to_trader(self):
        """推送给下单工人（通过 trader.send_orders）"""
        orders = []
        if self.okx_cache:
            orders.append(self.okx_cache)
        if self.binance_cache:
            orders.append(self.binance_cache)
        
        if orders and self.brain.trader:
            self.brain.trader.send_orders(orders)
            logger.info(f"📤【开仓工人】已推送 {len(orders)} 个订单给下单工人")
    
    def _cleanup(self):
        """清空所有缓存"""
        # 取消超时任务
        if self.timeout_task:
            self.timeout_task.cancel()
            self.timeout_task = None
        
        # 清空数据
        self.pending_params = None
        self.okx_lever_ok = False
        self.binance_lever_ok = False
        self.okx_cache = None
        self.binance_cache = None
        
        # 清空临时数据
        self.okx_trade_price = 0.0
        self.binance_trade_price = 0.0
        self.ctVal = 0.0
        self.lotSz = 0.0
        self.minSz = 0.0
        self.stepSize = 0.0
        self.minQty = 0.0
        self.sz = 0.0
        self.quantity = 0.0
        
        logger.info("🧹【开仓工人】缓存已清空")