# trading/full_auto/auto_close.py
"""
全自动清仓工人 - 持续监控，条件触发清仓

工作流程：
1. 收到标签 {"info": "开启全自动"} → 立刻启动监控循环
2. 收到标签 {"info": "结束全自动"} → 立刻停止循环，取消所有任务，完全重置状态

主循环：
- 步骤1：拷贝平仓模板
- 步骤2：读取行情数据和私人数据
- 步骤3：填充平仓参数，有值就填充，填充完立即创建副本
- 步骤4：缓存本次资金费结算时间，检测变化并启动60秒倒计时
- 步骤5：检测清仓条件，任一触发即平仓

清仓条件：
1. 孤儿单：只有一个交易所有持仓
2. 不是套利单：合约名不同 或 方向相同 或 仓位价值差 > 100
3. 危险仓位：|标记价涨跌盈亏幅| ≥ 36 或 |最新价涨跌盈亏幅| ≥ 36
4. 费率差缩小：rate_diff ≤ 0.3
5/6. 资金费后公式：60秒倒计时结束后，综合盈亏 ≥ 0.5 或 ≤ -0.2

触发清仓后：
- 推送平仓参数副本给下单工人
- 打印日志说明触发条件
- 清空参数和数据缓存
- 保留开启全自动状态，继续下一轮循环
"""

import asyncio
import logging
import copy
from typing import Dict, Any, Optional

from ..templates import CLOSE_POSITION_OKX, CLOSE_POSITION_BINANCE

logger = logging.getLogger(__name__)


class FullAutoCloser:
    def __init__(self, brain):
        self.brain = brain
        self.data_manager = brain.data_manager
        
        # 工作状态
        self.is_active = False                  # 是否激活
        self.monitor_task = None                # 监控循环任务
        self.funding_timer_task = None          # 60秒倒计时任务
        
        # 平仓参数缓存（填充时使用）
        self.okx_close_cache = None
        self.binance_close_cache = None
        
        # 平仓参数副本（填充完成后立即创建，触发清仓时直接发送）
        self.okx_close_copy = None
        self.binance_close_copy = None
        
        # 本次资金费结算时间缓存（用于检测是否更新）
        self.cached_okx_settle_time = None
        self.cached_binance_settle_time = None
        
        # 资金费公式检测标志（60秒倒计时结束后变为True）
        self.funding_check_active = False
        
        # 当前持仓合约名（用于检测费率差）
        self.current_symbol = None
        
        # 防重入标志
        self._is_closing = False
        
        logger.info("🔚【全自动清仓工人】初始化完成")
    
    # ==================== 标签控制 ====================
    
    def on_data(self, data: Dict[str, Any]):
        """被动接收大脑推送的数据"""
        if "info" not in data:
            return
        
        info = data["info"]
        logger.info(f"📥【全自动清仓工人】收到标签: {info}")
        
        if info == "开启全自动":
            self._activate()
        elif info == "结束全自动":
            self._deactivate()
    
    def _activate(self):
        """激活，立刻开始监控"""
        if self.is_active:
            return
        
        self.is_active = True
        self._stop_monitor_task()
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("✅【全自动清仓工人】已激活，开始持续监控")
    
    def _deactivate(self):
        """立刻停止所有工作，完全重置"""
        logger.info("🛑【全自动清仓工人】收到结束全自动标签，立刻重置")
        
        self.is_active = False
        self.funding_check_active = False
        
        self._stop_monitor_task()
        self._cancel_funding_timer()
        
        self._full_cleanup()
        logger.info("🛑【全自动清仓工人】已停用，状态完全重置")
    
    def _stop_monitor_task(self):
        """停止监控任务"""
        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
            self.monitor_task = None
    
    def _cancel_funding_timer(self):
        """取消资金费倒计时任务"""
        if self.funding_timer_task and not self.funding_timer_task.done():
            self.funding_timer_task.cancel()
            self.funding_timer_task = None
    
    # ==================== 主监控循环 ====================
    
    async def _monitor_loop(self):
        """持续监控循环"""
        logger.info("🔄【全自动清仓工人】监控循环启动")
        
        while self.is_active:
            try:
                # 步骤1：拷贝平仓模板
                self._init_close_cache()
                
                # 步骤2：读取数据
                market_data, user_data = await self._fetch_data()
                if market_data is None or user_data is None:
                    await asyncio.sleep(0.5)
                    continue
                
                # 步骤3：填充平仓参数，有值就填充，填充完立即创建副本
                has_position = self._fill_close_params(user_data)
                
                # 步骤4：缓存资金费结算时间，检测变化
                self._update_settle_time_cache(user_data)
                
                # 步骤5：检测清仓条件（有持仓才检测）
                if has_position:
                    await self._check_close_conditions(market_data, user_data)
                
                await asyncio.sleep(0.5)
                
            except asyncio.CancelledError:
                logger.info("🛑【全自动清仓工人】监控循环被取消")
                break
            except Exception as e:
                logger.error(f"❌【全自动清仓工人】监控循环异常: {e}")
                await asyncio.sleep(1)
        
        logger.info("🛑【全自动清仓工人】监控循环结束")
    
    # ==================== 步骤1：拷贝模板 ====================
    
    def _init_close_cache(self):
        """拷贝平仓模板到缓存"""
        self.okx_close_cache = copy.deepcopy(CLOSE_POSITION_OKX)
        self.binance_close_cache = copy.deepcopy(CLOSE_POSITION_BINANCE)
    
    # ==================== 步骤2：读取数据 ====================
    
    async def _fetch_data(self):
        """并行读取行情数据和私人数据"""
        try:
            market_task = asyncio.create_task(self.data_manager.get_public_market_data())
            user_task = asyncio.create_task(self.data_manager.get_private_user_data())
            
            market_result, user_result = await asyncio.gather(market_task, user_task)
            
            market_data = market_result.get('data', {}) if market_result else {}
            user_data = user_result.get('data', {}) if user_result else {}
            
            return market_data, user_data
            
        except Exception as e:
            logger.error(f"❌【全自动清仓工人】读取数据失败: {e}")
            return None, None
    
    # ==================== 步骤3：填充平仓参数 ====================
    
    def _fill_close_params(self, user_data: Dict) -> bool:
        """
        填充平仓参数，有值就填充，填充完立即创建副本
        
        返回: True 表示至少有一个交易所有持仓
              False 表示两个交易所都没有持仓
        """
        okx_data = user_data.get('okx', {})
        binance_data = user_data.get('binance', {})
        
        # 提取开仓合约名
        okx_symbol = okx_data.get('开仓合约名', '')
        binance_symbol = binance_data.get('开仓合约名', '')
        
        # 记录当前持仓合约名（用于检测费率差）
        self.current_symbol = binance_symbol if binance_symbol else okx_symbol
        
        has_okx = bool(okx_symbol)
        has_binance = bool(binance_symbol)
        
        # 两个都没有持仓
        if not has_okx and not has_binance:
            self.okx_close_copy = None
            self.binance_close_copy = None
            return False
        
        # ========== 欧易有持仓，填充欧易参数，创建副本 ==========
        if has_okx:
            # 转换合约名格式：BTCUSDT → BTC-USDT-SWAP
            okx_inst_id = self._convert_okx_symbol(okx_symbol)
            # 开仓方向转小写
            okx_position_side = okx_data.get('开仓方向', '').lower()
            
            self.okx_close_cache['params']['instId'] = okx_inst_id
            self.okx_close_cache['params']['posSide'] = okx_position_side
            
            # 创建副本
            self.okx_close_copy = copy.deepcopy(self.okx_close_cache)
            logger.debug(f"📝【全自动清仓工人】欧易平仓参数已填充: {okx_inst_id}")
        else:
            self.okx_close_copy = None
        
        # ========== 币安有持仓，填充币安参数，创建副本 ==========
        if has_binance:
            # 开仓方向保持大写
            binance_position_side = binance_data.get('开仓方向', '').upper()
            # 持仓币数
            binance_quantity = binance_data.get('持仓币数', 0)
            
            self.binance_close_cache['params']['symbol'] = binance_symbol
            self.binance_close_cache['params']['positionSide'] = binance_position_side
            
            # quantity 格式化
            qty_str = f"{float(binance_quantity):.8f}".rstrip('0').rstrip('.')
            self.binance_close_cache['params']['quantity'] = qty_str
            
            # side：平仓方向与开仓方向相反
            if binance_position_side == 'LONG':
                self.binance_close_cache['params']['side'] = 'SELL'
            else:
                self.binance_close_cache['params']['side'] = 'BUY'
            
            # 创建副本
            self.binance_close_copy = copy.deepcopy(self.binance_close_cache)
            logger.debug(f"📝【全自动清仓工人】币安平仓参数已填充: {binance_symbol}")
        else:
            self.binance_close_copy = None
        
        return True
    
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
    
    # ==================== 步骤4：缓存资金费结算时间 ====================
    
    def _update_settle_time_cache(self, user_data: Dict):
        """缓存本次资金费结算时间，检测变化并启动倒计时"""
        okx_data = user_data.get('okx', {})
        binance_data = user_data.get('binance', {})
        
        # 读取当前值
        current_okx_time = okx_data.get('本次资金费结算时间')
        current_binance_time = binance_data.get('本次资金费结算时间')
        
        # 检测是否变化（只有缓存有值时才比较，首次运行不触发）
        settle_changed = False
        
        if self.cached_okx_settle_time is not None and current_okx_time != self.cached_okx_settle_time:
            settle_changed = True
            logger.info(f"📅【全自动清仓工人】欧易资金费结算时间更新")
        
        if self.cached_binance_settle_time is not None and current_binance_time != self.cached_binance_settle_time:
            settle_changed = True
            logger.info(f"📅【全自动清仓工人】币安资金费结算时间更新")
        
        # 更新缓存
        self.cached_okx_settle_time = current_okx_time
        self.cached_binance_settle_time = current_binance_time
        
        # 任意一个变了，启动60秒倒计时
        if settle_changed:
            self._start_funding_delay_timer()
    
    def _start_funding_delay_timer(self):
        """启动60秒倒计时任务"""
        self._cancel_funding_timer()
        self.funding_check_active = False
        self.funding_timer_task = asyncio.create_task(self._funding_delay_timer())
        logger.info("⏰【全自动清仓工人】启动60秒倒计时，结束后开始公式检测")
    
    async def _funding_delay_timer(self):
        """60秒倒计时，结束后开启公式检测"""
        try:
            await asyncio.sleep(60)
            if self.is_active:
                self.funding_check_active = True
                logger.info("✅【全自动清仓工人】60秒倒计时结束，开始公式检测")
        except asyncio.CancelledError:
            logger.info("🛑【全自动清仓工人】倒计时被取消")
    
    # ==================== 步骤5：检测清仓条件 ====================
    
    async def _check_close_conditions(self, market_data: Dict, user_data: Dict):
        """检测所有清仓条件，任一触发即平仓"""
        if self._is_closing:
            return
        
        okx_data = user_data.get('okx', {})
        binance_data = user_data.get('binance', {})
        
        # 条件1：孤儿单（只有一个交易所有持仓）
        if self._check_orphan(okx_data, binance_data):
            await self._execute_close("孤儿单")
            return
        
        # 两个都有持仓，继续检测其他条件
        if not self.okx_close_copy or not self.binance_close_copy:
            return
        
        # 条件2：不是套利单
        if self._check_not_arbitrage(okx_data, binance_data):
            await self._execute_close("不是套利单")
            return
        
        # 条件3：危险仓位
        if self._check_dangerous(okx_data, binance_data):
            await self._execute_close("危险仓位")
            return
        
        # 条件4：费率差缩小
        if self._check_rate_diff_small(market_data):
            await self._execute_close("费率差缩小")
            return
        
        # 条件5/6：资金费后公式
        if self.funding_check_active:
            formula_result = self._check_funding_formula(okx_data, binance_data)
            if formula_result == "profit":
                self.funding_check_active = False
                await self._execute_close("资金费后公式触发（≥0.5）")
                return
            elif formula_result == "loss":
                self.funding_check_active = False
                await self._execute_close("资金费后公式触发（≤-0.2）")
                return
    
    # -------------------- 条件1：孤儿单 --------------------
    
    def _check_orphan(self, okx_data: Dict, binance_data: Dict) -> bool:
        """检查是否孤儿单（只有一个交易所有持仓）"""
        okx_symbol = okx_data.get('开仓合约名', '')
        binance_symbol = binance_data.get('开仓合约名', '')
        
        has_okx = bool(okx_symbol)
        has_binance = bool(binance_symbol)
        
        if has_okx != has_binance:
            logger.warning(f"⚠️【全自动清仓工人】检测到孤儿单: 欧易={has_okx}, 币安={has_binance}")
            return True
        
        return False
    
    # -------------------- 条件2：不是套利单 --------------------
    
    def _check_not_arbitrage(self, okx_data: Dict, binance_data: Dict) -> bool:
        """检查是否不是套利单"""
        okx_symbol = okx_data.get('开仓合约名', '')
        binance_symbol = binance_data.get('开仓合约名', '')
        okx_side = okx_data.get('开仓方向', '').lower()
        binance_side = binance_data.get('开仓方向', '').lower()
        
        okx_value = float(okx_data.get('开仓价仓位价值', 0))
        binance_value = float(binance_data.get('开仓价仓位价值', 0))
        
        # 合约名不同
        if okx_symbol != binance_symbol:
            logger.warning(f"⚠️【全自动清仓工人】合约名不同: 欧易={okx_symbol}, 币安={binance_symbol}")
            return True
        
        # 方向相同
        if okx_side == binance_side:
            logger.warning(f"⚠️【全自动清仓工人】方向相同: 欧易={okx_side}, 币安={binance_side}")
            return True
        
        # 仓位价值差 > 100
        value_diff = abs(okx_value - binance_value)
        if value_diff > 100:
            logger.warning(f"⚠️【全自动清仓工人】仓位价值差 > 100: {value_diff:.2f}")
            return True
        
        return False
    
    # -------------------- 条件3：危险仓位 --------------------
    
    def _check_dangerous(self, okx_data: Dict, binance_data: Dict) -> bool:
        """检查是否危险仓位（涨跌幅绝对值 ≥ 36）"""
        # 欧易
        okx_mark = abs(float(okx_data.get('标记价涨跌盈亏幅', 0)))
        okx_last = abs(float(okx_data.get('最新价涨跌盈亏幅', 0)))
        
        if okx_mark >= 36:
            logger.warning(f"⚠️【全自动清仓工人】欧易标记价涨跌盈亏幅 ≥ 36: {okx_mark:.2f}")
            return True
        if okx_last >= 36:
            logger.warning(f"⚠️【全自动清仓工人】欧易最新价涨跌盈亏幅 ≥ 36: {okx_last:.2f}")
            return True
        
        # 币安
        binance_mark = abs(float(binance_data.get('标记价涨跌盈亏幅', 0)))
        binance_last = abs(float(binance_data.get('最新价涨跌盈亏幅', 0)))
        
        if binance_mark >= 36:
            logger.warning(f"⚠️【全自动清仓工人】币安标记价涨跌盈亏幅 ≥ 36: {binance_mark:.2f}")
            return True
        if binance_last >= 36:
            logger.warning(f"⚠️【全自动清仓工人】币安最新价涨跌盈亏幅 ≥ 36: {binance_last:.2f}")
            return True
        
        return False
    
    # -------------------- 条件4：费率差缩小 --------------------
    
    def _check_rate_diff_small(self, market_data: Dict) -> bool:
        """检查费率差是否 ≤ 0.3"""
        if not self.current_symbol:
            return False
        
        symbol_data = market_data.get(self.current_symbol)
        if not symbol_data:
            return False
        
        try:
            rate_diff = float(symbol_data.get('rate_diff') or 0)
            if rate_diff <= 0.3:
                logger.warning(f"⚠️【全自动清仓工人】费率差缩小: {self.current_symbol} rate_diff={rate_diff}")
                return True
        except Exception as e:
            logger.error(f"❌【全自动清仓工人】检查费率差异常: {e}")
        
        return False
    
    # -------------------- 条件5/6：资金费后公式 --------------------
    
    def _check_funding_formula(self, okx_data: Dict, binance_data: Dict) -> Optional[str]:
        """
        计算综合盈亏公式
        
        结果 = (欧易最新价浮盈 + 币安最新价浮盈) × 100 / 分母
        分母 = |最新价浮盈| 较大一方的 开仓价仓位价值
        
        返回: "profit" 表示 ≥ 0.5
              "loss" 表示 ≤ -0.2
              None 表示不满足
        """
        try:
            okx_pnl = float(okx_data.get('最新价浮盈', 0))
            binance_pnl = float(binance_data.get('最新价浮盈', 0))
            
            okx_value = float(okx_data.get('开仓价仓位价值', 0))
            binance_value = float(binance_data.get('开仓价仓位价值', 0))
            
            # 分母 = |最新价浮盈| 较大一方的开仓价仓位价值
            if abs(okx_pnl) >= abs(binance_pnl):
                denominator = okx_value
            else:
                denominator = binance_value
            
            if denominator == 0:
                return None
            
            result = (okx_pnl + binance_pnl) * 100 / denominator
            
            logger.debug(f"📊【全自动清仓工人】公式: 欧易浮盈={okx_pnl:.2f}, 币安浮盈={binance_pnl:.2f}, 分母={denominator:.2f}, 结果={result:.4f}")
            
            if result >= 0.5:
                logger.warning(f"⚠️【全自动清仓工人】公式结果 ≥ 0.5: {result:.4f}")
                return "profit"
            
            if result <= -0.2:
                logger.warning(f"⚠️【全自动清仓工人】公式结果 ≤ -0.2: {result:.4f}")
                return "loss"
            
            return None
            
        except Exception as e:
            logger.error(f"❌【全自动清仓工人】计算公式异常: {e}")
            return None
    
    # ==================== 执行清仓 ====================
    
    async def _execute_close(self, reason: str):
        """执行清仓"""
        if self._is_closing:
            return
        
        self._is_closing = True
        
        try:
            logger.info("=" * 50)
            logger.info(f"🔚【全自动清仓工人】触发清仓！原因: {reason}")
            
            # 发送准备好的副本
            orders = []
            if self.okx_close_copy:
                orders.append(self.okx_close_copy)
            if self.binance_close_copy:
                orders.append(self.binance_close_copy)
            
            if orders and self.brain.trader:
                self.brain.trader.send_orders(orders)
                logger.info(f"📤【全自动清仓工人】已推送 {len(orders)} 个平仓订单给下单工人")
            
            logger.info("=" * 50)
            
            # 清理工作缓存，保留 is_active
            self._cleanup_work()
            
        except Exception as e:
            logger.error(f"❌【全自动清仓工人】执行清仓异常: {e}")
        finally:
            self._is_closing = False
    
    # ==================== 清理 ====================
    
    def _cleanup_work(self):
        """清理本次工作缓存，保留 is_active 和结算时间缓存"""
        self.okx_close_cache = None
        self.binance_close_cache = None
        self.okx_close_copy = None
        self.binance_close_copy = None
        self.current_symbol = None
        self.funding_check_active = False
        self._cancel_funding_timer()
        logger.debug("🧹【全自动清仓工人】工作缓存已清空")
    
    def _full_cleanup(self):
        """完全重置"""
        self.okx_close_cache = None
        self.binance_close_cache = None
        self.okx_close_copy = None
        self.binance_close_copy = None
        self.cached_okx_settle_time = None
        self.cached_binance_settle_time = None
        self.funding_check_active = False
        self.current_symbol = None
        self._is_closing = False
        self._cancel_funding_timer()
        logger.info("🧹【全自动清仓工人】完全重置")