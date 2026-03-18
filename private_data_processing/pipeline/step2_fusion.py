"""
第二步：融合更新
职责：
1. 预先创建空容器并缓存（binance/okx）
2. 收到数据直接更新对应容器
3. 返回副本给调度器
4. 检测到平仓后开始5秒倒计时，到时完全重置容器
"""
import time
import logging
import threading
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


# 成品数据模板
TRADE_TEMPLATE = {
    "交易所": None,
    "账户资产额": None,
    "资产币种": None,
    "保证金模式": None,
    "保证金币种": None,
    "开仓合约名": None,
    "开仓方向": None,
    "开仓执行方式": None,
    "开仓价": None,
    "持仓币数": None,
    "持仓张数": None,
    "合约面值": None,
    "开仓价仓位价值": None,
    "杠杆": None,
    "开仓保证金": None,
    "开仓手续费": None,
    "开仓手续费币种": None,
    "开仓时间": None,
    "标记价": None,
    "标记价涨跌盈亏幅": None,
    "标记价保证金": None,
    "标记价仓位价值": None,
    "标记价浮盈": None,
    "标记价浮盈百分比": None,
    "最新价": None,
    "最新价涨跌盈亏幅": None,
    "最新价保证金": None,
    "最新价仓位价值": None,
    "最新价浮盈": None,
    "最新价浮盈百分比": None,
    "止损触发方式": None,
    "止损触发价": None,
    "止损幅度": None,
    "止盈触发方式": None,
    "止盈触发价": None,
    "止盈幅度": None,
    "本次资金费": 0,
    "累计资金费": 0,
    "资金费结算次数": 0,
    "平均资金费率": None,
    "本次资金费结算时间": None,
    "平仓执行方式": None,
    "平仓价": None,
    "平仓价涨跌盈亏幅": None,
    "平仓价仓位价值": None,
    "平仓手续费": None,
    "平仓手续费币种": None,
    "平仓收益": None,
    "平仓收益率": None,
    "平仓时间": None,
}


class Step2Fusion:
    """第二步：融合更新"""
    
    def __init__(self):
        # 预先创建容器缓存（币安和欧易完全隔离）
        self.containers = {
            "binance": TRADE_TEMPLATE.copy(),
            "okx": TRADE_TEMPLATE.copy(),
        }
        self.containers["binance"]["交易所"] = "binance"
        self.containers["okx"]["交易所"] = "okx"
        
        # 线程锁保护容器
        self._lock = threading.Lock()
        
        # 重置线程标记（各自独立）
        self.reset_threads = {
            "binance": None,
            "okx": None
        }
        
        logger.info("✅【私人step2】容器缓存已创建: binance, okx")
    
    def _delayed_reset(self, exchange: str):
        """5秒后重置容器"""
        try:
            # 分段睡眠，期间可以检查是否需要取消
            for i in range(5):
                time.sleep(1)
                # 检查这个重置线程是否还是当前有效的
                with self._lock:
                    if self.reset_threads[exchange] != threading.current_thread():
                        logger.debug(f"⏰【私人step2】【{exchange}】重置线程已被取代，放弃执行")
                        return
            
            with self._lock:
                # 再次确认还是当前线程
                if self.reset_threads[exchange] != threading.current_thread():
                    return
                
                # 完全重置容器
                self._reset_container(self.containers[exchange])
                logger.info(f"🔄【私人step2】【{exchange}】5秒倒计时结束，容器已完全重置")
            
        except Exception as e:
            logger.error(f"❌【私人step2】【{exchange}】延迟重置失败: {e}")
        finally:
            with self._lock:
                if self.reset_threads[exchange] == threading.current_thread():
                    self.reset_threads[exchange] = None
    
    def _start_reset_timer(self, exchange: str):
        """启动重置定时器（确保只有一个生效）"""
        with self._lock:
            # 取消旧的线程（通过标记方式，新线程启动后旧线程会自动放弃）
            old_thread = self.reset_threads[exchange]
            if old_thread and old_thread.is_alive():
                logger.debug(f"⏰【私人step2】【{exchange}】取消旧的重置线程")
            
            # 创建并启动新线程
            thread = threading.Thread(target=self._delayed_reset, args=(exchange,))
            thread.daemon = True
            thread.start()
            self.reset_threads[exchange] = thread
            logger.info(f"⏰【私人step2】【{exchange}】重置定时器已启动，将在5秒后清理")
    
    def process(self, extracted_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理提取后的数据
        
        Args:
            extracted_data: step1提取的字段
            
        Returns:
            更新后的成品数据副本，None表示无效
        """
        exchange = extracted_data.get('交易所')
        data_type = extracted_data.get('data_type', 'unknown')
        
        logger.debug(f"🔍【私人step2】收到数据: 交易所={exchange}, 类型={data_type}")
        
        if not exchange or exchange not in self.containers:
            logger.warning(f"⚠️【私人step2】未知交易所: {exchange}")
            return None
        
        # ===== 检测是否需要启动重置定时器 =====
        # 检查平仓字段
        close_fields = ["平仓执行方式", "平仓价", "平仓收益", "平仓时间"]
        has_close_field = any(
            extracted_data.get(field) is not None 
            for field in close_fields
        )
        
        # 检查平仓事件
        event_type = extracted_data.get('event_type', '')
        is_close_event = event_type in ["_06_触发止损", "_08_触发止盈", "_10_主动平仓"]
        
        # 只要检测到平仓，就启动重置定时器（但不会立即清理）
        if has_close_field or is_close_event:
            self._start_reset_timer(exchange)
            logger.info(f"⏰【私人step2】【{exchange}】检测到平仓，启动5秒重置定时器")
        
        # ===== 更新容器（始终更新，包括平仓数据包） =====
        with self._lock:
            update_count = 0
            for key, value in extracted_data.items():
                if key in self.containers[exchange]:
                    old_value = self.containers[exchange].get(key)
                    self.containers[exchange][key] = value
                    update_count += 1
                    if exchange == 'okx':  # 保留okx的详细日志
                        logger.debug(f"📝【step2-okx】字段 {key}: {old_value} -> {value}")
            
            logger.debug(f"📊【私人step2-{exchange}】更新了 {update_count} 个字段")
            
            # 返回副本给调度器（包含最新的平仓数据）
            return self.containers[exchange].copy()
    
    def _reset_container(self, container: Dict):
        """
        完全重置容器到初始模板状态
        不保留任何字段值，就像新建的容器一样
        """
        exchange = container["交易所"]
        
        # 创建全新的模板副本
        new_container = TRADE_TEMPLATE.copy()
        new_container["交易所"] = exchange
        
        # 完全替换原容器的内容
        container.clear()
        container.update(new_container)
        
        logger.info(f"✨【私人step2】【{exchange}】容器已完全重置为初始状态")
    
    def get_container(self, exchange: str) -> Optional[Dict]:
        """获取指定交易所的容器副本（调试用）"""
        with self._lock:
            if exchange in self.containers:
                return self.containers[exchange].copy()
        return None