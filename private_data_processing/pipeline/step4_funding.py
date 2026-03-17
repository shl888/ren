"""
第四步：资金费特殊处理
==================================================
【文件职责】
1. 收到container
2. 检查缓存
3. 按对应交易所的逻辑更新缓存
4. 用更新后的缓存覆盖container
5. 返回container（给调度器）

【重要变更 - 2026.03.17】
==================================================
🔴 修改点：按明确业务逻辑重构
- 币安：根据"本次资金费结算时间"判断
- 欧易：根据"累计资金费"值变化判断
- 首次缓存只存5字段初始值
- 严格遵循：缓存→覆盖最新→输出
- 平仓后5秒清空缓存
==================================================
"""
import logging
import asyncio
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class Step4Funding:
    """第四步：资金费处理 - 按业务逻辑重构版"""
    
    def __init__(self):
        # 缓存整个container（每个交易所独立）
        self.cache = {
            "binance": None,
            "okx": None
        }
        
        # 最新数据存储（覆盖更新）
        self.latest_data = {
            "binance": None,
            "okx": None
        }
        
        # 线程锁保护缓存（仅用于缓存读写）
        self._lock = threading.Lock()
        
        # 清理线程（每个交易所独立）
        self.reset_threads = {
            "binance": None,
            "okx": None
        }
        
        # 清理倒计时秒数
        self.reset_countdown = 5
        
        # 运行标志
        self.running = True
        
        # 启动独立循环
        asyncio.create_task(self._funding_loop("binance"))
        asyncio.create_task(self._funding_loop("okx"))
        
        logger.info("✅【私人step4】资金费缓存已创建: binance, okx")
        logger.info("🔄【私人step4】独立循环已启动（按业务逻辑重构版）")
    
    # ========== 独立循环 ==========
    
    async def _funding_loop(self, exchange: str):
        """
        交易所资金费独立循环
        每次循环读取最新数据，完整走完资金费流程
        """
        logger.debug(f"🔄【私人step4】【{exchange}】资金费循环已启动")
        
        while self.running:
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU
            
            try:
                # 1. 读取最新数据
                data = self.latest_data[exchange]
                
                if data is None:
                    await asyncio.sleep(0.5)
                    continue
                
                # 2. 根据交易所选择处理逻辑
                if exchange == "binance":
                    result = self._process_binance(data)
                else:  # okx
                    result = self._process_okx(data)
                
                # 3. 将处理结果更新到latest_data
                if result:
                    self.latest_data[exchange] = result
                
                # 4. 短暂休息
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌【私人step4】【{exchange}】循环异常: {e}")
                await asyncio.sleep(1)
    
    # ========== 更新最新数据 ==========
    
    def update_data(self, exchange: str, container: Dict[str, Any]):
        """
        外部调用：更新最新数据（覆盖模式）
        这是唯一被外部调用的方法！
        """
        self.latest_data[exchange] = container
        return container
    
    # ========== process 方法 ==========
    
    def process(self, container: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理资金费数据
        只做数据更新，处理由独立循环负责
        """
        exchange = container.get("交易所")
        self.update_data(exchange, container)
        return container  # 返回原数据，让流程继续
    
    # ========== 币安资金费处理 ==========
    
    def _process_binance(self, container: Dict[str, Any]) -> Dict[str, Any]:
        """币安资金费处理逻辑 - 按业务规则"""
        exchange = "binance"
        
        with self._lock:
            # 1. 检查平仓清理
            if container.get("平仓时间") is not None:
                self._schedule_reset(exchange)
            
            # 2. 获取缓存
            cached = self.cache[exchange]
            latest = container.copy()
            
            # 3. 首次收到数据：创建缓存（只存5字段初始值）
            if cached is None:
                logger.debug(f"💰【私人step4】【{exchange}】首次收到数据，创建缓存")
                self.cache[exchange] = {
                    "本次资金费": 0,
                    "累计资金费": 0,
                    "资金费结算次数": 0,
                    "平均资金费率": None,
                    "本次资金费结算时间": None
                }
                cached = self.cache[exchange]
                # 首次直接返回最新数据
                return latest
            
            # 4. 获取本次资金费结算时间
            new_time = latest.get("本次资金费结算时间")
            
            # 5. 场景A：结算时间为空
            if not new_time:  # None 或空字符串
                logger.debug(f"💰【私人step4】【{exchange}】结算时间为空，直接返回")
                return latest
            
            # 6. 结算时间不为空
            cache_time = cached.get("本次资金费结算时间")
            
            # 场景B1：与缓存时间相同（没有新结算）
            if cache_time == new_time:
                logger.debug(f"💰【私人step4】【{exchange}】无新结算（时间相同），用缓存覆盖")
                latest["本次资金费"] = cached["本次资金费"]
                latest["累计资金费"] = cached["累计资金费"]
                latest["资金费结算次数"] = cached["资金费结算次数"]
                latest["平均资金费率"] = cached["平均资金费率"]
                latest["本次资金费结算时间"] = cached["本次资金费结算时间"]
                return latest
            
            # 场景B2：与缓存时间不同（有新结算）
            logger.debug(f"💰【私人step4】【{exchange}】检测到新结算: {cache_time} -> {new_time}")
            
            try:
                # 获取本次资金费
                this_fee = latest.get("本次资金费")
                if this_fee is None:
                    logger.warning(f"⚠️【私人step4】【{exchange}】新结算但无本次资金费")
                    return latest
                
                this_fee_float = float(this_fee)
                old_accum = float(cached.get("累计资金费") or 0)
                
                # 计算累计资金费 = 旧累计 + 新本次
                new_accum = old_accum + this_fee_float
                new_accum_rounded = self._round_4(new_accum)
                
                # 更新缓存
                # 1. 本次资金费 - 直接覆盖
                cached["本次资金费"] = this_fee_float
                
                # 2. 累计资金费 - 计算结果
                cached["累计资金费"] = new_accum_rounded
                
                # 3. 结算次数 +1
                cached["资金费结算次数"] = int(cached.get("资金费结算次数") or 0) + 1
                
                # 4. 结算时间 - 直接覆盖
                cached["本次资金费结算时间"] = new_time
                
                # 5. 计算平均资金费率
                position_value = latest.get("开仓价仓位价值")
                if position_value:
                    try:
                        pv = float(position_value)
                        if pv > 0:
                            avg_rate = new_accum_rounded * 100 / pv
                            cached["平均资金费率"] = self._round_4(avg_rate)
                    except (ValueError, TypeError, ZeroDivisionError):
                        pass
                
                # 用更新后的缓存覆盖最新数据
                latest["本次资金费"] = cached["本次资金费"]
                latest["累计资金费"] = cached["累计资金费"]
                latest["资金费结算次数"] = cached["资金费结算次数"]
                latest["平均资金费率"] = cached["平均资金费率"]
                latest["本次资金费结算时间"] = cached["本次资金费结算时间"]
                
                logger.debug(f"💰【私人step4】【{exchange}】新结算完成: 本次={this_fee_float}, 累计={new_accum_rounded}, 次数={cached['资金费结算次数']}")
                
            except (ValueError, TypeError) as e:
                logger.error(f"❌【私人step4】【{exchange}】处理资金费异常: {e}")
            
            return latest
    
    # ========== 欧易资金费处理 ==========
    
    def _process_okx(self, container: Dict[str, Any]) -> Dict[str, Any]:
        """欧易资金费处理逻辑 - 按业务规则"""
        exchange = "okx"
        
        with self._lock:
            # 1. 检查平仓清理
            if container.get("平仓时间") is not None:
                self._schedule_reset(exchange)
            
            # 2. 获取缓存
            cached = self.cache[exchange]
            latest = container.copy()
            
            # 3. 首次收到数据：创建缓存（只存5字段初始值）
            if cached is None:
                logger.debug(f"💰【私人step4】【{exchange}】首次收到数据，创建缓存")
                self.cache[exchange] = {
                    "本次资金费": 0,
                    "累计资金费": 0,
                    "资金费结算次数": 0,
                    "平均资金费率": None,
                    "本次资金费结算时间": None
                }
                cached = self.cache[exchange]
                # 首次直接返回最新数据
                return latest
            
            # 4. 获取累计资金费
            new_accum = latest.get("累计资金费")
            
            # 5. 场景A：累计资金费为0
            if new_accum in [None, 0, "", "0"] or (isinstance(new_accum, (int, float)) and new_accum == 0):
                logger.debug(f"💰【私人step4】【{exchange}】累计资金费为0，直接返回")
                return latest
            
            # 6. 累计资金费不为0
            try:
                new_accum_float = float(new_accum)
                old_accum_float = float(cached.get("累计资金费") or 0)
                
                # 场景B1：与缓存相同（没有新结算）
                if abs(new_accum_float - old_accum_float) < 0.000001:
                    logger.debug(f"💰【私人step4】【{exchange}】无新结算，用缓存覆盖")
                    latest["本次资金费"] = cached["本次资金费"]
                    latest["累计资金费"] = cached["累计资金费"]
                    latest["资金费结算次数"] = cached["资金费结算次数"]
                    latest["平均资金费率"] = cached["平均资金费率"]
                    latest["本次资金费结算时间"] = cached["本次资金费结算时间"]
                    return latest
                
                # 场景B2：与缓存不同（有新结算）
                logger.debug(f"💰【私人step4】【{exchange}】检测到新结算: {old_accum_float} -> {new_accum_float}")
                
                # 计算本次资金费 = 新累计 - 旧累计
                this_fee = new_accum_float - old_accum_float
                cached["本次资金费"] = self._round_4(this_fee)
                
                # 更新累计资金费
                cached["累计资金费"] = new_accum_float
                
                # 结算次数+1
                cached["资金费结算次数"] = int(cached.get("资金费结算次数") or 0) + 1
                
                # 结算时间 = 当前北京时间
                cached["本次资金费结算时间"] = self._get_beijing_time()
                
                # 计算平均费率
                position_value = latest.get("开仓价仓位价值")
                if position_value:
                    try:
                        pv = float(position_value)
                        if pv > 0:
                            avg_rate = new_accum_float * 100 / pv
                            cached["平均资金费率"] = self._round_4(avg_rate)
                    except (ValueError, TypeError):
                        pass
                
                # 用更新后的缓存覆盖最新数据
                latest["本次资金费"] = cached["本次资金费"]
                latest["累计资金费"] = cached["累计资金费"]
                latest["资金费结算次数"] = cached["资金费结算次数"]
                latest["平均资金费率"] = cached["平均资金费率"]
                latest["本次资金费结算时间"] = cached["本次资金费结算时间"]
                
                logger.debug(f"💰【私人step4】【{exchange}】新结算完成: 本次={cached['本次资金费']}, 累计={cached['累计资金费']}, 次数={cached['资金费结算次数']}")
                
            except (ValueError, TypeError) as e:
                logger.error(f"❌【私人step4】【{exchange}】处理资金费异常: {e}")
            
            return latest
    
    # ========== 通用工具函数 ==========
    
    def _round_4(self, value):
        """四舍五入保留4位小数"""
        if value is None:
            return None
        try:
            return round(float(value), 4)
        except (ValueError, TypeError):
            return None
    
    def _get_beijing_time(self, timestamp_ms: Optional[int] = None) -> str:
        """
        获取北京时间（仅欧易资金费结算使用）
        Args:
            timestamp_ms: 毫秒级时间戳，None表示当前时间
        Returns:
            格式化字符串: "2026.03.16 08:00:03"
        """
        try:
            if timestamp_ms is not None:
                dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            else:
                dt = datetime.now(timezone.utc)
            
            beijing_tz = timezone(timedelta(hours=8))
            beijing_time = dt.astimezone(beijing_tz)
            
            return beijing_time.strftime("%Y.%m.%d %H:%M:%S")
        except Exception as e:
            logger.error(f"❌【私人step4】时间转换失败: {e}")
            return datetime.now().strftime("%Y.%m.%d %H:%M:%S")
    
    def _reset_container(self, container: Dict):
        """完全重置容器到初始状态"""
        if container is None:
            return
        
        # 重置5个资金费字段
        container["本次资金费"] = 0
        container["累计资金费"] = 0
        container["资金费结算次数"] = 0
        container["平均资金费率"] = None
        container["本次资金费结算时间"] = None
        
        logger.debug(f"💰 资金费数据已重置")
    
    def _delayed_reset_sync(self, exchange: str):
        """同步版：延迟重置容器"""
        try:
            time.sleep(self.reset_countdown)
            
            with self._lock:
                if self.cache[exchange] is not None:
                    self._reset_container(self.cache[exchange])
                    logger.debug(f"✨【私人step4】【{exchange}】平仓清理完成，缓存已重置")
            
        except Exception as e:
            logger.error(f"❌【私人step4】【{exchange}】延迟重置失败: {e}")
        finally:
            self.reset_threads[exchange] = None
    
    def _schedule_reset(self, exchange: str):
        """启动独立线程执行清理"""
        # 如果已有线程在运行，先取消？（这里简单处理：允许新线程覆盖旧线程）
        thread = threading.Thread(target=self._delayed_reset_sync, args=(exchange,))
        thread.daemon = True
        thread.start()
        self.reset_threads[exchange] = thread
        logger.debug(f"⏰【私人step4】【{exchange}】清理线程已启动: {self.reset_countdown}秒后重置")