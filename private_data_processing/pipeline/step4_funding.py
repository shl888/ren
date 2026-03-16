"""
第四步：资金费特殊处理
==================================================
【文件职责】
1. 收到container
2. 检查缓存
3. 按对应交易所的逻辑更新缓存
4. 用更新后的缓存覆盖container
5. 返回container（给调度器）

【重要变更 - 2026.03.14】
==================================================
🔴 修改点：改为独立循环+覆盖更新模式
- 每个交易所独立循环运行
- 循环内读取最新数据，完整走完资金费流程
- 数据覆盖更新，永远处理最新
- 无锁设计，互不干扰
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
    """第四步：资金费处理 - 独立循环版"""
    
    def __init__(self):
        # 缓存整个container（每个交易所独立）
        self.cache = {
            "binance": None,
            "okx": None
        }
        
        # 🔴 新增：最新数据存储（覆盖更新）
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
        
        # 🔴 新增：运行标志
        self.running = True
        
        # 🔴 新增：启动独立循环
        asyncio.create_task(self._funding_loop("binance"))
        asyncio.create_task(self._funding_loop("okx"))
        
        logger.info("✅【私人step4】资金费缓存已创建: binance, okx")
        logger.info("🔄【私人step4】独立循环已启动（覆盖更新模式）")
    
    # ========== 🔴 新增：独立循环 ==========
    
    async def _funding_loop(self, exchange: str):
        """
        交易所资金费独立循环
        每次循环读取最新数据，完整走完资金费流程
        """
        logger.info(f"🔄【私人step4】【{exchange}】资金费循环已启动")
        
        while self.running:
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU，避免CPU空转
            try:
                # ===== 1. 读取最新数据（覆盖更新）=====
                data = self.latest_data[exchange]
                
                if data is None:
                    # 没有数据就短暂休息
                    await asyncio.sleep(0.01)
                    continue
                
                # ===== 2. 根据交易所选择处理逻辑 =====
                if exchange == "binance":
                    result = self._process_binance(data)
                else:  # okx
                    result = self._process_okx(data)
                
                # ===== 3. 如果有结果，推送（异步，不等待）=====
                if result:
                    # 🔴 注意：这里不直接返回，而是让调度器通过process获取
                    # 因为调度器会调用process获取处理后的数据
                    pass
                
                # ===== 4. 短暂休息，避免CPU空转 =====
                # 可以根据需要调整，比如10ms
                await asyncio.sleep(0.01)
                
            except Exception as e:
                logger.error(f"❌【私人step4】【{exchange}】循环异常: {e}")
                import traceback
                logger.error(traceback.format_exc())
                await asyncio.sleep(0.1)
    
    # ========== 🔴 新增：更新最新数据 ==========
    
    def update_data(self, exchange: str, container: Dict[str, Any]):
        """
        外部调用：更新最新数据（覆盖模式）
        这是唯一被外部调用的方法！
        """
        self.latest_data[exchange] = container
        logger.debug(f"📥【私人step4】【{exchange}】收到新数据，已覆盖")
        return container  # 立即返回，不处理！
    
    # ========== 原有 process 方法（修改为只更新数据）==========
    
    def process(self, container: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理资金费数据
        🔴【修改点】现在只做数据更新，不处理！
        处理由独立循环负责
        
        根据交易所选择不同的处理逻辑
        """
        exchange = container.get("交易所")
        
        # 🔴 只更新数据，不处理！
        self.update_data(exchange, container)
        
        # 返回原数据（让流程继续）
        return container
    
    # ========== 币安房间（原有代码，一字不改）==========
    
    def _process_binance(self, container: Dict[str, Any]) -> Dict[str, Any]:
        """币安资金费处理逻辑"""
        exchange = "binance"
        
        with self._lock:
            # 获取缓存的container
            cached = self.cache[exchange]
            
            # 获取本次的结算时间
            new_time = container.get("本次资金费结算时间")
            
            # ===== 第一次收到数据 =====
            if cached is None:
                logger.debug(f"💰【私人step4】【{exchange}】首次收到数据，直接缓存")
                self.cache[exchange] = container.copy()
                # 注意：时间已经在Step1转换好了，这里不再转换
                result = container.copy()
            
            # 获取缓存的结算时间
            cache_time = cached.get("本次资金费结算时间") if cached else None
            
            # ===== 场景1：缓存时间为空 =====
            if cache_time is None and cached is not None:
                # 场景1a：新时间也为空
                if new_time is None:
                    logger.debug(f"💰【私人step4】【{exchange}】场景1a：无结算，全字段覆盖缓存")
                    self.cache[exchange] = container.copy()
                    
                # 场景1b：新时间有值（首次结算）
                else:
                    logger.debug(f"💰【私人step4】【{exchange}】场景1b：首次结算，时间={new_time}")
                    # 先缓存新数据
                    self.cache[exchange] = container.copy()
                    # 更新5个资金费字段（累加）
                    self._update_funding_fields_binance(self.cache[exchange], cached, is_first=True)
            
            # ===== 场景2：缓存时间有值 =====
            elif cache_time is not None and cached is not None:
                # 场景2c：时间相同
                if cache_time == new_time:
                    logger.debug(f"💰【私人step4】【{exchange}】场景2c：同次结算，资金费字段保持")
                    # 其他字段覆盖缓存
                    self._update_other_fields(cached, container)
                    self.cache[exchange] = cached
                
                # 场景2d：时间不同（新结算）
                elif new_time is not None:
                    logger.debug(f"💰【私人step4】【{exchange}】场景2d：新结算，时间={new_time}")
                    # 先缓存新数据
                    self.cache[exchange] = container.copy()
                    # 更新5个资金费字段（累加）
                    self._update_funding_fields_binance(self.cache[exchange], cached, is_first=False)
            
            # ===== 检查平仓并启动线程清理 =====
            if container.get("平仓时间") is not None:
                self._schedule_reset(exchange)
            
            # ===== 返回更新后的缓存 =====
            result = self.cache[exchange].copy() if self.cache[exchange] else container.copy()
        
        return result
    
    def _update_funding_fields_binance(self, new_cache: Dict, old_cache: Dict, is_first: bool):
        """币安：更新5个资金费字段"""
        new_fee = new_cache.get("本次资金费")
        
        if is_first:
            old_total = 0
        else:
            old_total = float(old_cache.get("累计资金费") or 0)
        
        try:
            new_fee_float = float(new_fee) if new_fee is not None else 0
            new_total = old_total + new_fee_float
            new_cache["累计资金费"] = self._round_4(new_total)
            
            if is_first:
                new_cache["资金费结算次数"] = 1
            else:
                old_count = int(old_cache.get("资金费结算次数") or 0)
                new_cache["资金费结算次数"] = old_count + 1
            
            position_value = new_cache.get("开仓价仓位价值")
            if position_value is not None:
                try:
                    pv = float(position_value)
                    if pv > 0:
                        avg_rate = (new_total * 100) / pv
                        new_cache["平均资金费率"] = self._round_4(avg_rate)
                except (ValueError, TypeError, ZeroDivisionError):
                    pass
        except (ValueError, TypeError):
            pass
    
    # ========== 欧易房间（修改后）==========
    
    def _process_okx(self, container: Dict[str, Any]) -> Dict[str, Any]:
        """欧易资金费处理逻辑"""
        exchange = "okx"
        
        with self._lock:
            # 获取缓存的container
            cached = self.cache[exchange]
            
            # ===== 第一次收到数据 =====
            if cached is None:
                logger.debug(f"💰【私人step4】【{exchange}】首次收到数据，初始化缓存")
                # 缓存 = 新数据
                self.cache[exchange] = container.copy()
                # 初始化5个资金费字段
                self.cache[exchange]["本次资金费"] = 0
                self.cache[exchange]["累计资金费"] = 0
                self.cache[exchange]["资金费结算次数"] = 0
                self.cache[exchange]["平均资金费率"] = None
                self.cache[exchange]["本次资金费结算时间"] = None
                # 返回原数据
                result = container.copy()
            
            # ===== 非第一次：用新数据更新缓存 =====
            else:
                # 1. 先处理5个特殊字段以外的其他字段（直接覆盖）
                funding_fields = [
                    "本次资金费", "累计资金费", "资金费结算次数",
                    "平均资金费率", "本次资金费结算时间"
                ]
                
                # ✅ [蚂蚁基因修复] 使用items()迭代器，每次迭代让出（但这里是同步方法，不能await）
                # 由于container通常很小（几十个字段），这个循环很快，不会造成阻塞
                for key, value in container.items():
                    if key not in funding_fields:
                        cached[key] = value
                
                # 2. 处理累计资金费（判断是否有新结算）
                old_total = cached.get("累计资金费", 0)
                new_total = container.get("累计资金费")
                
                # 如果新数据中有累计资金费
                if new_total is not None:
                    try:
                        old_total_float = float(old_total)
                        new_total_float = float(new_total)
                        
                        # 判断是否不同（有新结算）
                        if abs(new_total_float - old_total_float) >= 0.000001:
                            logger.info(f"💰【私人step4】【{exchange}】检测到资金费结算: {old_total_float} -> {new_total_float}")
                            
                            # 2.1 本次资金费 = 新累计 - 旧累计
                            this_fee = new_total_float - old_total_float
                            cached["本次资金费"] = self._round_4(this_fee)
                            
                            # 2.2 累计资金费 = 新累计
                            cached["累计资金费"] = new_total_float
                            
                            # 2.3 结算次数 +1
                            cached["资金费结算次数"] = int(cached.get("资金费结算次数") or 0) + 1
                            
                            # 2.4 结算时间 = 当前北京时间
                            cached["本次资金费结算时间"] = self._get_beijing_time()
                            
                            # 2.5 平均资金费率 = （累计资金费 *100）/ 开仓价仓位价值
                            position_value = cached.get("开仓价仓位价值")
                            if position_value is not None:
                                try:
                                    pv = float(position_value)
                                    if pv > 0:
                                        avg_rate = cached["累计资金费"] * 100 / pv
                                        cached["平均资金费率"] = self._round_4(avg_rate)
                                except (ValueError, TypeError, ZeroDivisionError):
                                    pass
                    except (ValueError, TypeError):
                        pass
                
                # 更新缓存
                self.cache[exchange] = cached
                result = cached.copy()
            
            # ===== 检查平仓并启动线程清理 =====
            if container.get("平仓时间") is not None:
                self._schedule_reset(exchange)
        
        return result
    
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
    
    def _reset_funding_fields(self, cache_entry: Dict):
        """重置资金费相关字段"""
        cache_entry["本次资金费"] = 0
        cache_entry["累计资金费"] = 0
        cache_entry["资金费结算次数"] = 0
        cache_entry["平均资金费率"] = None
        cache_entry["本次资金费结算时间"] = None
        logger.info(f"💰 资金费数据已重置")
    
    def _update_other_fields(self, cached: Dict, new_data: Dict):
        """更新非资金费字段 - 直接覆盖，空值也覆盖"""
        funding_fields = [
            "本次资金费", "累计资金费", "资金费结算次数",
            "平均资金费率", "本次资金费结算时间"
        ]
        
        # ✅ [蚂蚁基因修复] 这个循环在锁内执行，但数据量很小，保持原样
        for key, value in new_data.items():
            if key not in funding_fields:
                cached[key] = value
    
    def _reset_container(self, container: Dict):
        """完全重置容器到初始状态"""
        # 清空所有字段
        for key in list(container.keys()):
            if key in ["本次资金费", "累计资金费", "资金费结算次数"]:
                container[key] = 0
            else:
                container[key] = None
        
        # 保留交易所字段
        exchange = container.get("交易所")
        if exchange:
            container["交易所"] = exchange
    
    def _delayed_reset_sync(self, exchange: str):
        """同步版：延迟重置容器"""
        try:
            time.sleep(self.reset_countdown)
            
            with self._lock:
                if self.cache[exchange] is not None:
                    self._reset_container(self.cache[exchange])
                    logger.info(f"✨【私人step4】【{exchange}】平仓清理完成，容器已重置")
            
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
        logger.info(f"⏰【私人step4】【{exchange}】清理线程已启动: {self.reset_countdown}秒后重置")