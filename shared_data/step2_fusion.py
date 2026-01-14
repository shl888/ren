"""
第二步：数据融合与统一规格
功能：将Step1提取的5种数据源，按交易所+合约名合并成一条
输出：每个交易所每个合约一条完整数据
"""

import logging
from typing import Dict, List, Any, Optional, TYPE_CHECKING
from collections import defaultdict
from dataclasses import dataclass
import time

# 类型检查时导入，避免循环依赖
if TYPE_CHECKING:
    from step1_filter import ExtractedData

logger = logging.getLogger(__name__)

@dataclass 
class FusedData:
    """融合后的统一数据结构"""
    exchange: str
    symbol: str
    contract_name: str
    latest_price: Optional[str] = None
    funding_rate: Optional[str] = None
    last_settlement_time: Optional[int] = None      # 币安历史数据提供
    current_settlement_time: Optional[int] = None   # 实时数据提供
    next_settlement_time: Optional[int] = None      # OKX提供

class Step2Fusion:
    """第二步：数据融合"""
    
    def __init__(self):
        self.stats = defaultdict(int)
        self.fusion_stats = {
            "total_groups": 0,
            "success_groups": 0,
            "failed_groups": 0
        }
        self.last_log_time = 0
        self.log_interval = 60  # 1分钟，单位：秒
        self.process_count = 0
        # 分别记录每个交易所的详细日志计数器
        self.okx_log_counter = 0
        self.binance_log_counter = 0
    
    def process(self, step1_results: List["ExtractedData"]) -> List[FusedData]:
        """
        处理Step1的提取结果，按交易所+合约名合并
        """
        # 重置统计，避免累积
        self.fusion_stats = {
            "total_groups": 0,
            "success_groups": 0,
            "failed_groups": 0
        }
        self.stats.clear()
        
        # 频率控制：只偶尔显示处理日志
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        # 按 exchange + symbol 分组
        grouped = defaultdict(list)
        for item in step1_results:
            key = f"{item.exchange}_{item.symbol}"
            grouped[key].append(item)
        
        self.fusion_stats["total_groups"] = len(grouped)
        
        # 处理日志 - 已启用
        if should_log:
            logger.info(f"🔄【流水线步骤2】开始融合Step1输出的 {len(step1_results)} 条精简数据...")
            logger.info(f"【流水线步骤2】检测到 {len(grouped)} 个不同的交易所合约")
            
            # 统计每个交易所的合约组数
            exchange_groups = defaultdict(list)
            for key in grouped:
                exchange = key.split("_")[0] if "_" in key else "unknown"
                exchange_groups[exchange].append(key)
            
            for exchange, groups in exchange_groups.items():
                logger.info(f"【流水线步骤2】  {exchange}: {len(groups)} 个合约")
        
        # 合并每组数据
        results = []
        exchange_contracts = defaultdict(set)  # 统计成功融合的合约
        # 重置计数器
        self.okx_log_counter = 0
        self.binance_log_counter = 0
        
        # 先按交易所分组，确保能看到两个交易所的数据
        exchange_groups = {}
        for key, items in grouped.items():
            exchange = key.split("_")[0] if "_" in key else "unknown"
            if exchange not in exchange_groups:
                exchange_groups[exchange] = []
            exchange_groups[exchange].append((key, items))
        
        # 处理每个交易所的数据
        for exchange in ["okx", "binance"]:
            if exchange not in exchange_groups:
                continue
                
            # 处理日志 - 已启用
            logger.info(f"📋【流水线步骤2】处理{exchange.upper()}数据...")
            
            for key, items in exchange_groups[exchange]:
                try:
                    fused = self._merge_group(items)
                    if fused:
                        results.append(fused)
                        exchange_contracts[fused.exchange].add(fused.symbol)
                        self.stats[fused.exchange] += 1
                        self.fusion_stats["success_groups"] += 1
                        
                        # 打印详细融合结果（每个交易所最多2条）- 已启用
                        if exchange == "okx" and self.okx_log_counter < 2:
                            self._log_fused_data(fused, key, items, self.okx_log_counter + 1)
                            self.okx_log_counter += 1
                        elif exchange == "binance" and self.binance_log_counter < 2:
                            self._log_fused_data(fused, key, items, self.binance_log_counter + 1)
                            self.binance_log_counter += 1
                            
                    else:
                        self.fusion_stats["failed_groups"] += 1
                        # 打印融合失败信息（每个交易所最多2条）- 已启用
                        if exchange == "okx" and self.okx_log_counter < 2:
                            logger.warning(f"⚠️【流水线步骤2】OKX融合失败详情 {self.okx_log_counter + 1}:")
                            logger.warning(f"   合约组: {key}")
                            logger.warning(f"   源数据数量: {len(items)} 条")
                            logger.warning(f"   源数据类型: {[item.data_type for item in items]}")
                            logger.warning(f"   失败原因: 缺少必要字段或数据不完整")
                            self.okx_log_counter += 1
                        elif exchange == "binance" and self.binance_log_counter < 2:
                            logger.warning(f"⚠️【流水线步骤2】币安融合失败详情 {self.binance_log_counter + 1}:")
                            logger.warning(f"   合约组: {key}")
                            logger.warning(f"   源数据数量: {len(items)} 条")
                            logger.warning(f"   源数据类型: {[item.data_type for item in items]}")
                            logger.warning(f"   失败原因: 缺少必要字段或数据不完整")
                            self.binance_log_counter += 1
                except Exception as e:
                    self.fusion_stats["failed_groups"] += 1
                    # 打印异常失败信息 - 已启用
                    if exchange == "okx" and self.okx_log_counter < 2:
                        logger.error(f"❌【流水线步骤2】OKX融合异常 {self.okx_log_counter + 1}:")
                        logger.error(f"   合约组: {key}")
                        logger.error(f"   源数据数量: {len(items)} 条")
                        logger.error(f"   错误信息: {e}")
                        self.okx_log_counter += 1
                    elif exchange == "binance" and self.binance_log_counter < 2:
                        logger.error(f"❌【流水线步骤2】币安融合异常 {self.binance_log_counter + 1}:")
                        logger.error(f"   合约组: {key}")
                        logger.error(f"   源数据数量: {len(items)} 条")
                        logger.error(f"   错误信息: {e}")
                        self.binance_log_counter += 1
                    # 只在日志频率控制时打印错误 - 已启用
                    if should_log:
                        logger.error(f"❌【流水线步骤2】融合失败: {key} - {e}")
                    continue
        
        # 处理完成后日志 - 已启用
        if should_log:
            # 处理完成后，打印统计结果
            logger.info(f"✅【流水线步骤2】Step2融合完成，共生成 {len(results)} 条融合数据")
            
            # 按交易所统计合约数
            okx_contracts = len(exchange_contracts.get("okx", set()))
            binance_contracts = len(exchange_contracts.get("binance", set()))
            total_contracts = okx_contracts + binance_contracts
            
            logger.info("📊【流水线步骤2】融合结果合约统计:")
            if okx_contracts > 0:
                logger.info(f"  • OKX合约数: {okx_contracts} 个")
                if okx_contracts < 2:
                    logger.warning(f"⚠️【流水线步骤2】OKX只有 {okx_contracts} 个合约，少于预期2个")
            if binance_contracts > 0:
                logger.info(f"  • 币安合约数: {binance_contracts} 个")
                if binance_contracts < 2:
                    logger.warning(f"⚠️【流水线步骤2】币安只有 {binance_contracts} 个合约，少于预期2个")
            
            # 如果没有某个交易所的数据
            if okx_contracts == 0:
                logger.warning(f"⚠️【流水线步骤2】本次没有OKX融合数据")
            if binance_contracts == 0:
                logger.warning(f"⚠️【流水线步骤2】本次没有币安融合数据")
            
            logger.info(f"  • 总计: {total_contracts} 个合约")
            
            # 融合过程统计（合约组数）
            logger.info(f"📊【流水线步骤2】融合过程统计:")
            logger.info(f"  • 检测到合约组数: {self.fusion_stats['total_groups']} 组")
            logger.info(f"  • 成功融合: {self.fusion_stats['success_groups']} 组")
            logger.info(f"  • 失败/跳过: {self.fusion_stats['failed_groups']} 组")
            
            # 详细日志统计
            logger.info("📊【流水线步骤2】详细日志统计:")
            logger.info(f"  • OKX显示 {self.okx_log_counter} 条详细结果")
            logger.info(f"  • 币安显示 {self.binance_log_counter} 条详细结果")
            
            # 验证字段完整性（只针对成功融合的结果）
            if results:
                self._validate_fields(results)
            
            self.last_log_time = current_time
            # 重置计数（仅用于频率控制）
            self.process_count = 0
        
        self.process_count += 1
        
        return results
    
    def _log_fused_data(self, fused: FusedData, key: str, items: List["ExtractedData"], counter: int):
        """记录融合数据的详细日志"""
        exchange_name = "OKX" if fused.exchange == "okx" else "币安"
        logger.info(f"📝【流水线步骤2】{exchange_name}详细融合结果 {counter}:")
        logger.info(f"• 合约组: {key}")
        logger.info(f"• 融合后数据:")
        logger.info(f"• 交易所: {fused.exchange}")
        logger.info(f"• 交易对: {fused.symbol}")
        logger.info(f"• 合约名: {fused.contract_name}")
        logger.info(f"• 最新价格: {fused.latest_price}")
        logger.info(f"• 资金费率: {fused.funding_rate}")
        
        # 根据不同交易所显示不同的时间字段
        if fused.exchange == "okx":
            if fused.current_settlement_time:
                logger.info(f"• 本次结算时间: {fused.current_settlement_time} ({self._timestamp_to_str(fused.current_settlement_time)})")
            else:
                logger.info(f"• 本次结算时间: None")
            
            if fused.next_settlement_time:
                logger.info(f"• 下次结算时间: {fused.next_settlement_time} ({self._timestamp_to_str(fused.next_settlement_time)})")
            else:
                logger.info(f"• 下次结算时间: None")
                
            logger.info(f"• 上次结算时间: {fused.last_settlement_time} (OKX应为None)")
                
        elif fused.exchange == "binance":
            if fused.last_settlement_time:
                logger.info(f"• 上次结算时间: {fused.last_settlement_time} ({self._timestamp_to_str(fused.last_settlement_time)})")
            else:
                logger.info(f"• 上次结算时间: None")
            
            if fused.current_settlement_time:
                logger.info(f"• 本次结算时间: {fused.current_settlement_time} ({self._timestamp_to_str(fused.current_settlement_time)})")
            else:
                logger.info(f"• 本次结算时间: None")
                
            logger.info(f"• 下次结算时间: {fused.next_settlement_time} (币安应为None)")
        
        logger.info(f"• 融合源数据数量: {len(items)} 条")
        logger.info(f"• 源数据类型: {[item.data_type for item in items]}")
        
        # 显示源数据详情
        if len(items) <= 3:  # 如果源数据不多，显示详情
            for i, item in enumerate(items, 1):
                logger.info(f"• 源数据 {i}: {item.data_type} - {item.payload}")
    
    def _timestamp_to_str(self, timestamp: Optional[int]) -> str:
        """将时间戳转换为可读字符串"""
        if timestamp is None:
            return "None"
        try:
            import datetime
            dt = datetime.datetime.fromtimestamp(timestamp / 1000)  # 假设是毫秒时间戳
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return f"无效时间戳: {timestamp}"
    
    def _validate_fields(self, results: List[FusedData]):
        """验证字段完整性"""
        okx_valid = 0
        binance_valid = 0
        
        okx_contracts = []
        binance_contracts = []
        
        for item in results:
            if item.exchange == "okx":
                okx_contracts.append(item)
                # OKX验证：应该有next_settlement_time，没有last_settlement_time
                if item.next_settlement_time is not None and item.last_settlement_time is None:
                    okx_valid += 1
            elif item.exchange == "binance":
                binance_contracts.append(item)
                # 币安验证：应该有last_settlement_time，没有next_settlement_time
                if item.last_settlement_time is not None and item.next_settlement_time is None:
                    binance_valid += 1
        
        # 验证统计
        okx_count = len(okx_contracts)
        binance_count = len(binance_contracts)
        
        if okx_count > 0:
            validation_rate = (okx_valid / okx_count) * 100
            logger.info(f"📊【流水线步骤2】OKX合约验证:")
            logger.info(f"• 验证通过: {okx_valid}/{okx_count} ({validation_rate:.1f}%)")
            logger.info(f"• last_settlement_time正确为空: ✓")
            
        if binance_count > 0:
            validation_rate = (binance_valid / binance_count) * 100
            logger.info(f"📊【流水线步骤2】币安合约验证:")
            logger.info(f"• 验证通过: {binance_valid}/{binance_count} ({validation_rate:.1f}%)")
            logger.info(f"• next_settlement_time正确为空: ✓")
    
    def _merge_group(self, items: List["ExtractedData"]) -> Optional[FusedData]:
        """合并同一组内的所有数据"""
        if not items:
            return None
        
        # 取第一条的基础信息
        first = items[0]
        exchange = first.exchange
        symbol = first.symbol
        
        # 初始化融合结果
        fused = FusedData(
            exchange=exchange,
            symbol=symbol,
            contract_name=""
        )
        
        # 按交易所分发处理
        if exchange == "okx":
            return self._merge_okx(items, fused)
        elif exchange == "binance":
            return self._merge_binance(items, fused)
        else:
            return None
    
    def _merge_okx(self, items: List["ExtractedData"], fused: FusedData) -> Optional[FusedData]:
        """合并OKX数据：ticker + funding_rate"""
        
        for item in items:
            payload = item.payload
            
            # 提取合约名（OKX数据里都有）
            if not fused.contract_name and "contract_name" in payload:
                fused.contract_name = payload["contract_name"]
            
            # ticker数据：提取价格
            if item.data_type == "okx_ticker":
                fused.latest_price = payload.get("latest_price")
            
            # funding_rate数据：提取费率和时间
            elif item.data_type == "okx_funding_rate":
                fused.funding_rate = payload.get("funding_rate")
                fused.current_settlement_time = self._to_int(payload.get("current_settlement_time"))
                fused.next_settlement_time = self._to_int(payload.get("next_settlement_time"))
        
        # 验证：至少要有价格或费率之一
        if not any([fused.latest_price, fused.funding_rate]):
            return None
        
        return fused
    
    def _merge_binance(self, items: List["ExtractedData"], fused: FusedData) -> Optional[FusedData]:
        """合并币安数据：核心是以mark_price为准"""
        
        # 第一步：找mark_price数据（必须有）
        mark_price_item = None
        for item in items:
            if item.data_type == "binance_mark_price":
                mark_price_item = item
                break
        
        if not mark_price_item:
            return None
        
        # 从mark_price提取核心数据
        mark_payload = mark_price_item.payload
        fused.contract_name = mark_payload.get("contract_name", fused.symbol)
        fused.funding_rate = mark_payload.get("funding_rate")
        fused.current_settlement_time = self._to_int(mark_payload.get("current_settlement_time"))
        
        # 验证：mark_price必须有费率
        if fused.funding_rate is None:
            return None
        
        # ticker数据：提取价格
        for item in items:
            if item.data_type == "binance_ticker":
                fused.latest_price = item.payload.get("latest_price")
                break
        
        # funding_settlement数据：填充上次结算时间
        for item in items:
            if item.data_type == "binance_funding_settlement":
                fused.last_settlement_time = self._to_int(item.payload.get("last_settlement_time"))
                break  # 只取第一个
        
        return fused
    
    def _to_int(self, value: Any) -> Optional[int]:
        """安全转换为int"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None