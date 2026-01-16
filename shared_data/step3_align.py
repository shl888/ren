"""
第三步：筛选双平台合约 + 时间转换（精确匹配版）
功能：1. 精确匹配OKX和币安都有的合约 2. UTC时间戳转UTC+8 3. 转24小时制字符串
修正：使用精确匹配逻辑，避免错误匹配（如BTC不会匹配到BTCDOM）
"""

import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

@dataclass
class AlignedData:
    """对齐后的数据结构"""
    symbol: str
    okx_contract_name: Optional[str] = None
    binance_contract_name: Optional[str] = None
    
    # OKX数据
    okx_price: Optional[str] = None
    okx_funding_rate: Optional[str] = None
    okx_last_settlement: Optional[str] = None
    okx_current_settlement: Optional[str] = None
    okx_next_settlement: Optional[str] = None
    
    # 币安数据
    binance_price: Optional[str] = None
    binance_funding_rate: Optional[str] = None
    binance_last_settlement: Optional[str] = None
    binance_current_settlement: Optional[str] = None
    binance_next_settlement: Optional[str] = None
    
    # 时间戳备份（用于后续计算）
    okx_current_ts: Optional[int] = None
    okx_next_ts: Optional[int] = None
    binance_current_ts: Optional[int] = None
    binance_last_ts: Optional[int] = None

class Step3Align:
    """第三步：双平台对齐 + 时间转换（精确匹配版）"""
    
    def __init__(self):
        self.last_log_time = 0
        self.log_interval = 300  # 5分钟，单位：秒
        self.process_count = 0
    
    def process(self, fused_results: List) -> List[AlignedData]:
        """处理Step2的融合结果 - 使用精确匹配"""
        # 频率控制：只偶尔显示处理日志
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        # 处理前日志 - 只在频率控制时打印
        if should_log:
            logger.info(f"🔄【流水线步骤3】开始精确匹配step2输出的 {len(fused_results)} 条融合数据...")
        
        # 按交易所分组
        okx_items = []
        binance_items = []
        
        for item in fused_results:
            if item.exchange == "okx":
                okx_items.append(item)
            elif item.exchange == "binance":
                binance_items.append(item)
        
        # 提取币种映射（精确提取）
        okx_coin_to_item = self._extract_okx_coins(okx_items)
        binance_coin_to_item = self._extract_binance_coins(binance_items)
        
        # 找出共同币种（精确匹配）
        common_coins = sorted(list(set(okx_coin_to_item.keys()) & set(binance_coin_to_item.keys())))
        
        # 找出各自独有的币种
        okx_only_coins = sorted(list(set(okx_coin_to_item.keys()) - set(binance_coin_to_item.keys())))
        binance_only_coins = sorted(list(set(binance_coin_to_item.keys()) - set(okx_coin_to_item.keys())))
        
        # 只保留双平台都有的合约（精确匹配）
        align_results = []
        time_conversion_errors = 0
        match_errors = []
        
        for coin in common_coins:
            okx_item = okx_coin_to_item.get(coin)
            binance_item = binance_coin_to_item.get(coin)
            
            if okx_item and binance_item:
                # 验证匹配是否合理（防止错误匹配）
                is_valid = self._validate_match(okx_item, binance_item, coin)
                
                if is_valid:
                    try:
                        aligned = self._align_item(coin, okx_item, binance_item)
                        if aligned:
                            align_results.append(aligned)
                            
                            # 统计时间转换错误
                            if (okx_item.current_settlement_time and not aligned.okx_current_settlement) or \
                               (okx_item.next_settlement_time and not aligned.okx_next_settlement) or \
                               (binance_item.last_settlement_time and not aligned.binance_last_settlement) or \
                               (binance_item.current_settlement_time and not aligned.binance_current_settlement):
                                time_conversion_errors += 1
                                
                    except Exception as e:
                        # 只在频率控制时打印错误
                        if should_log:
                            logger.error(f"❌【流水线步骤3】对齐失败: {coin} - {e}")
                        continue
                else:
                    match_errors.append({
                        "coin": coin,
                        "okx_contract": okx_item.contract_name,
                        "binance_contract": binance_item.contract_name
                    })
        
        # 处理后日志 - 只在频率控制时打印
        if should_log:
            logger.info(f"📊【流水线步骤3】精确匹配统计:")
            logger.info(f"  • OKX合约数: {len(okx_items)} 个")
            logger.info(f"  • 币安合约数: {len(binance_items)} 个")
            logger.info(f"  • 共同币种数: {len(common_coins)} 个")
            logger.info(f"  • 仅OKX币种: {len(okx_only_coins)} 个")
            logger.info(f"  • 仅币安币种: {len(binance_only_coins)} 个")
            logger.info(f"  • 对齐成功: {len(align_results)} 个")
            
            if match_errors:
                logger.warning(f"⚠️【流水线步骤3】发现 {len(match_errors)} 个疑似错误匹配:")
                for error in match_errors[:5]:  # 只显示前5个
                    logger.warning(f"  {error['coin']}: OKX={error['okx_contract']}, 币安={error['binance_contract']}")
                if len(match_errors) > 5:
                    logger.warning(f"  ... 还有 {len(match_errors) - 5} 个未显示")
            
            # 时间转换统计
            if time_conversion_errors == 0:
                logger.info(f"✅【流水线步骤3】时间转换: 全部 {len(align_results)} 个合约转换成功")
            else:
                logger.warning(f"⚠️【流水线步骤3】时间转换: {time_conversion_errors} 个合约存在转换错误")
            
            self.last_log_time = current_time
            # 重置计数（仅用于频率控制）
            self.process_count = 0
        
        self.process_count += 1
        
        return align_results
    
    def _extract_okx_coins(self, okx_items: List) -> Dict[str, any]:
        """从OKX合约中提取币种映射"""
        coin_to_item = {}
        
        for item in okx_items:
            contract_name = item.contract_name
            if contract_name and "-USDT-SWAP" in contract_name:
                # 精确提取币种（去掉 -USDT-SWAP）
                coin = contract_name.replace("-USDT-SWAP", "")
                if coin not in coin_to_item:
                    coin_to_item[coin] = item
        
        return coin_to_item
    
    def _extract_binance_coins(self, binance_items: List) -> Dict[str, any]:
        """从币安合约中提取币种映射"""
        coin_to_item = {}
        
        for item in binance_items:
            contract_name = item.contract_name
            if contract_name and contract_name.endswith("USDT"):
                # 精确提取币种（去掉 USDT）
                coin = contract_name[:-4]  # 去掉最后4个字符"USDT"
                if coin not in coin_to_item:
                    coin_to_item[coin] = item
        
        return coin_to_item
    
    def _validate_match(self, okx_item, binance_item, coin: str) -> bool:
        """验证币种匹配是否合理"""
        # 验证OKX合约名格式
        okx_contract = okx_item.contract_name
        if not okx_contract or "-USDT-SWAP" not in okx_contract:
            logger.debug(f"OKX合约名格式异常: {okx_contract}")
            return False
        
        # 验证币安合约名格式
        binance_contract = binance_item.contract_name
        if not binance_contract or not binance_contract.endswith("USDT"):
            logger.debug(f"币安合约名格式异常: {binance_contract}")
            return False
        
        # 验证币种是否匹配
        extracted_okx_coin = okx_contract.replace("-USDT-SWAP", "")
        extracted_binance_coin = binance_contract[:-4]  # 去掉"USDT"
        
        if extracted_okx_coin != extracted_binance_coin:
            logger.warning(f"币种提取不一致: 预期={coin}, OKX提取={extracted_okx_coin}, 币安提取={extracted_binance_coin}")
            return False
        
        # 特殊检查：防止部分匹配
        # 例如：coin="BTC"，但合约可能是"BTCUSDT"（正确）或"BTCDOMUSDT"（错误）
        if extracted_okx_coin != coin:
            logger.warning(f"币种不匹配: 预期={coin}, 实际={extracted_okx_coin}")
            return False
        
        # 检查是否为常见的错误匹配
        common_mistakes = [
            ("BTC", "BTCDOM"),  # BTC不应该匹配BTCDOM
            ("PUMP", "PUMPBTC"),  # PUMP不应该匹配PUMPBTC
            ("BABY", "BABYDOGE"),  # BABY不应该匹配BABYDOGE
            ("DOGE", "BABYDOGE"),  # DOGE不应该匹配BABYDOGE
        ]
        
        for correct_coin, wrong_coin in common_mistakes:
            if coin == correct_coin and extracted_binance_coin == wrong_coin:
                logger.warning(f"发现常见错误匹配: {correct_coin} 匹配到了 {wrong_coin}")
                return False
        
        return True
    
    def _align_item(self, symbol: str, okx_item, binance_item) -> Optional[AlignedData]:
        """对齐单个合约"""
        
        aligned = AlignedData(symbol=symbol)
        
        # OKX数据
        if okx_item:
            aligned.okx_contract_name = okx_item.contract_name
            aligned.okx_price = okx_item.latest_price
            aligned.okx_funding_rate = okx_item.funding_rate
            aligned.okx_current_ts = okx_item.current_settlement_time
            aligned.okx_next_ts = okx_item.next_settlement_time
            
            # 时间转换：UTC时间戳 -> UTC+8 -> 24小时字符串
            aligned.okx_current_settlement = self._ts_to_str(okx_item.current_settlement_time)
            aligned.okx_next_settlement = self._ts_to_str(okx_item.next_settlement_time)
            aligned.okx_last_settlement = None
        
        # 币安数据
        if binance_item:
            aligned.binance_contract_name = binance_item.contract_name
            aligned.binance_price = binance_item.latest_price
            aligned.binance_funding_rate = binance_item.funding_rate
            aligned.binance_last_ts = binance_item.last_settlement_time
            aligned.binance_current_ts = binance_item.current_settlement_time
            
            # 时间转换
            aligned.binance_last_settlement = self._ts_to_str(binance_item.last_settlement_time)
            aligned.binance_current_settlement = self._ts_to_str(binance_item.current_settlement_time)
            aligned.binance_next_settlement = None
        
        return aligned
    
    def _ts_to_str(self, ts: Optional[int]) -> Optional[str]:
        """时间戳转换：UTC毫秒 -> UTC+8 -> 24小时制字符串"""
        # 增加无效值检查
        if ts is None or ts <= 0:  # 无效或负值时间戳
            return None
        
        try:
            # 1. 先拿到纯UTC时间（关键！用utcfromtimestamp）
            dt_utc = datetime.utcfromtimestamp(ts / 1000)
            
            # 2. 加8小时到北京
            dt_bj = dt_utc + timedelta(hours=8)
            
            # 3. 转24小时字符串
            return dt_bj.strftime("%Y-%m-%d %H:%M:%S")
        
        except Exception as e:
            return None