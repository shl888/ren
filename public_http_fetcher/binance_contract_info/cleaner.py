"""
币安合约精度清洗器 - 清洗模块
职责：接收原始合约数据，清洗出4个核心字段，推送到大脑模块
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

# ✅ 导入大脑模块的接收函数
from smart_brain import receive_private_data as brain_receive_private_data

logger = logging.getLogger(__name__)


class BinanceContractCleaner:
    """币安合约精度清洗器"""
    
    # 4个核心字段
    CORE_FIELDS = [
        'symbol',       # 合约ID，如 BTCUSDT
        'stepSize',     # 最小交易步长（精度），从 LOT_SIZE filter 获取
        'minQty',       # 最小开仓数量，从 LOT_SIZE filter 获取
        'minNotional'   # 最小名义价值，从 MIN_NOTIONAL filter 获取
    ]
    
    def __init__(self):
        self.last_cleaned_data = None
        self.cleaned_count = 0
        logger.info("✅ [币安合约精度清洗器] 初始化完成")
    
    async def clean_and_push(self, raw_data: Dict[str, Any]) -> bool:
        """
        清洗原始数据并推送到大脑模块
        
        Args:
            raw_data: 从获取器获取的原始数据
        """
        if not raw_data or 'data' not in raw_data:
            logger.error("❌ [币安合约精度清洗器] 原始数据格式错误")
            return False
        
        raw_contracts = raw_data['data'].get('perpetual_contracts', [])
        if not raw_contracts:
            logger.warning("⚠️ [币安合约精度清洗器] 没有合约数据需要清洗")
            return False
        
        logger.info("=" * 60)
        logger.info("🧹 [币安合约精度清洗器] 开始清洗数据")
        logger.info(f"📦 待清洗合约数量: {len(raw_contracts)}")
        logger.info("=" * 60)
        
        # 清洗每个合约
        cleaned_contracts = []
        for raw_contract in raw_contracts:
            await asyncio.sleep(0)
            cleaned = self._clean_single_contract(raw_contract)
            if cleaned:
                cleaned_contracts.append(cleaned)
        
        # ==================== 构建推送数据（与OKX格式对齐）====================
        push_data = {
            'exchange': 'binance',
            'data_type': 'contract_info',      # ✅ 与OKX一致，大脑识别为参考数据
            'timestamp': datetime.now().isoformat(),
            'data': {
                'total_contracts': len(cleaned_contracts),
                'contracts': cleaned_contracts,
                'core_fields': self.CORE_FIELDS
            }
        }
        
        # 保存最后清洗的数据
        self.last_cleaned_data = push_data
        self.cleaned_count = len(cleaned_contracts)
        
        # ==================== 推送目的地：大脑模块 ====================
        try:
            await brain_receive_private_data(push_data)
            logger.debug("✅ [币安合约精度清洗器] 已推送到大脑模块")
        except Exception as e:
            logger.error(f"❌ [币安合约精度清洗器] 推送大脑模块失败: {e}")
            return False
        
        # ==================== 日志输出 ====================
        logger.info("=" * 60)
        logger.info(f"✅ [币安合约精度清洗器] 清洗并推送成功！")
        logger.info(f"📊 原始合约: {len(raw_contracts)} 个")
        logger.info(f"🧹 清洗后: {len(cleaned_contracts)} 个")
        logger.info("📤 推送目的地: 大脑模块")
        logger.info("=" * 60)
        
        # 打印样例
        if cleaned_contracts:
            sample = cleaned_contracts[0]
            logger.info(f"📋 样例合约: {sample.get('symbol')}")
            logger.info(f"   - 步长(stepSize): {sample.get('stepSize')}")
            logger.info(f"   - 最小数量(minQty): {sample.get('minQty')}")
            logger.info(f"   - 最小名义值(minNotional): {sample.get('minNotional')}")
        
        return True
    
    def _clean_single_contract(self, raw_contract: Dict) -> Optional[Dict]:
        """
        清洗单个合约，只保留核心字段
        从 filters 数组中提取 LOT_SIZE 和 MIN_NOTIONAL
        """
        cleaned = {}
        
        # 1. 获取 symbol
        symbol = raw_contract.get('symbol')
        if not symbol:
            logger.warning("⚠️ 合约缺少 symbol 字段")
            return None
        cleaned['symbol'] = symbol
        
        # 2. 从 filters 中提取 LOT_SIZE 和 MIN_NOTIONAL
        filters = raw_contract.get('filters', [])
        
        lot_size_filter = None
        min_notional_filter = None
        
        for f in filters:
            filter_type = f.get('filterType')
            if filter_type == 'LOT_SIZE':
                lot_size_filter = f
            elif filter_type == 'MIN_NOTIONAL':
                min_notional_filter = f
        
        # 3. 提取 stepSize 和 minQty（从 LOT_SIZE）
        if lot_size_filter:
            try:
                cleaned['stepSize'] = float(lot_size_filter.get('stepSize', 0))
                cleaned['minQty'] = float(lot_size_filter.get('minQty', 0))
            except (ValueError, TypeError):
                cleaned['stepSize'] = 0.0
                cleaned['minQty'] = 0.0
                logger.warning(f"⚠️ 合约 {symbol} LOT_SIZE 字段格式异常")
        else:
            cleaned['stepSize'] = 0.0
            cleaned['minQty'] = 0.0
            logger.warning(f"⚠️ 合约 {symbol} 缺少 LOT_SIZE filter")
        
        # 4. 提取 minNotional（从 MIN_NOTIONAL）
        if min_notional_filter:
            try:
                cleaned['minNotional'] = float(min_notional_filter.get('notional', 0))
            except (ValueError, TypeError):
                cleaned['minNotional'] = 0.0
                logger.warning(f"⚠️ 合约 {symbol} MIN_NOTIONAL 字段格式异常")
        else:
            cleaned['minNotional'] = 0.0
            logger.warning(f"⚠️ 合约 {symbol} 缺少 MIN_NOTIONAL filter")
        
        return cleaned
    
    def get_last_cleaned_data(self) -> Optional[Dict]:
        """获取最后一次清洗的数据"""
        return self.last_cleaned_data
        