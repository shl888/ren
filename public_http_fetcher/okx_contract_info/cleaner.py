"""
OKX合约面值清洗器 - 清洗模块
职责：接收原始合约数据，清洗出8个核心字段，推送到私人数据处理模块 和 大脑模块
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

# 导入私人数据处理模块
from private_data_processing.manager import receive_private_data

# ✅ 导入大脑模块的接收函数
from smart_brain import receive_private_data as brain_receive_private_data

logger = logging.getLogger(__name__)


class OKXContractCleaner:
    """OKX合约面值清洗器"""
    
    # 8个核心字段
    CORE_FIELDS = [
        'instId',        # 合约ID
        'ctVal',         # 面值
        'lotSz',         # 最小交易单位
        'minSz',         # 最小开仓数量
        'lever',         # 最大杠杆
        'tickSz',        # 最小价格变动
        'ctValCcy',      # 面值货币
        'settleCcy'      # 结算货币
    ]
    
    def __init__(self):
        self.last_cleaned_data = None
        self.cleaned_count = 0
        logger.info("✅ [OKX合约清洗器] 初始化完成")
    
    async def clean_and_push(self, raw_data: Dict[str, Any]) -> bool:
        """
        清洗原始数据并推送到私人数据处理模块 和 大脑模块
        
        Args:
            raw_data: 从获取器获取的原始数据
        """
        if not raw_data or 'data' not in raw_data:
            logger.error("❌ [OKX合约清洗器] 原始数据格式错误")
            return False
        
        raw_contracts = raw_data['data'].get('usdt_contracts', [])
        if not raw_contracts:
            logger.warning("⚠️ [OKX合约清洗器] 没有合约数据需要清洗")
            return False
        
        logger.info("=" * 60)
        logger.info("🧹 [OKX合约清洗器] 开始清洗数据")
        logger.info(f"📦 待清洗合约数量: {len(raw_contracts)}")
        logger.info("=" * 60)
        
        # 清洗每个合约
        cleaned_contracts = []
        for raw_contract in raw_contracts:
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
            cleaned = self._clean_single_contract(raw_contract)
            if cleaned:
                cleaned_contracts.append(cleaned)
        
        # 构建推送数据
        push_data = {
            'exchange': 'okx',
            'data_type': 'contract_info',
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
        
        # ==================== 推送目的地1：私人数据处理模块 ====================
        try:
            await receive_private_data(push_data)
            logger.info("✅ [OKX合约清洗器] 已推送到私人数据处理模块")
        except Exception as e:
            logger.error(f"❌ [OKX合约清洗器] 推送私人数据处理模块失败: {e}")
        
        # ==================== 推送目的地2：大脑模块 ====================
        try:
            await brain_receive_private_data(push_data)
            logger.info("✅ [OKX合约清洗器] 已推送到大脑模块")
        except Exception as e:
            logger.error(f"❌ [OKX合约清洗器] 推送大脑模块失败: {e}")
        
        # ==================== 日志输出 ====================
        logger.info("=" * 60)
        logger.info(f"✅ [OKX合约清洗器] 清洗并推送成功！")
        logger.info(f"📊 原始合约: {len(raw_contracts)} 个")
        logger.info(f"🧹 清洗后: {len(cleaned_contracts)} 个")
        logger.info("📤 推送目的地: 私人数据处理模块、大脑模块")
        logger.info("=" * 60)
        
        # 打印样例
        if cleaned_contracts:
            sample = cleaned_contracts[0]
            logger.info(f"📋 样例合约: {sample.get('instId')}")
            logger.info(f"   - 面值: {sample.get('ctVal')} {sample.get('ctValCcy')}")
            logger.info(f"   - 最小单位: {sample.get('lotSz')} 张")
            logger.info(f"   - 最大杠杆: {sample.get('lever')}x")
            logger.info(f"   - 结算货币: {sample.get('settleCcy')}")
        
        return True
    
    def _clean_single_contract(self, raw_contract: Dict) -> Optional[Dict]:
        """
        清洗单个合约，只保留核心字段
        """
        cleaned = {}
        
        for field in self.CORE_FIELDS:
            # 这个小循环只有8个字段，影响极小，保持同步即可
            if field in raw_contract:
                value = raw_contract[field]
                
                # 数值字段类型转换
                if field in ['ctVal', 'lotSz', 'minSz', 'tickSz']:
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        pass
                elif field == 'lever':
                    try:
                        value = int(value)
                    except (ValueError, TypeError):
                        pass
                
                cleaned[field] = value
            else:
                logger.warning(f"⚠️ 合约 {raw_contract.get('instId', 'unknown')} 缺少字段: {field}")
                cleaned[field] = None
        
        return cleaned
    
    def get_last_cleaned_data(self) -> Optional[Dict]:
        """获取最后一次清洗的数据"""
        return self.last_cleaned_data