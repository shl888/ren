"""
原始私人数据缓存 - 保存最新一份原始数据，供调试查看
"""
import json
import os
import asyncio
from datetime import datetime
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class RawDataCache:
    """原始数据缓存管理器"""
    
    def __init__(self, cache_dir: str = "private_data_cache"):
        self.cache_dir = cache_dir
        self.latest_data = {
            'binance': {},
            'okx': {}
        }
        
        # 确保缓存目录存在
        os.makedirs(cache_dir, exist_ok=True)
        for exchange in ['binance', 'okx']:
            os.makedirs(os.path.join(cache_dir, exchange), exist_ok=True)
        
        logger.info(f"[原始缓存] 初始化完成，缓存目录: {cache_dir}")
    
    async def save(self, exchange: str, data_type: str, raw_data: Dict[str, Any]):
        """保存原始数据（新的覆盖旧的）"""
        try:
            timestamp = datetime.now().isoformat()
            cache_entry = {
                'exchange': exchange,
                'data_type': data_type,
                'timestamp': timestamp,
                'raw_data': raw_data
            }
            
            # 1. 更新内存中的最新数据
            self.latest_data[exchange][data_type] = cache_entry
            
            # 2. 保存到文件（latest.json）
            latest_file = os.path.join(self.cache_dir, exchange, 'latest.json')
            with open(latest_file, 'w', encoding='utf-8') as f:
                json.dump(cache_entry, f, indent=2, ensure_ascii=False)
            
            # 3. 按类型保存历史记录（可选）
            type_dir = os.path.join(self.cache_dir, exchange, data_type)
            os.makedirs(type_dir, exist_ok=True)
            
            # 使用时间戳作为文件名，避免重复
            filename = f"{timestamp.replace(':', '-').replace('.', '-')}.json"
            type_file = os.path.join(type_dir, filename)
            
            with open(type_file, 'w', encoding='utf-8') as f:
                json.dump(raw_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"[原始缓存] 已保存 {exchange}.{data_type}")
            
        except Exception as e:
            logger.error(f"[原始缓存] 保存数据失败: {e}")
    
    def get_latest(self, exchange: str = None):
        """获取最新的缓存数据"""
        if exchange:
            return self.latest_data.get(exchange, {})
        return self.latest_data
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        stats = {
            'total_entries': 0,
            'exchanges': {}
        }
        
        for exchange in ['binance', 'okx']:
            count = len(self.latest_data[exchange])
            stats['exchanges'][exchange] = {
                'data_types_count': count,
                'data_types': list(self.latest_data[exchange].keys())
            }
            stats['total_entries'] += count
        
        return stats
        
        