"""
缓存管理器
职责：按合约名_事件类型存文件，平仓时清理
"""
import json
import glob
import os
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

# 缓存目录
CACHE_DIR = "binance/order_update"

def ensure_cache_dir():
    """确保缓存目录存在"""
    Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)

def save_order_event(symbol: str, category: str, data: Dict[str, Any]):
    """
    保存订单事件（追加写入）
    
    Args:
        symbol: 合约名，如 HUSDT
        category: 事件分类，如 01_开仓
        data: 完整的private_data
    """
    try:
        ensure_cache_dir()
        filename = f"{CACHE_DIR}/{symbol}_{category}.json"
        
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + '\n')
            
        logger.debug(f"💾 已缓存: {symbol}_{category}")
        
    except Exception as e:
        logger.error(f"❌ 保存订单事件失败: {e}")

def clear_symbol_cache(symbol: str):
    """
    平仓时清理该合约所有缓存
    
    Args:
        symbol: 已平仓的合约名
    """
    try:
        pattern = f"{CACHE_DIR}/{symbol}_*.json"
        removed_count = 0
        
        for f in glob.glob(pattern):
            try:
                os.remove(f)
                removed_count += 1
                logger.debug(f"🧹 已清理缓存: {f}")
            except Exception as e:
                logger.error(f"清理缓存失败 {f}: {e}")
        
        if removed_count > 0:
            logger.info(f"🧹 已清理 {symbol} 的 {removed_count} 个缓存文件")
            
    except Exception as e:
        logger.error(f"❌ 清理缓存失败: {e}")

def get_symbol_cache_files(symbol: str) -> list:
    """获取某合约的所有缓存文件列表"""
    pattern = f"{CACHE_DIR}/{symbol}_*.json"
    return glob.glob(pattern)