"""
数据完成部门 - HTTP路由
用于从浏览器查看模块内部存储的数据状态
"""
from fastapi import APIRouter
import logging
from datetime import datetime

# 导入数据完成部门的接收器单例
from data_completion_department.receiver import get_receiver

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/completion", tags=["Data Completion"])


@router.get("/status")
async def get_completion_status():
    """
    获取数据完成部门当前存储的所有数据
    直接返回内存存储区的内容，方便调试查看
    """
    try:
        receiver = get_receiver()
        
        # 获取存储区数据
        store = receiver.memory_store
        
        # 构建返回数据
        result = {
            "timestamp": datetime.now().isoformat(),
            "private_data": store.get('private_data'),  # 包含okx和binance的单条数据
            "market_data": store.get('market_data'),
            "recent_received": store.get('all_received', [])[-5:]  # 最近5条接收记录
        }
        
        # 添加统计信息
        if result["private_data"]:
            private = result["private_data"].get('data', {})
            result["private_stats"] = {
                "has_okx": "okx" in private,
                "has_binance": "binance" in private,
                "okx_symbol": private.get("okx", {}).get("开仓合约名"),
                "binance_symbol": private.get("binance", {}).get("开仓合约名")
            }
        
        if result["market_data"] and result["market_data"].get('data'):
            market_data = result["market_data"]["data"]
            result["market_stats"] = {
                "total_contracts": market_data.get('total_contracts', 0),
                "sample_symbols": list(market_data.keys())[:5]
            }
        
        logger.info("📊 数据完成部门状态已通过路由查看")
        return result
        
    except Exception as e:
        logger.error(f"❌ 获取完成部门状态失败: {e}")
        return {
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }


@router.get("/status/private")
async def get_private_data():
    """查看私人数据（包含okx和binance的单条数据）"""
    receiver = get_receiver()
    return receiver.memory_store.get('private_data', {"note": "暂无私人数据"})


@router.get("/status/market")
async def get_market_data():
    """查看行情数据"""
    receiver = get_receiver()
    return receiver.memory_store.get('market_data', {"note": "暂无行情数据"})