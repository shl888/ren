"""
OKX合约面值获取器 - 完整版
获取OKX所有永续合约，过滤出USDT结算的合约，推送原始数据
"""
import asyncio
import logging
import aiohttp
from datetime import datetime
from typing import Dict, Any, Optional

# 导入私人数据处理模块
from private_data_processing.manager import receive_private_data

logger = logging.getLogger(__name__)


class OKXContractFetcher:
    """OKX合约面值获取器（获取后立即过滤USDT合约）"""
    
    # OKX API地址
    API_URL = "https://www.okx.com/api/v5/public/instruments"
    
    def __init__(self):
        self.is_fetched = False          # 是否已成功获取
        self.last_attempt_time = None    # 最后成功时间
        self.fetched_count = 0            # 成功获取的USDT合约数
        
        # 重试策略（秒）
        self.retry_delays = {
            'network_error': 5,    # 网络错误
            'timeout': 10,          # 超时
            'api_error': 30,        # API返回错误
            'rate_limit': 60,       # 频率限制
            'default': 15            # 默认
        }
        
        logger.info("✅ [OKX合约] 获取器初始化完成")
    
    async def startup_fetch(self):
        """
        启动后延迟60秒获取一次
        供 launcher.py 调用
        """
        logger.info("⏳ [OKX合约] 等待60秒后开始获取...")
        await asyncio.sleep(60)
        
        if self.is_fetched:
            logger.info("✅ [OKX合约] 已成功获取过，跳过本次启动获取")
            return
        
        logger.info("🚀 [OKX合约] 开始执行启动获取任务")
        await self.fetch_once()
    
    async def fetch_once(self) -> bool:
        """
        执行一次获取（最多重试3次）
        返回是否最终成功
        """
        logger.info("=" * 60)
        logger.info("📡 [OKX合约] 开始获取合约面值数据")
        logger.info(f"⏱️  时间: {datetime.now().isoformat()}")
        logger.info("=" * 60)
        
        for attempt in range(3):  # 最多尝试3次
            attempt_num = attempt + 1
            logger.info(f"🔄 第{attempt_num}/3次尝试")
            
            try:
                # 执行API请求（内部已过滤USDT合约）
                result = await self._fetch_from_api()
                
                if result['success']:
                    # ✅ 成功：推送过滤后的数据
                    await self._push_filtered_data(result['filtered_data'])
                    
                    self.is_fetched = True
                    self.last_attempt_time = datetime.now()
                    self.fetched_count = result['usdt_count']
                    
                    logger.info("=" * 60)
                    logger.info(f"🎉 [OKX合约] 获取成功！")
                    logger.info(f"📊 原始合约总数: {result['total_count']}")
                    logger.info(f"💰 过滤后USDT合约: {result['usdt_count']}")
                    logger.info(f"📤 已推送过滤后的数据")
                    logger.info("=" * 60)
                    
                    return True
                
                # ❌ 失败：分析错误类型，决定等待时间
                error_type = self._analyze_error(result.get('error', ''))
                wait_time = self.retry_delays.get(error_type, self.retry_delays['default'])
                
                logger.warning(f"⚠️ 第{attempt_num}次失败: {result.get('error', '未知错误')}")
                logger.warning(f"📋 错误类型: {error_type}, {wait_time}秒后重试")
                
                if attempt < 2:  # 还有重试机会
                    await asyncio.sleep(wait_time)
                
            except Exception as e:
                # 意外异常
                logger.error(f"❌ 第{attempt_num}次尝试异常: {e}")
                
                if attempt < 2:
                    await asyncio.sleep(self.retry_delays['default'])
        
        # 3次都失败
        logger.error("=" * 60)
        logger.error("💥 [OKX合约] 3次尝试均失败，不再重试")
        logger.error("=" * 60)
        return False
    
    async def _fetch_from_api(self) -> Dict[str, Any]:
        """
        调用OKX API获取合约信息
        返回处理结果（已过滤USDT合约）
        """
        result = {
            'success': False,
            'filtered_data': None,    # 过滤后的数据（只含USDT合约）
            'total_count': 0,          # API返回的总合约数
            'usdt_count': 0,           # 过滤后的USDT合约数
            'error': None
        }
        
        try:
            params = {
                "instType": "SWAP"  # 永续合约
            }
            
            logger.info(f"📡 请求URL: {self.API_URL}")
            logger.info(f"📡 参数: {params}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.API_URL, 
                    params=params,
                    timeout=30
                ) as response:
                    
                    logger.info(f"📥 响应状态码: {response.status}")
                    
                    if response.status != 200:
                        result['error'] = f"HTTP {response.status}"
                        return result
                    
                    # 解析JSON
                    data = await response.json()
                    
                    # 检查OKX返回码
                    if data.get('code') != '0':
                        result['error'] = f"API错误: {data.get('msg', '未知错误')}"
                        return result
                    
                