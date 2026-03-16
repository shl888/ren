"""
OKX合约面值获取器 - 获取模块
职责：只负责从OKX API获取数据，过滤USDT合约，返回原始数据
"""
import asyncio
import logging
import aiohttp
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class OKXContractFetcher:
    """OKX合约面值获取器（只获取+过滤，不推送）"""
    
    API_URL = "https://www.okx.com/api/v5/public/instruments"
    
    def __init__(self):
        self.is_fetched = False
        self.last_attempt_time = None
        self.fetched_count = 0
        self.last_raw_data = None
        
        self.retry_delays = {
            'network_error': 5,
            'timeout': 10,
            'api_error': 30,
            'rate_limit': 60,
            'default': 15
        }
        
        logger.info("✅ [OKX合约获取器] 初始化完成")
    
    async def startup_fetch(self):
        """启动后延迟60秒获取一次"""
        logger.info("⏳ [OKX合约获取器] 等待60秒后开始获取...")
        await asyncio.sleep(60)
        
        if self.is_fetched:
            logger.info("✅ [OKX合约获取器] 已成功获取过，跳过本次启动获取")
            return self.last_raw_data
        
        logger.info("🚀 [OKX合约获取器] 开始执行启动获取任务")
        return await self.fetch_once()
    
    async def fetch_once(self) -> Optional[Dict[str, Any]]:
        """执行一次获取（最多重试3次）"""
        logger.info("=" * 60)
        logger.info("📡 [OKX合约获取器] 开始获取合约面值数据")
        logger.info(f"⏱️  时间: {datetime.now().isoformat()}")
        logger.info("=" * 60)
        
        for attempt in range(3):
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU
            attempt_num = attempt + 1
            logger.info(f"🔄 第{attempt_num}/3次尝试")
            
            try:
                result = await self._fetch_from_api()
                
                if result['success']:
                    self.is_fetched = True
                    self.last_attempt_time = datetime.now()
                    self.fetched_count = result['usdt_count']
                    self.last_raw_data = result['filtered_data']
                    
                    logger.info("=" * 60)
                    logger.info(f"🎉 [OKX合约获取器] 获取成功！")
                    logger.info(f"📊 原始合约总数: {result['total_count']}")
                    logger.info(f"💰 过滤后USDT合约: {result['usdt_count']}")
                    logger.info("=" * 60)
                    
                    return result['filtered_data']
                
                error_type = self._analyze_error(result.get('error', ''))
                wait_time = self.retry_delays.get(error_type, self.retry_delays['default'])
                
                logger.warning(f"⚠️ 第{attempt_num}次失败: {result.get('error', '未知错误')}")
                
                if attempt < 2:
                    await asyncio.sleep(wait_time)
                
            except Exception as e:
                logger.error(f"❌ 第{attempt_num}次尝试异常: {e}")
                if attempt < 2:
                    await asyncio.sleep(self.retry_delays['default'])
        
        logger.error("💥 [OKX合约获取器] 3次尝试均失败")
        return None
    
    async def _fetch_from_api(self) -> Dict[str, Any]:
        """调用OKX API获取合约信息"""
        result = {
            'success': False,
            'filtered_data': None,
            'total_count': 0,
            'usdt_count': 0,
            'error': None
        }
        
        try:
            params = {"instType": "SWAP"}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(self.API_URL, params=params, timeout=30) as response:
                    
                    if response.status != 200:
                        result['error'] = f"HTTP {response.status}"
                        return result
                    
                    data = await response.json()
                    
                    if data.get('code') != '0':
                        result['error'] = f"API错误: {data.get('msg', '未知错误')}"
                        return result
                    
                    instruments = data.get('data', [])
                    result['total_count'] = len(instruments)
                    
                    # ✅ [蚂蚁基因修复] 将过滤操作放到线程池执行，避免阻塞事件循环
                    loop = asyncio.get_event_loop()
                    
                    # 使用线程池执行过滤操作
                    usdt_contracts = await loop.run_in_executor(
                        None,
                        lambda: [inst for inst in instruments if inst.get('settleCcy') == 'USDT']
                    )
                    
                    result['usdt_count'] = len(usdt_contracts)
                    
                    # 构建原始数据（保留所有字段）
                    result['filtered_data'] = {
                        'exchange': 'okx',
                        'data_type': 'contract_info_raw',
                        'timestamp': datetime.now().isoformat(),
                        'data': {
                            'total_raw_contracts': result['total_count'],
                            'usdt_contracts': usdt_contracts
                        }
                    }
                    result['success'] = True
                    
                    logger.info(f"✅ 获取到 {result['total_count']} 个原始合约，其中USDT合约 {result['usdt_count']} 个")
                    
        except asyncio.TimeoutError:
            result['error'] = "请求超时"
        except aiohttp.ClientError as e:
            result['error'] = f"网络错误: {str(e)}"
        except Exception as e:
            result['error'] = f"未知错误: {str(e)}"
        
        return result
    
    def _analyze_error(self, error_msg: str) -> str:
        """分析错误类型"""
        error_msg = error_msg.lower()
        if 'timeout' in error_msg:
            return 'timeout'
        elif 'network' in error_msg or 'connection' in error_msg:
            return 'network_error'
        elif 'rate limit' in error_msg or 'too many requests' in error_msg:
            return 'rate_limit'
        elif 'api error' in error_msg:
            return 'api_error'
        else:
            return 'default'
    
    def get_last_raw_data(self) -> Optional[Dict]:
        """获取最后一次获取的原始数据"""
        return self.last_raw_data