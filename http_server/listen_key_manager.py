"""
ListenKey管理器 - 负责币安listenKey的生命周期管理
集成在http模块内部，作为exchange_api的扩展
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class ListenKeyManager:
    """ListenKey生命周期管理器"""
    
    def __init__(self, exchange_api, brain_store):
        """
        参数:
            exchange_api: ExchangeAPI实例
            brain_store: 大脑数据存储接口（需实现get_api_credentials和save_listen_key方法）
        """
        self.api = exchange_api
        self.brain = brain_store
        
        # 状态管理
        self.running = False
        self.maintenance_task = None
        self.current_keys = {}  # 缓存当前有效的listenKey
        
        # 续期配置
        self.renewal_interval = 25 * 60  # 25分钟（秒）
        self.retry_delay = 30  # 重试延迟（秒）
        self.max_retries = 3   # 最大重试次数
        
        logger.info("🔑 ListenKey管理器初始化完成")
    
    async def start(self):
        """启动ListenKey管理服务"""
        if self.running:
            logger.warning("ListenKey管理服务已在运行")
            return True
        
        logger.info("🚀 启动ListenKey管理服务...")
        self.running = True
        
        # 启动维护循环
        self.maintenance_task = asyncio.create_task(self._maintenance_loop())
        
        # 立即执行一次检查
        asyncio.create_task(self._check_and_renew_keys())
        
        logger.info("✅ ListenKey管理服务已启动")
        return True
    
    async def stop(self):
        """停止ListenKey管理服务"""
        logger.info("🛑 停止ListenKey管理服务...")
        self.running = False
        
        if self.maintenance_task:
            self.maintenance_task.cancel()
            try:
                await self.maintenance_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ ListenKey管理服务已停止")
    
    async def get_current_key(self, exchange: str) -> Optional[str]:
        """获取当前有效的listenKey"""
        return self.current_keys.get(exchange)
    
    async def force_renew_key(self, exchange: str) -> Optional[str]:
        """强制更新指定交易所的listenKey"""
        logger.info(f"🔄 强制更新{exchange}的listenKey...")
        return await self._acquire_or_renew_key(exchange, force_new=True)
    
    async def _maintenance_loop(self):
        """ListenKey维护主循环"""
        logger.info("⏰ ListenKey维护循环已启动")
        
        while self.running:
            try:
                # 等待续期间隔
                await asyncio.sleep(self.renewal_interval)
                
                # 执行续期检查
                await self._check_and_renew_keys()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"ListenKey维护循环异常: {e}")
                await asyncio.sleep(60)  # 出错后等待1分钟
    
    async def _check_and_renew_keys(self):
        """检查并续期所有交易所的listenKey"""
        try:
            # 检查币安
            await self._check_binance_key()
            
            # 未来可以添加其他交易所
            # await self._check_okx_key()
            
        except Exception as e:
            logger.error(f"检查续期失败: {e}")
    
    async def _check_binance_key(self):
        """检查并续期币安listenKey"""
        try:
            # 1. 检查是否有API凭证
            api_creds = await self.brain.get_api_credentials('binance')
            if not api_creds:
                logger.debug("币安API凭证未就绪，跳过listenKey检查")
                return
            
            # 2. 获取当前listenKey
            current_key = await self.brain.get_listen_key('binance')
            
            if not current_key:
                # 首次获取
                logger.info("首次获取币安listenKey")
                new_key = await self._acquire_new_key('binance', api_creds)
            else:
                # 尝试续期现有key
                new_key = await self._renew_existing_key('binance', current_key, api_creds)
            
            # 3. 更新缓存
            if new_key:
                self.current_keys['binance'] = new_key
                
        except Exception as e:
            logger.error(f"检查币安listenKey失败: {e}")
    
    async def _acquire_new_key(self, exchange: str, api_creds: Dict[str, str]) -> Optional[str]:
        """获取新的listenKey"""
        try:
            logger.info(f"获取新的{exchange} listenKey...")
            
            if exchange == 'binance':
                result = await self.api.get_binance_listen_key(
                    api_key=api_creds['api_key'],
                    api_secret=api_creds['api_secret']
                )
                
                if result.get('success'):
                    new_key = result['listenKey']
                    logger.info(f"✅ 获取到新的{exchange} listenKey: {new_key[:15]}...")
                    
                    # 保存到大脑
                    await self.brain.save_listen_key(exchange, new_key)
                    
                    return new_key
                else:
                    logger.error(f"❌ 获取{exchange} listenKey失败: {result.get('error')}")
                    
        except Exception as e:
            logger.error(f"获取{exchange} listenKey异常: {e}")
        
        return None
    
    async def _renew_existing_key(self, exchange: str, listen_key: str, api_creds: Dict[str, str]) -> Optional[str]:
        """续期现有的listenKey"""
        try:
            if exchange == 'binance':
                # 尝试续期
                result = await self.api.keep_alive_binance_listen_key(
                    api_key=api_creds['api_key'],
                    api_secret=api_creds['api_secret'],
                    listen_key=listen_key
                )
                
                if result.get('success'):
                    logger.debug(f"✅ {exchange} listenKey续期成功: {listen_key[:15]}...")
                    return listen_key  # listenKey不变
                else:
                    logger.warning(f"⚠️ {exchange} listenKey续期失败，尝试获取新Key: {result.get('error')}")
                    
                    # 续期失败，获取新Key
                    return await self._acquire_new_key(exchange, api_creds)
                    
        except Exception as e:
            logger.error(f"续期{exchange} listenKey异常: {e}")
            
            # 异常情况下也获取新Key
            return await self._acquire_new_key(exchange, api_creds)
        
        return None
    
    async def _acquire_or_renew_key(self, exchange: str, force_new: bool = False) -> Optional[str]:
        """获取或续期listenKey（统一入口）"""
        try:
            # 获取API凭证
            api_creds = await self.brain.get_api_credentials(exchange)
            if not api_creds:
                logger.error(f"❌ {exchange} API凭证不存在")
                return None
            
            if force_new:
                # 强制获取新Key
                return await self._acquire_new_key(exchange, api_creds)
            else:
                # 尝试续期现有Key
                current_key = await self.brain.get_listen_key(exchange)
                if current_key:
                    return await self._renew_existing_key(exchange, current_key, api_creds)
                else:
                    return await self._acquire_new_key(exchange, api_creds)
                    
        except Exception as e:
            logger.error(f"获取/续期{exchange} listenKey失败: {e}")
            return None
    
    async def get_status(self) -> Dict[str, Any]:
        """获取管理器状态"""
        status = {
            'running': self.running,
            'current_keys': {k: v[:10] + '...' if v else None for k, v in self.current_keys.items()},
            'config': {
                'renewal_interval': self.renewal_interval,
                'retry_delay': self.retry_delay,
                'max_retries': self.max_retries
            },
            'timestamp': datetime.now().isoformat()
        }
        return status