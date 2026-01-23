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
    
    def __init__(self, brain_store):
        """
        参数:
            brain_store: 大脑数据存储接口（需实现get_api_credentials和save_listen_key方法）
        """
        self.brain = brain_store
        self.exchange_api = None  # 懒加载，不立即创建
        
        # 状态管理
        self.running = False
        self.maintenance_task = None
        
        # 续期配置
        self.renewal_interval = 25 * 60  # 25分钟（秒）
        self.api_check_interval = 5  # 5秒检查API
        
        logger.info("🔑 ListenKey管理器初始化完成")
    
    async def start(self) -> bool:
        """启动ListenKey管理服务"""
        if self.running:
            logger.warning("ListenKey管理服务已在运行")
            return True
        
        logger.info("🚀 启动ListenKey管理服务...")
        self.running = True
        
        # 启动维护循环
        self.maintenance_task = asyncio.create_task(self._maintenance_loop())
        
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
        
        # 清理ExchangeAPI
        if self.exchange_api:
            await self.exchange_api.close()
            self.exchange_api = None
        
        logger.info("✅ ListenKey管理服务已停止")
    
    async def _maintenance_loop(self):
        """ListenKey维护主循环 - 严格按老板的方案"""
        logger.info("⏰ ListenKey维护循环已启动")
        
        while self.running:
            try:
                # 步骤1：检查并获取令牌（内部会循环等API）
                await self._check_and_renew_keys()
                
                # 步骤5：等待25分钟再续期
                logger.info(f"⏳ 等待{self.renewal_interval/60}分钟后进行下次续期")
                await asyncio.sleep(self.renewal_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"ListenKey维护循环异常: {e}")
                await asyncio.sleep(60)  # 出错后等待1分钟
    
    async def _check_and_renew_keys(self):
        """检查并续期所有交易所的listenKey"""
        try:
            # 币安令牌获取
            await self._check_binance_key()
        except Exception as e:
            logger.error(f"检查续期失败: {e}")
    
    async def _check_binance_key(self):
        """检查并续期币安listenKey - 严格按老板的方案实现"""
        logger.info("🔍 开始币安令牌检查流程...")
        
        # 步骤1：任务已启动 ✅
        
        # 步骤2：循环5秒获取API文件
        api_creds = None
        api_check_count = 0
        
        while self.running:
            # 从大脑获取API
            api_creds = await self.brain.get_api_credentials('binance')
            api_check_count += 1
            
            if api_creds and api_creds.get('api_key'):
                logger.info(f"✅ 第{api_check_count}次尝试：成功获取币安API凭证")
                break
            else:
                logger.debug(f"⏳ 第{api_check_count}次尝试：币安API凭证未就绪，{self.api_check_interval}秒后重试...")
                await asyncio.sleep(self.api_check_interval)
        
        if not self.running:
            return
        
        # 步骤3：向币安交易所连接，获取令牌
        try:
            # 懒加载创建ExchangeAPI
            if not self.exchange_api:
                from .exchange_api import ExchangeAPI
                self.exchange_api = ExchangeAPI("binance", api_creds)
                # 不调用initialize()，让它在需要时才初始化
                logger.info("✅ 懒加载创建币安ExchangeAPI")
            
            # 获取当前listenKey
            current_key = await self.brain.get_listen_key('binance')
            
            if current_key:
                logger.info("🔄 尝试续期现有币安listenKey")
                # 步骤5：执行令牌续期
                result = await self.exchange_api.keep_alive_binance_listen_key(current_key)
                
                if result.get('success'):
                    logger.info(f"✅ 币安listenKey续期成功: {current_key[:5]}...")
                    new_key = current_key
                else:
                    logger.warning(f"⚠️ 币安listenKey续期失败，重新获取新令牌: {result.get('error')}")
                    # 步骤6：续期失败，重新获取新令牌
                    result = await self.exchange_api.get_binance_listen_key()
                    if result.get('success'):
                        new_key = result['listenKey']
                    else:
                        raise Exception(f"获取新令牌失败: {result.get('error')}")
            else:
                logger.info("🆕 首次获取币安listenKey")
                result = await self.exchange_api.get_binance_listen_key()
                if result.get('success'):
                    new_key = result['listenKey']
                else:
                    raise Exception(f"获取令牌失败: {result.get('error')}")
            
            # 步骤4：把令牌推送到大脑
            if new_key:
                await self.brain.save_listen_key('binance', new_key)
                logger.info(f"✅ 币安listenKey已推送到大脑: {new_key[:5]}...")
            
        except Exception as e:
            logger.error(f"❌ 币安令牌获取失败: {e}")
            # 出错后等待一段时间再重试
            await asyncio.sleep(30)
    
    async def get_current_key(self, exchange: str) -> Optional[str]:
        """获取当前有效的listenKey - 从大脑获取"""
        return await self.brain.get_listen_key(exchange)
    
    async def force_renew_key(self, exchange: str) -> Optional[str]:
        """强制更新指定交易所的listenKey"""
        logger.info(f"🔄 强制更新{exchange}的listenKey...")
        # 这里简化处理，直接走完整流程
        await self._check_binance_key()
        return await self.brain.get_listen_key(exchange)
    
    async def get_status(self) -> Dict[str, Any]:
        """获取管理器状态"""
        return {
            'running': self.running,
            'current_key': await self.brain.get_listen_key('binance'),
            'exchange_api_ready': self.exchange_api is not None,
            'config': {
                'renewal_interval': self.renewal_interval,
                'api_check_interval': self.api_check_interval
            },
            'timestamp': datetime.now().isoformat()
        }