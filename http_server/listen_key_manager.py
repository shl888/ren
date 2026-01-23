# http_server/listen_key_manager.py
"""
ListenKey管理器 - 改为直接HTTP实现，删除ExchangeAPI依赖
"""
import asyncio
import logging
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class ListenKeyManager:
    """ListenKey生命周期管理器 - 直接HTTP实现"""
    
    def __init__(self, brain_store):
        """
        参数:
            brain_store: 大脑数据存储接口（需实现get_api_credentials和save_listen_key方法）
        """
        self.brain = brain_store
        # 🚨 删除：self.exchange_api = None
        # 🚨 删除所有ExchangeAPI相关引用
        
        # 状态管理
        self.running = False
        self.maintenance_task = None
        
        # 续期配置
        self.renewal_interval = 25 * 60  # 25分钟（秒）
        self.api_check_interval = 5  # 5秒检查API
        
        # HTTP配置
        self.binance_testnet_url = "https://testnet.binancefuture.com/fapi/v1/listenKey"
        
        logger.info("🔑 ListenKey管理器初始化完成（直接HTTP版）")
    
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
        
        logger.info("✅ ListenKey管理服务已停止")
    
    async def _maintenance_loop(self):
        """ListenKey维护主循环 - 直接HTTP实现"""
        logger.info("⏰ ListenKey维护循环已启动（直接HTTP）")
        
        while self.running:
            try:
                # 步骤1：检查并获取令牌
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
            # 币安令牌获取（现在只有这个）
            await self._check_binance_key()
        except Exception as e:
            logger.error(f"检查续期失败: {e}")
    
    async def _check_binance_key(self):
        """检查并续期币安listenKey - 直接HTTP实现"""
        logger.info("🔍 开始币安令牌检查流程...")
        
        # 步骤1：循环5秒获取API文件
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
        
        # 步骤3：直接HTTP请求币安API
        try:
            # 获取当前listenKey
            current_key = await self.brain.get_listen_key('binance')
            
            if current_key:
                logger.info("🔄 尝试续期现有币安listenKey")
                # 步骤5：执行令牌续期（直接HTTP）
                result = await self._keep_alive_binance_key(api_creds['api_key'], current_key)
                
                if result.get('success'):
                    logger.info(f"✅ 币安listenKey续期成功: {current_key[:5]}...")
                    new_key = current_key
                else:
                    logger.warning(f"⚠️ 币安listenKey续期失败，重新获取新令牌: {result.get('error')}")
                    # 步骤6：续期失败，重新获取新令牌
                    result = await self._get_binance_listen_key(api_creds['api_key'])
                    if result.get('success'):
                        new_key = result['listenKey']
                    else:
                        raise Exception(f"获取新令牌失败: {result.get('error')}")
            else:
                logger.info("🆕 首次获取币安listenKey")
                result = await self._get_binance_listen_key(api_creds['api_key'])
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
    
    async def _get_binance_listen_key(self, api_key: str) -> Dict[str, Any]:
        """直接HTTP获取币安listenKey"""
        try:
            url = self.binance_testnet_url
            headers = {"X-MBX-APIKEY": api_key}
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers) as response:
                    data = await response.json()
                    
                    if 'listenKey' in data:
                        logger.info("✅ [HTTP] 币安listenKey获取成功")
                        return {"success": True, "listenKey": data['listenKey']}
                    else:
                        error_msg = data.get('msg', 'Unknown error')
                        logger.error(f"❌ [HTTP] 币安listenKey获取失败: {error_msg}")
                        return {"success": False, "error": error_msg}
                        
        except Exception as e:
            logger.error(f"❌ [HTTP] 获取币安listenKey异常: {e}")
            return {"success": False, "error": str(e)}
    
    async def _keep_alive_binance_key(self, api_key: str, listen_key: str) -> Dict[str, Any]:
        """直接HTTP延长币安listenKey有效期"""
        try:
            url = self.binance_testnet_url
            headers = {"X-MBX-APIKEY": api_key}
            
            # 币安使用PUT方法延长listenKey
            async with aiohttp.ClientSession() as session:
                async with session.put(url, headers=headers) as response:
                    if response.status == 200:
                        logger.debug(f"✅ [HTTP] 币安listenKey续期成功: {listen_key[:10]}...")
                        return {"success": True}
                    else:
                        data = await response.json()
                        error_msg = data.get('msg', f'HTTP {response.status}')
                        logger.warning(f"⚠️ [HTTP] 币安listenKey续期失败: {error_msg}")
                        return {"success": False, "error": error_msg}
                        
        except Exception as e:
            logger.error(f"❌ [HTTP] 币安listenKey续期异常: {e}")
            return {"success": False, "error": str(e)}
    
    async def _close_binance_listen_key(self, api_key: str, listen_key: str) -> Dict[str, Any]:
        """直接HTTP关闭币安listenKey"""
        try:
            url = self.binance_testnet_url
            headers = {"X-MBX-APIKEY": api_key}
            
            # 币安使用DELETE方法关闭listenKey
            async with aiohttp.ClientSession() as session:
                async with session.delete(url, headers=headers) as response:
                    if response.status == 200:
                        logger.info(f"✅ [HTTP] 币安listenKey关闭成功: {listen_key[:10]}...")
                        return {"success": True}
                    else:
                        data = await response.json()
                        error_msg = data.get('msg', f'HTTP {response.status}')
                        logger.warning(f"⚠️ [HTTP] 币安listenKey关闭失败: {error_msg}")
                        return {"success": False, "error": error_msg}
                        
        except Exception as e:
            logger.error(f"❌ [HTTP] 关闭币安listenKey异常: {e}")
            return {"success": False, "error": str(e)}
    
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
            'config': {
                'renewal_interval': self.renewal_interval,
                'api_check_interval': self.api_check_interval,
                'binance_url': self.binance_testnet_url
            },
            'implementation': 'direct_http',
            'timestamp': datetime.now().isoformat()
        }