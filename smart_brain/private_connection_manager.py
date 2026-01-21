"""
私人连接管理器 - 大脑的指挥官
负责：1. 凭证生命周期 2. 调度HTTP模块 3. 管理连接池 4. 处理连接事件
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable

logger = logging.getLogger(__name__)

class PrivateConnectionManager:
    """私人连接系统的指挥官"""
    
    def __init__(self, brain):
        self.brain = brain
        self.private_pool = None  # 私人连接池实例
        self.running = False
        
        # 凭证与令牌存储
        self.credentials = {}  # 从环境变量加载的API密钥
        self.active_tokens = {
            'binance': {
                'listen_key': None,
                'expiry_time': None,  # listenKey过期时间
                'last_refresh': None
            }
        }
        
        # 任务管理
        self.keepalive_task = None  # 币安listenKey续期任务
        
        logger.info("🧠 [私人连接管理器] 初始化完成")
    
    async def initialize(self):
        """初始化管理器"""
        logger.info("🧠 [私人连接管理器] 正在初始化...")
        
        # 1. 从大脑的DataManager获取凭证（从环境变量加载的）
        self.credentials = self.brain.data_manager.memory_store.get('env_apis', {})
        if not self.credentials:
            logger.error("❌ [私人连接管理器] 未找到API凭证，请检查环境变量")
            return False
        
        # 2. 创建私人连接池实例
        try:
            # 动态导入，避免循环依赖
            from private_ws_pool import PrivateWebSocketPool
            self.private_pool = PrivateWebSocketPool(
                status_callback=self._handle_pool_status,
                data_callback=self.brain.data_manager.receive_private_data
            )
            logger.info("✅ [私人连接管理器] 私人连接池实例已创建")
        except ImportError as e:
            logger.error(f"❌ [私人连接管理器] 无法导入私人连接池模块: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ [私人连接管理器] 创建连接池失败: {e}")
            return False
        
        self.running = True
        logger.info("✅ [私人连接管理器] 初始化完成")
        return True
    
    async def start_all_connections(self):
        """启动所有交易所的私人连接"""
        logger.info("🚀 [私人连接管理器] 正在启动所有私人连接...")
        
        tasks = []
        for exchange in ['binance', 'okx']:
            if exchange in self.credentials:
                tasks.append(self._setup_exchange_connection(exchange))
        
        # 并发启动所有连接
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if r is True)
        logger.info(f"🎯 [私人连接管理器] 连接启动完成: {success_count}/{len(tasks)} 成功")
        return success_count > 0
    
    async def _setup_exchange_connection(self, exchange: str) -> bool:
        """设置指定交易所的私人连接"""
        try:
            logger.info(f"🔗 [私人连接管理器] 正在设置 {exchange} 私人连接...")
            
            if exchange == 'binance':
                return await self._setup_binance_connection()
            elif exchange == 'okx':
                return await self._setup_okx_connection()
            else:
                logger.error(f"❌ [私人连接管理器] 不支持的交易所: {exchange}")
                return False
                
        except Exception as e:
            logger.error(f"❌ [私人连接管理器] 设置{exchange}连接失败: {e}")
            return False
    
    async def _setup_binance_connection(self) -> bool:
        """设置币安私人连接"""
        try:
            # 1. 获取listenKey
            from http_server.exchange_api import ExchangeAPI
            creds = self.credentials['binance']
            
            logger.info("🔑 [私人连接管理器] 正在获取币安listenKey...")
            result = await ExchangeAPI.get_binance_listen_key(
                api_key=creds['api_key'],
                api_secret=creds['api_secret']
            )
            
            if not result.get('success'):
                logger.error(f"❌ [私人连接管理器] 获取币安listenKey失败: {result.get('error')}")
                return False
            
            listen_key = result['listenKey']
            
            # 2. 保存listenKey（设置60分钟后过期）
            self.active_tokens['binance'] = {
                'listen_key': listen_key,
                'expiry_time': datetime.now() + timedelta(minutes=55),  # 提前5分钟续期
                'last_refresh': datetime.now(),
                'api_key': creds['api_key'],
                'api_secret': creds['api_secret']
            }
            logger.info(f"✅ [私人连接管理器] 币安listenKey已保存: {listen_key[:15]}...")
            
            # 3. 启动listenKey续期任务
            if not self.keepalive_task or self.keepalive_task.done():
                self.keepalive_task = asyncio.create_task(self._binance_keepalive_loop())
            
            # 4. 命令连接池建立连接
            if self.private_pool:
                return await self.private_pool.connect_binance(
                    listen_key=listen_key,
                    credentials=creds  # 传递凭证，用于可能的重新获取
                )
            
            return False
            
        except Exception as e:
            logger.error(f"❌ [私人连接管理器] 设置币安连接异常: {e}")
            return False
    
    async def _setup_okx_connection(self) -> bool:
        """设置欧意私人连接"""
        try:
            creds = self.credentials['okx']
            
            # 欧意直接使用API密钥连接
            if self.private_pool:
                return await self.private_pool.connect_okx(
                    api_key=creds['api_key'],
                    api_secret=creds['api_secret'],
                    passphrase=creds.get('passphrase', '')  # 兼容可能有的passphrase
                )
            
            return False
            
        except Exception as e:
            logger.error(f"❌ [私人连接管理器] 设置欧意连接失败: {e}")
            return False
    
    async def _binance_keepalive_loop(self):
        """币安listenKey续期循环"""
        logger.info("⏰ [私人连接管理器] 币安listenKey续期任务已启动")
        
        while self.running:
            try:
                await asyncio.sleep(60)  # 每分钟检查一次
                
                binance_info = self.active_tokens.get('binance')
                if not binance_info or not binance_info.get('listen_key'):
                    continue
                
                # 检查是否需要续期（提前5分钟）
                if datetime.now() >= binance_info['expiry_time']:
                    logger.info("🔄 [私人连接管理器] 正在续期币安listenKey...")
                    
                    from http_server.exchange_api import ExchangeAPI
                    result = await ExchangeAPI.keep_alive_binance_listen_key(
                        api_key=binance_info['api_key'],
                        api_secret=binance_info['api_secret'],
                        listen_key=binance_info['listen_key']
                    )
                    
                    if result.get('success'):
                        # 更新过期时间
                        self.active_tokens['binance']['expiry_time'] = datetime.now() + timedelta(minutes=55)
                        self.active_tokens['binance']['last_refresh'] = datetime.now()
                        logger.debug("✅ [私人连接管理器] listenKey续期成功")
                    else:
                        logger.warning(f"⚠️ [私人连接管理器] listenKey续期失败: {result.get('error')}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [私人连接管理器] 续期循环异常: {e}")
                await asyncio.sleep(10)
    
    async def _handle_pool_status(self, status_data: Dict[str, Any]):
        """
        处理连接池上报的状态事件
        这是连接池 → 大脑的核心回调
        """
        try:
            exchange = status_data.get('exchange')
            event = status_data.get('event')
            timestamp = status_data.get('timestamp', datetime.now().isoformat())
            
            logger.info(f"📡 [私人连接管理器] 收到{exchange}状态事件: {event}")
            
            # 根据事件类型处理
            if event == 'connection_closed':
                # 连接断开，需要重连
                logger.warning(f"⚠️ [私人连接管理器] {exchange}连接断开，准备重连...")
                await self._handle_reconnection(exchange, status_data)
                
            elif event == 'connection_established':
                logger.info(f"✅ [私人连接管理器] {exchange}私人连接已建立")
                
            elif event == 'error':
                logger.error(f"🚨 [私人连接管理器] {exchange}报告错误: {status_data.get('message')}")
                
            # 可以添加更多事件处理逻辑...
            
        except Exception as e:
            logger.error(f"❌ [私人连接管理器] 处理状态事件失败: {e}")
    
    async def _handle_reconnection(self, exchange: str, status_data: Dict[str, Any]):
        """处理重连逻辑"""
        try:
            logger.info(f"🔁 [私人连接管理器] 正在处理{exchange}重连...")
            
            # 等待一小段时间再重连
            await asyncio.sleep(3)
            
            if exchange == 'binance':
                # 检查listenKey是否过期
                binance_info = self.active_tokens.get('binance')
                if binance_info and binance_info.get('listen_key'):
                    if datetime.now() >= binance_info['expiry_time']:
                        # listenKey过期，需要重新获取
                        logger.warning("🔄 [私人连接管理器] listenKey已过期，重新获取...")
                        await self._setup_binance_connection()
                    else:
                        # listenKey有效，直接重连
                        if self.private_pool:
                            await self.private_pool.reconnect_binance(
                                listen_key=binance_info['listen_key']
                            )
                else:
                    # 没有listenKey信息，重新设置
                    await self._setup_binance_connection()
                    
            elif exchange == 'okx':
                # 欧意直接重连（使用存储的凭证）
                if self.private_pool:
                    creds = self.credentials.get('okx', {})
                    await self.private_pool.reconnect_okx(
                        api_key=creds.get('api_key'),
                        api_secret=creds.get('api_secret'),
                        passphrase=creds.get('passphrase', '')
                    )
                    
        except Exception as e:
            logger.error(f"❌ [私人连接管理器] {exchange}重连失败: {e}")
    
    async def shutdown(self):
        """关闭所有连接和任务"""
        logger.info("🛑 [私人连接管理器] 正在关闭...")
        self.running = False
        
        # 1. 取消续期任务
        if self.keepalive_task:
            self.keepalive_task.cancel()
            try:
                await self.keepalive_task
            except asyncio.CancelledError:
                pass
        
        # 2. 关闭币安listenKey
        binance_info = self.active_tokens.get('binance')
        if binance_info and binance_info.get('listen_key'):
            try:
                from http_server.exchange_api import ExchangeAPI
                await ExchangeAPI.close_binance_listen_key(
                    api_key=binance_info['api_key'],
                    api_secret=binance_info['api_secret'],
                    listen_key=binance_info['listen_key']
                )
            except Exception as e:
                logger.error(f"❌ [私人连接管理器] 关闭listenKey失败: {e}")
        
        # 3. 关闭连接池
        if self.private_pool:
            await self.private_pool.shutdown()
        
        logger.info("✅ [私人连接管理器] 已关闭")
        