"""
私人WebSocket连接池管理器 - 完整版
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Callable

# 导入我们刚刚创建的组件
from .connection import BinancePrivateConnection, OKXPrivateConnection
from .raw_data_cache import RawDataCache
from .data_formatter import PrivateDataFormatter

logger = logging.getLogger(__name__)

class PrivateWebSocketPool:
    """私人连接池 - 完整实现"""
    
    def __init__(self, status_callback: Callable, data_callback: Callable):
        """
        参数:
            status_callback: 状态回调函数 (连接池 → 大脑)
            data_callback: 数据回调函数 (连接池 → 大脑DataManager)
        """
        self.status_callback = status_callback
        self.data_callback = data_callback
        
        # 组件初始化
        self.raw_data_cache = RawDataCache()
        self.data_formatter = PrivateDataFormatter()
        
        # 连接存储
        self.connections = {
            'binance': None,
            'okx': None
        }
        
        # 连接配置
        self.binance_credentials = None
        self.okx_credentials = None
        
        logger.info("🔗 [私人连接池] 初始化完成")
    
    async def start(self):
        """启动连接池"""
        try:
            logger.info("✅ [私人连接池] 已启动")
            return True
            
        except Exception as e:
            logger.error(f"❌ [私人连接池] 启动失败: {e}")
            return False
    
    async def connect_binance(self, listen_key: str, credentials: Dict[str, str]) -> bool:
        """建立币安私人连接"""
        try:
            logger.info(f"🔗 [私人连接池] 正在建立币安私人连接...")
            
            # 保存凭证（用于重连）
            self.binance_credentials = credentials
            
            # 创建连接实例
            connection = BinancePrivateConnection(
                listen_key=listen_key,
                status_callback=self._forward_status,
                data_callback=self._process_and_forward_data,
                raw_data_cache=self.raw_data_cache
            )
            
            # 建立连接
            success = await connection.connect()
            if success:
                self.connections['binance'] = connection
                logger.info("✅ [私人连接池] 币安私人连接建立成功")
            else:
                logger.error("❌ [私人连接池] 币安私人连接建立失败")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ [私人连接池] 建立币安连接异常: {e}")
            await self._report_status('binance', 'error', {'message': str(e)})
            return False
    
    async def connect_okx(self, api_key: str, api_secret: str, passphrase: str = '') -> bool:
        """建立欧意私人连接"""
        try:
            logger.info(f"🔗 [私人连接池] 正在建立欧意私人连接...")
            
            # 保存凭证
            self.okx_credentials = {
                'api_key': api_key,
                'api_secret': api_secret,
                'passphrase': passphrase
            }
            
            # 创建连接实例
            connection = OKXPrivateConnection(
                api_key=api_key,
                api_secret=api_secret,
                passphrase=passphrase,
                status_callback=self._forward_status,
                data_callback=self._process_and_forward_data,
                raw_data_cache=self.raw_data_cache
            )
            
            # 建立连接
            success = await connection.connect()
            if success:
                self.connections['okx'] = connection
                logger.info("✅ [私人连接池] 欧意私人连接建立成功")
            else:
                logger.error("❌ [私人连接池] 欧意私人连接建立失败")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ [私人连接池] 建立欧意连接异常: {e}")
            await self._report_status('okx', 'error', {'message': str(e)})
            return False
    
    async def reconnect_binance(self, listen_key: str) -> bool:
        """重连币安"""
        logger.info(f"🔁 [私人连接池] 正在重连币安...")
        
        # 断开现有连接
        if self.connections['binance']:
            await self.connections['binance'].disconnect()
            self.connections['binance'] = None
        
        # 使用新的listen_key重新连接
        if self.binance_credentials:
            return await self.connect_binance(listen_key, self.binance_credentials)
        else:
            logger.error("❌ [私人连接池] 无法重连币安：没有保存的凭证")
            return False
    
    async def reconnect_okx(self, api_key: str = None, api_secret: str = None, 
                           passphrase: str = '') -> bool:
        """重连欧意"""
        logger.info(f"🔁 [私人连接池] 正在重连欧意...")
        
        # 断开现有连接
        if self.connections['okx']:
            await self.connections['okx'].disconnect()
            self.connections['okx'] = None
        
        # 使用提供的凭证或保存的凭证
        if api_key and api_secret:
            return await self.connect_okx(api_key, api_secret, passphrase)
        elif self.okx_credentials:
            creds = self.okx_credentials
            return await self.connect_okx(
                creds['api_key'], 
                creds['api_secret'], 
                creds.get('passphrase', '')
            )
        else:
            logger.error("❌ [私人连接池] 无法重连欧意：没有保存的凭证")
            return False
    
    async def _process_and_forward_data(self, raw_formatted_data: Dict[str, Any]):
        """处理并转发数据（连接 → 格式化器 → 大脑）"""
        try:
            # 1. 进一步格式化数据
            formatted_data = await self.data_formatter.format(raw_formatted_data)
            
            # 2. 添加处理元数据
            formatted_data['processed_timestamp'] = datetime.now().isoformat()
            formatted_data['formatter_version'] = self.data_formatter.formatter_version
            
            # 3. 转发给大脑
            await self.data_callback(formatted_data)
            
            logger.debug(f"📨 [私人连接池] 已转发数据: {formatted_data['exchange']}.{formatted_data['data_type']}")
            
        except Exception as e:
            logger.error(f"❌ [私人连接池] 处理转发数据失败: {e}")
            # 即使格式化失败，也尝试转发原始数据
            try:
                raw_formatted_data['processing_error'] = str(e)
                await self.data_callback(raw_formatted_data)
            except:
                pass
    
    async def _forward_status(self, status_data: Dict[str, Any]):
        """转发状态信息给大脑"""
        try:
            await self.status_callback(status_data)
        except Exception as e:
            logger.error(f"❌ [私人连接池] 转发状态失败: {e}")
    
    async def _report_status(self, exchange: str, event: str, extra_data: Dict[str, Any] = None):
        """报告状态（内部使用）"""
        await self._forward_status({
            'exchange': exchange,
            'event': event,
            'timestamp': datetime.now().isoformat(),
            **(extra_data or {})
        })
    
    async def shutdown(self):
        """关闭所有连接和组件"""
        logger.info("🛑 [私人连接池] 正在关闭...")
        
        # 关闭所有连接
        shutdown_tasks = []
        for exchange, connection in self.connections.items():
            if connection:
                shutdown_tasks.append(connection.disconnect())
        
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        
        self.connections = {'binance': None, 'okx': None}
        logger.info("✅ [私人连接池] 已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取连接池状态"""
        status = {
            'timestamp': datetime.now().isoformat(),
            'connections': {},
            'components': {
                'raw_data_cache': 'active' if self.raw_data_cache else 'inactive',
                'data_formatter': self.data_formatter.get_status() if self.data_formatter else 'inactive'
            }
        }
        
        for exchange in ['binance', 'okx']:
            connection = self.connections[exchange]
            status['connections'][exchange] = {
                'connected': connection.connected if connection else False,
                'has_credentials': bool(
                    self.binance_credentials if exchange == 'binance' 
                    else self.okx_credentials
                )
            }
        
        return status
        