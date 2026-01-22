"""
私人WebSocket连接池管理器 - 完整版
新增：带重试的连接方法，临时存储API用于重试
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, Callable

# 导入我们刚刚创建的组件
from .connection import BinancePrivateConnection, OKXPrivateConnection
from .raw_data_cache import RawDataCache
from .data_formatter import PrivateDataFormatter

logger = logging.getLogger(__name__)

class PrivateWebSocketPool:
    """私人连接池 - 带重试机制的完整实现"""
    
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
        
        # ✅ 新增：临时存储用于重试（连接成功后立即清除）
        self.temp_credentials = {
            'binance': {
                'listen_key': None,
                'credentials': None,
                'saved_at': None,
                'max_retain_seconds': 300  # 最多保存5分钟
            },
            'okx': {
                'credentials': None,
                'saved_at': None,
                'max_retain_seconds': 300
            }
        }
        
        # 重试统计
        self.retry_stats = {
            'binance': {'total_attempts': 0, 'successful_connections': 0, 'failed_connections': 0},
            'okx': {'total_attempts': 0, 'successful_connections': 0, 'failed_connections': 0}
        }
        
        # 清理任务
        self.cleanup_task = None
        
        logger.info("🔗 [私人连接池] 初始化完成（带重试机制）")
    
    async def start(self):
        """启动连接池"""
        try:
            # 启动清理任务
            self.cleanup_task = asyncio.create_task(self._cleanup_expired_credentials())
            
            logger.info("✅ [私人连接池] 已启动")
            return True
            
        except Exception as e:
            logger.error(f"❌ [私人连接池] 启动失败: {e}")
            return False
    
    # ==================== 新增：带重试的连接方法 ====================
    
    async def establish_binance_connection(self, listen_key: str, credentials: Dict[str, str],
                                          max_retries: int = 3, retry_delay: int = 2) -> bool:
        """
        建立币安私人连接（带内部重试）
        
        参数:
            listen_key: 币安listen_key令牌
            credentials: API凭证
            max_retries: 最大重试次数
            retry_delay: 重试延迟基数（秒）
        
        返回:
            bool: 连接是否成功
        """
        logger.info(f"🔌 [私人连接池] 建立币安连接（最多{max_retries}次重试）...")
        
        # 验证参数
        if not listen_key:
            logger.error("❌ [私人连接池] listen_key为空，无法连接")
            return False
        
        # 1. 保存凭证用于重试
        self._save_credentials_for_retry('binance', listen_key, credentials)
        self.retry_stats['binance']['total_attempts'] += 1
        
        # 2. 尝试连接（带重试）
        success = False
        last_error = None
        
        for attempt in range(max_retries):
            current_attempt = attempt + 1
            logger.info(f"  🔄 尝试连接币安 ({current_attempt}/{max_retries})...")
            
            try:
                # 使用当前保存的凭证
                current_creds = self.temp_credentials['binance']
                if not current_creds['listen_key']:
                    logger.error("  ❌ 没有可用的令牌")
                    break
                
                # 创建连接实例
                connection = BinancePrivateConnection(
                    listen_key=current_creds['listen_key'],
                    status_callback=self._forward_status,
                    data_callback=self._process_and_forward_data,
                    raw_data_cache=self.raw_data_cache
                )
                
                # 建立连接
                attempt_success = await connection.connect()
                
                if attempt_success:
                    self.connections['binance'] = connection
                    success = True
                    
                    # ✅ 连接成功，清除临时存储
                    self._clear_saved_credentials('binance')
                    
                    self.retry_stats['binance']['successful_connections'] += 1
                    logger.info(f"  ✅ 币安连接成功（第{current_attempt}次尝试）")
                    
                    # 报告连接成功
                    await self._report_status('binance', 'connection_established', {
                        'attempt': current_attempt,
                        'total_attempts': max_retries
                    })
                    
                    break
                else:
                    # 连接失败
                    logger.warning(f"  ⚠️ 币安连接失败（第{current_attempt}次尝试）")
                    last_error = "连接返回失败"
                    
                    if current_attempt < max_retries:
                        # 指数退避等待
                        wait_time = retry_delay ** attempt
                        logger.info(f"  ⏸️ 等待{wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                        
            except Exception as e:
                logger.error(f"  ❌ 连接异常: {e}")
                last_error = str(e)
                
                if current_attempt < max_retries:
                    await asyncio.sleep(retry_delay)
        
        # 3. 处理最终结果
        if not success:
            self.retry_stats['binance']['failed_connections'] += 1
            
            # 所有重试都失败，清除临时存储
            self._clear_saved_credentials('binance')
            
            logger.error(f"❌ 币安连接{max_retries}次重试均失败")
            
            # 报告连接失败
            await self._report_status('binance', 'connection_failed', {
                'max_retries': max_retries,
                'last_error': last_error
            })
        
        return success
    
    async def establish_okx_connection(self, api_key: str, api_secret: str, passphrase: str = '',
                                      max_retries: int = 3, retry_delay: int = 2) -> bool:
        """
        建立欧意私人连接（带内部重试）
        """
        logger.info(f"🔌 [私人连接池] 建立欧意连接（最多{max_retries}次重试）...")
        
        # 验证参数
        if not api_key or not api_secret:
            logger.error("❌ [私人连接池] API密钥为空，无法连接")
            return False
        
        # 1. 保存凭证用于重试
        credentials = {'api_key': api_key, 'api_secret': api_secret, 'passphrase': passphrase}
        self._save_credentials_for_retry('okx', None, credentials)
        self.retry_stats['okx']['total_attempts'] += 1
        
        # 2. 尝试连接（带重试）
        success = False
        last_error = None
        
        for attempt in range(max_retries):
            current_attempt = attempt + 1
            logger.info(f"  🔄 尝试连接欧意 ({current_attempt}/{max_retries})...")
            
            try:
                # 使用当前保存的凭证
                current_creds = self.temp_credentials['okx']
                if not current_creds['credentials']:
                    logger.error("  ❌ 没有可用的API凭证")
                    break
                
                creds = current_creds['credentials']
                
                # 创建连接实例
                connection = OKXPrivateConnection(
                    api_key=creds['api_key'],
                    api_secret=creds['api_secret'],
                    passphrase=creds.get('passphrase', ''),
                    status_callback=self._forward_status,
                    data_callback=self._process_and_forward_data,
                    raw_data_cache=self.raw_data_cache
                )
                
                # 建立连接
                attempt_success = await connection.connect()
                
                if attempt_success:
                    self.connections['okx'] = connection
                    success = True
                    
                    # ✅ 连接成功，清除临时存储
                    self._clear_saved_credentials('okx')
                    
                    self.retry_stats['okx']['successful_connections'] += 1
                    logger.info(f"  ✅ 欧意连接成功（第{current_attempt}次尝试）")
                    
                    # 报告连接成功
                    await self._report_status('okx', 'connection_established', {
                        'attempt': current_attempt,
                        'total_attempts': max_retries
                    })
                    
                    break
                else:
                    # 连接失败
                    logger.warning(f"  ⚠️ 欧意连接失败（第{current_attempt}次尝试）")
                    last_error = "连接返回失败"
                    
                    if current_attempt < max_retries:
                        # 指数退避等待
                        wait_time = retry_delay ** attempt
                        logger.info(f"  ⏸️ 等待{wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                        
            except Exception as e:
                logger.error(f"  ❌ 连接异常: {e}")
                last_error = str(e)
                
                if current_attempt < max_retries:
                    await asyncio.sleep(retry_delay)
        
        # 3. 处理最终结果
        if not success:
            self.retry_stats['okx']['failed_connections'] += 1
            
            # 所有重试都失败，清除临时存储
            self._clear_saved_credentials('okx')
            
            logger.error(f"❌ 欧意连接{max_retries}次重试均失败")
            
            # 报告连接失败
            await self._report_status('okx', 'connection_failed', {
                'max_retries': max_retries,
                'last_error': last_error
            })
        
        return success
    
    # ==================== 原有连接方法（保持兼容） ====================
    
    async def connect_binance(self, listen_key: str, credentials: Dict[str, str]) -> bool:
        """
        建立币安私人连接（原有方法，兼容旧代码）
        内部调用新的establish_binance_connection方法
        """
        logger.warning("⚠️ [私人连接池] 使用旧方法connect_binance，建议使用establish_binance_connection")
        return await self.establish_binance_connection(
            listen_key=listen_key,
            credentials=credentials,
            max_retries=1,  # 旧方法默认不重试
            retry_delay=2
        )
    
    async def connect_okx(self, api_key: str, api_secret: str, passphrase: str = '') -> bool:
        """
        建立欧意私人连接（原有方法，兼容旧代码）
        内部调用新的establish_okx_connection方法
        """
        logger.warning("⚠️ [私人连接池] 使用旧方法connect_okx，建议使用establish_okx_connection")
        return await self.establish_okx_connection(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            max_retries=1,  # 旧方法默认不重试
            retry_delay=2
        )
    
    # ==================== 凭证管理方法 ====================
    
    def _save_credentials_for_retry(self, exchange: str, token: Optional[str], credentials: Dict[str, Any]):
        """保存凭证用于重试"""
        if exchange == 'binance':
            self.temp_credentials['binance'] = {
                'listen_key': token,
                'credentials': credentials.copy() if credentials else None,
                'saved_at': time.time(),
                'max_retain_seconds': 300
            }
            logger.debug(f"💾 保存{exchange}凭证用于重试")
            
        elif exchange == 'okx':
            self.temp_credentials['okx'] = {
                'credentials': credentials.copy() if credentials else None,
                'saved_at': time.time(),
                'max_retain_seconds': 300
            }
            logger.debug(f"💾 保存{exchange}凭证用于重试")
    
    def _clear_saved_credentials(self, exchange: str):
        """清除保存的凭证"""
        if exchange == 'binance':
            self.temp_credentials['binance'] = {
                'listen_key': None,
                'credentials': None,
                'saved_at': None,
                'max_retain_seconds': 300
            }
        elif exchange == 'okx':
            self.temp_credentials['okx'] = {
                'credentials': None,
                'saved_at': None,
                'max_retain_seconds': 300
            }
        
        logger.debug(f"🧹 已清除{exchange}保存的凭证")
    
    def _are_credentials_expired(self, exchange: str) -> bool:
        """检查保存的凭证是否过期"""
        creds = self.temp_credentials.get(exchange)
        if not creds or not creds['saved_at']:
            return True
        
        elapsed = time.time() - creds['saved_at']
        return elapsed > creds['max_retain_seconds']
    
    async def _cleanup_expired_credentials(self):
        """清理过期的临时凭证"""
        while True:
            try:
                await asyncio.sleep(60)  # 每分钟检查一次
                
                for exchange in ['binance', 'okx']:
                    if self._are_credentials_expired(exchange):
                        self._clear_saved_credentials(exchange)
                        logger.info(f"🧹 已清理过期的{exchange}临时凭证")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ 清理凭证任务异常: {e}")
                await asyncio.sleep(10)
    
    # ==================== 重连方法 ====================
    
    async def reconnect_binance(self) -> bool:
        """
        重连币安（使用保存的凭证）
        由连接管理器调用
        """
        logger.info("🔁 [私人连接池] 重连币安...")
        
        # 检查是否有保存的凭证
        creds = self.temp_credentials['binance']
        if not creds['listen_key']:
            logger.error("❌ 没有保存的币安凭证用于重连")
            return False
        
        # 检查凭证是否过期
        if self._are_credentials_expired('binance'):
            logger.warning("⚠️ 保存的币安凭证已过期")
            self._clear_saved_credentials('binance')
            return False
        
        # 使用保存的凭证重连
        return await self.establish_binance_connection(
            listen_key=creds['listen_key'],
            credentials=creds['credentials'],
            max_retries=3,
            retry_delay=2
        )
    
    async def reconnect_okx(self) -> bool:
        """
        重连欧意（使用保存的凭证）
        由连接管理器调用
        """
        logger.info("🔁 [私人连接池] 重连欧意...")
        
        # 检查是否有保存的凭证
        creds = self.temp_credentials['okx']
        if not creds['credentials']:
            logger.error("❌ 没有保存的欧意凭证用于重连")
            return False
        
        # 检查凭证是否过期
        if self._are_credentials_expired('okx'):
            logger.warning("⚠️ 保存的欧意凭证已过期")
            self._clear_saved_credentials('okx')
            return False
        
        # 使用保存的凭证重连
        creds_dict = creds['credentials']
        return await self.establish_okx_connection(
            api_key=creds_dict['api_key'],
            api_secret=creds_dict['api_secret'],
            passphrase=creds_dict.get('passphrase', ''),
            max_retries=3,
            retry_delay=2
        )
    
    # ==================== 原有方法保持不变 ====================
    
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
        
        # 关闭清理任务
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        
        # 关闭所有连接
        shutdown_tasks = []
        for exchange, connection in self.connections.items():
            if connection:
                shutdown_tasks.append(connection.disconnect())
        
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        
        # 清理所有存储
        self.connections = {'binance': None, 'okx': None}
        self._clear_saved_credentials('binance')
        self._clear_saved_credentials('okx')
        
        logger.info("✅ [私人连接池] 已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取连接池状态"""
        status = {
            'timestamp': datetime.now().isoformat(),
            'connections': {},
            'temp_credentials': {
                'binance': {
                    'has_listen_key': bool(self.temp_credentials['binance']['listen_key']),
                    'has_credentials': bool(self.temp_credentials['binance']['credentials']),
                    'saved_seconds_ago': int(time.time() - self.temp_credentials['binance']['saved_at']) 
                    if self.temp_credentials['binance']['saved_at'] else None,
                    'is_expired': self._are_credentials_expired('binance')
                },
                'okx': {
                    'has_credentials': bool(self.temp_credentials['okx']['credentials']),
                    'saved_seconds_ago': int(time.time() - self.temp_credentials['okx']['saved_at'])
                    if self.temp_credentials['okx']['saved_at'] else None,
                    'is_expired': self._are_credentials_expired('okx')
                }
            },
            'retry_stats': self.retry_stats,
            'components': {
                'raw_data_cache': 'active' if self.raw_data_cache else 'inactive',
                'data_formatter': self.data_formatter.get_status() if self.data_formatter else 'inactive'
            }
        }
        
        for exchange in ['binance', 'okx']:
            connection = self.connections[exchange]
            status['connections'][exchange] = {
                'connected': connection.connected if connection else False,
                'has_connection': bool(connection)
            }
        
        return status
    
    def get_connection_status(self) -> Dict[str, Any]:
        """获取连接状态（简化版，用于健康检查）"""
        status = {
            'binance': {
                'connected': self.connections['binance'].connected if self.connections['binance'] else False,
                'has_temp_credentials': bool(self.temp_credentials['binance']['listen_key'])
            },
            'okx': {
                'connected': self.connections['okx'].connected if self.connections['okx'] else False,
                'has_temp_credentials': bool(self.temp_credentials['okx']['credentials'])
            }
        }
        return status