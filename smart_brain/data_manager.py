"""
数据管理器 - 负责数据接收、存储和推送
"""
import asyncio
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class DataManager:
    def __init__(self, brain):
        self.brain = brain
        
        # 数据状态跟踪
        self.last_market_time = None
        self.last_market_count = 0
        self.last_account_time = None
        self.last_trade_time = None
        
        # 内存存储
        self.memory_store = {
            'market_data': {},
            'private_data': {},
            'encrypted_keys': {}
        }
    
    async def receive_market_data(self, processed_data):
        """
        接收市场数据处理后的数据
        并推送到前端
        """
        try:
            if isinstance(processed_data, list):
                self.last_market_count = len(processed_data)
                
                if logger.isEnabledFor(logging.DEBUG):
                    if processed_data and len(processed_data) > 0:
                        symbol = processed_data[0].get('symbol', 'unknown')
                        logger.debug(f"📣【智能大脑】收到批量数据: {len(processed_data)}条, 第一个合约: {symbol}")
            else:
                logger.warning(f"⚠️【智能大脑】 收到非列表类型市场数据: {type(processed_data)}")
                self.last_market_count = 1
            
            self.last_market_time = datetime.now()
            
            # 推送到前端
            if self.brain.frontend_relay:
                try:
                    await self.brain.frontend_relay.broadcast_market_data(processed_data)
                    if isinstance(processed_data, list) and len(processed_data) > 0:
                        logger.debug(f"✅【智能大脑】 已推送市场数据到前端: {len(processed_data)}条")
                except Exception as e:
                    logger.error(f"️❌【智能大脑】推送市场数据到前端失败: {e}")
            
        except Exception as e:
            logger.error(f"⚠️【智能大脑】接收数据错误: {e}")
    
    async def receive_private_data(self, private_data):
        """
        接收私人数据
        并推送到前端
        """
        try:
            data_type = private_data.get('data_type', 'unknown')
            exchange = private_data.get('exchange', 'unknown')
            
            now = datetime.now()
            
            if data_type == 'account_update' or data_type == 'account':
                self.last_account_time = now
                logger.info(f"💰【智能大脑】 收到账户私人数据: {exchange}")
            elif data_type == 'order_update' or data_type == 'trade':
                self.last_trade_time = now
                logger.info(f"📝【智能大脑】 收到交易私人数据: {exchange}")
            else:
                self.last_account_time = now
                logger.info(f"⚠️【智能大脑】 收到未知类型私人数据: {exchange}.{data_type}")
            
            # 推送到前端
            if self.brain.frontend_relay:
                try:
                    await self.brain.frontend_relay.broadcast_private_data(private_data)
                    logger.debug(f"✅【智能大脑】 已推送私人数据到前端: {exchange}.{data_type}")
                except Exception as e:
                    logger.error(f"❌【智能大脑】推送私人数据到前端失败: {e}")
                
        except Exception as e:
            logger.error(f"⚠️【智能大脑】接收私人数据错误: {e}")
    
    def _format_time_diff(self, last_time):
        if not last_time:
            return "⚠️【智能大脑】从未收到"
        
        now = datetime.now()
        diff = now - last_time
        
        if diff.total_seconds() < 60:
            return f"{int(diff.total_seconds())}秒前"
        elif diff.total_seconds() < 3600:
            return f"{int(diff.total_seconds() / 60)}分钟前"
        else:
            return f"{int(diff.total_seconds() / 3600)}小时前"
    
    async def _log_data_status(self):
        """定期记录数据状态"""
        while self.brain.running:
            try:
                await asyncio.sleep(60)
                
                market_count = self.last_market_count
                market_time = self._format_time_diff(self.last_market_time)
                
                if self.last_account_time:
                    account_status = f"✅【智能大脑】已更新，{self._format_time_diff(self.last_account_time)}"
                else:
                    account_status = "⚠️【智能大脑】从未收到"
                    
                if self.last_trade_time:
                    trade_status = f"✅【智能大脑】已更新，{self._format_time_diff(self.last_trade_time)}"
                else:
                    trade_status = "❌【智能大脑】从未收到"
                
                # 前端连接状态
                if self.brain.frontend_relay:
                    frontend_stats = self.brain.frontend_relay.get_stats_summary()
                    frontend_clients = frontend_stats.get('clients_connected', 0)
                    frontend_status = f"✅【智能大脑】已连接 {frontend_clients} 个客户端"
                else:
                    frontend_status = "⚠️【智能大脑】未启用"
                    frontend_clients = 0
                
                status_msg = f"""【智能大脑】【大脑数据状态】
成品数据，{market_count}条，已更新。{market_time}
私人数据-账户：{account_status}
私人数据-交易：{trade_status}
前端连接：{frontend_status}"""
                
                logger.info(status_msg)
                
                # 推送系统状态到前端
                if self.brain.frontend_relay and frontend_clients > 0:
                    try:
                        system_status = {
                            'market_data': {
                                'count': market_count,
                                'last_update': market_time
                            },
                            'private_data': {
                                'account': account_status,
                                'trade': trade_status
                            },
                            'frontend': {
                                'clients': frontend_clients,
                                'messages_sent': frontend_stats.get('messages_broadcast', 0)
                            },
                            'timestamp': datetime.now().isoformat()
                        }
                        await self.brain.frontend_relay.broadcast_system_status(system_status)
                    except Exception as e:
                        logger.debug(f"❌【智能大脑】推送系统状态失败: {e}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌【智能大脑】状态日志错误: {e}")
                await asyncio.sleep(10)
    
    async def store_market_data(self, data):
        """存储市场数据到内存"""
        # 实现数据存储逻辑
        pass
    
    async def store_private_data(self, data):
        """存储私人数据到内存"""
        pass
    
    async def push_to_frontend(self, data_type, data):
        """推送数据到前端"""
        pass
      