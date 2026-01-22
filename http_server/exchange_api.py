"""
交易所REST API封装
处理账户、交易、订单等操作，支持私人WebSocket连接
按照新流程：所有API由大脑传入，本模块不自取环境变量
"""
import asyncio
import logging
import sys
import os
import time
import hmac
import hashlib
import urllib.parse
import ccxt.async_support as ccxt
import aiohttp
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))  # smart_brain目录
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

# ✅ 只导入签名函数，不导入get_api_config
from .auth import generate_binance_signature, generate_okx_signature

logger = logging.getLogger(__name__)

class ExchangeAPI:
    """交易所API封装 - 重构版：不自取API，由调用者传入"""
    
    def __init__(self, exchange: str, api_key: str = "", api_secret: str = "", passphrase: str = ""):
        """
        重构：接收API参数，而不是从环境变量获取
        
        参数:
            exchange: 交易所名称 (binance, okx)
            api_key: API Key（可选，某些方法需要）
            api_secret: API Secret（可选，某些方法需要）
            passphrase: Passphrase（可选，欧意需要）
        """
        self.exchange = exchange.lower()
        # ✅ 存储传入的API，而不是从环境变量获取
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        
        self.client = None
        
        # 临时存储用于重试
        self.temp_api_storage = {}
        self.last_token_refresh = None
        
        logger.info(f"[{self.exchange}] ExchangeAPI实例已创建（API由调用者提供）")
        
    async def initialize(self):
        """初始化API客户端（使用传入的API）"""
        try:
            if self.exchange == "binance":
                if not self.api_key or not self.api_secret:
                    logger.warning(f"[{self.exchange}] 缺少API凭证，交易功能可能不可用")
                    return False
                    
                self.client = ccxt.binance({
                    'apiKey': self.api_key,
                    'secret': self.api_secret,
                    'enableRateLimit': True,
                    'options': {
                        'defaultType': 'future',
                        'adjustForTimeDifference': True,
                    }
                })
            elif self.exchange == "okx":
                if not self.api_key or not self.api_secret:
                    logger.warning(f"[{self.exchange}] 缺少API凭证，交易功能可能不可用")
                    return False
                    
                self.client = ccxt.okx({
                    'apiKey': self.api_key,
                    'secret': self.api_secret,
                    'password': self.passphrase,
                    'enableRateLimit': True,
                })
            else:
                logger.error(f"[{self.exchange}] 不支持的交易所")
                return False
            
            # 加载市场数据
            if self.client:
                await self.client.load_markets()
                logger.info(f"[{self.exchange}] API客户端初始化成功")
                return True
                
        except Exception as e:
            logger.error(f"[{self.exchange}] API客户端初始化失败: {e}")
        
        return False
    
    # ==================== 币安listenKey管理（静态方法） ====================
    
    @staticmethod
    async def get_binance_listen_key(api_key: str, api_secret: str) -> Dict[str, Any]:
        """
        获取币安私人WebSocket的listenKey（静态方法）
        由大脑调用，传入API参数
        
        参数:
            api_key: 币安API Key
            api_secret: 币安Secret Key
            
        返回:
            {"success": True, "listenKey": "xxx"} 或 {"success": False, "error": "message"}
        """
        try:
            # (实盘地址)币安 API 端点
#            url = "https://fapi.binance.com/fapi/v1/listenKey"
            
            # (模拟地址)币安Futures API 端点
            url = "https://testnet.binancefuture.com/fapi/v1/listenKey"
            
            # 生成请求头 (币安此端点只需要API-KEY)
            headers = {
                "X-MBX-APIKEY": api_key
            }
            
            # 发送POST请求
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
                        
        except aiohttp.ClientError as e:
            logger.error(f"❌ [HTTP] 获取币安listenKey网络错误: {e}")
            return {"success": False, "error": f"网络错误: {e}"}
        except Exception as e:
            logger.error(f"❌ [HTTP] 获取币安listenKey异常: {e}")
            return {"success": False, "error": str(e)}
    
    @staticmethod
    async def get_binance_listen_key_with_retry(api_key: str, api_secret: str, max_retries: int = 3) -> Dict[str, Any]:
        """
        获取币安listenKey（带重试）
        
        参数:
            api_key: 币安API Key
            api_secret: 币安Secret Key
            max_retries: 最大重试次数
            
        返回:
            {"success": True, "listenKey": "xxx"} 或 {"success": False, "error": "message"}
        """
        for attempt in range(max_retries):
            try:
                current_attempt = attempt + 1
                logger.info(f"🔄 获取币安listenKey (尝试{current_attempt}/{max_retries})...")
                
                result = await ExchangeAPI.get_binance_listen_key(api_key, api_secret)
                
                if result.get('success'):
                    logger.info(f"✅ 获取币安listenKey成功（第{current_attempt}次尝试）")
                    return result
                else:
                    logger.warning(f"⚠️ 获取listenKey失败: {result.get('error')}")
                    
                    if current_attempt < max_retries:
                        wait_time = 2 ** attempt  # 指数退避
                        logger.info(f"⏸️ 等待{wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                        
            except Exception as e:
                logger.error(f"❌ 获取listenKey异常: {e}")
                if current_attempt < max_retries:
                    await asyncio.sleep(2)
        
        # 所有重试都失败
        error_msg = f"获取币安listenKey所有{max_retries}次尝试均失败"
        logger.error(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}
    
    @staticmethod
    async def keep_alive_binance_listen_key(api_key: str, api_secret: str, listen_key: str) -> Dict[str, Any]:
        """
        延长币安listenKey有效期（静态方法）
        
        参数:
            api_key: 币安API Key
            api_secret: 币安Secret Key
            listen_key: 要延长的listenKey
            
        返回:
            {"success": True/False, "error": "message"}
        """
        try:
            # (实盘地址)币安 API 端点
#            url = "https://fapi.binance.com/fapi/v1/listenKey"
            
            # (模拟地址)币安Futures API 端点
            url = "https://testnet.binancefuture.com/fapi/v1/listenKey"
            
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
                        
        except aiohttp.ClientError as e:
            logger.error(f"❌ [HTTP] 币安listenKey续期网络错误: {e}")
            return {"success": False, "error": f"网络错误: {e}"}
        except Exception as e:
            logger.error(f"❌ [HTTP] 币安listenKey续期异常: {e}")
            return {"success": False, "error": str(e)}
    
    @staticmethod
    async def close_binance_listen_key(api_key: str, api_secret: str, listen_key: str) -> Dict[str, Any]:
        """
        关闭/删除币安listenKey（静态方法）
        
        参数:
            api_key: 币安API Key
            api_secret: 币安Secret Key
            listen_key: 要关闭的listenKey
            
        返回:
            {"success": True/False, "error": "message"}
        """
        try:
            # (实盘地址)币安 API 端点
#            url = "https://fapi.binance.com/fapi/v1/listenKey"
            
            # (模拟地址)币安Futures API 端点
            url = "https://testnet.binancefuture.com/fapi/v1/listenKey"
            
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
                        
        except aiohttp.ClientError as e:
            logger.error(f"❌ [HTTP] 关闭币安listenKey网络错误: {e}")
            return {"success": False, "error": f"网络错误: {e}"}
        except Exception as e:
            logger.error(f"❌ [HTTP] 关闭币安listenKey异常: {e}")
            return {"success": False, "error": str(e)}
    
    # ==================== 交易相关API方法（使用传入的API） ====================
    
    async def fetch_account_balance(self) -> Dict[str, Any]:
        """获取账户余额"""
        try:
            if not self.client:
                await self.initialize()
                if not self.client:
                    return {"error": "API客户端初始化失败"}
            
            balance = await self.client.fetch_balance()
            
            # 格式化余额数据
            formatted = {
                "total": balance.get("total", {}),
                "free": balance.get("free", {}),
                "used": balance.get("used", {}),
                "timestamp": datetime.now().isoformat()
            }
            
            return formatted
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 获取余额失败: {e}")
            return {"error": str(e)}
    
    async def fetch_positions(self) -> List[Dict[str, Any]]:
        """获取持仓"""
        try:
            if not self.client:
                await self.initialize()
                if not self.client:
                    return [{"error": "API客户端初始化失败"}]
            
            if self.exchange == "binance":
                # 币安持仓
                positions = await self.client.fetch_positions()
                formatted = []
                for pos in positions:
                    if float(pos.get('contracts', 0)) != 0:
                        formatted.append({
                            "symbol": pos['symbol'],
                            "side": pos['side'],
                            "contracts": float(pos['contracts']),
                            "entry_price": float(pos['entryPrice']),
                            "mark_price": float(pos['markPrice']),
                            "unrealized_pnl": float(pos['unrealizedPnl']),
                            "liquidation_price": float(pos['liquidationPrice']) if pos.get('liquidationPrice') else None,
                            "leverage": float(pos['leverage']) if pos.get('leverage') else 1,
                            "timestamp": datetime.now().isoformat()
                        })
                return formatted
                
            elif self.exchange == "okx":
                # 欧意持仓
                positions = await self.client.fetch_positions()
                formatted = []
                for pos in positions:
                    if float(pos.get('contracts', 0)) != 0:
                        formatted.append({
                            "symbol": pos['symbol'],
                            "side": pos['side'],
                            "contracts": float(pos['contracts']),
                            "entry_price": float(pos['entryPrice']),
                            "mark_price": float(pos['markPrice']),
                            "unrealized_pnl": float(pos['unrealizedPnl']),
                            "liquidation_price": float(pos['liquidationPrice']) if pos.get('liquidationPrice') else None,
                            "leverage": float(pos['leverage']) if pos.get('leverage') else 1,
                            "timestamp": datetime.now().isoformat()
                        })
                return formatted
                
        except Exception as e:
            logger.error(f"[{self.exchange}] 获取持仓失败: {e}")
            return [{"error": str(e)}]
    
    async def create_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        price: Optional[float] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """创建订单"""
        try:
            if not self.client:
                await self.initialize()
                if not self.client:
                    return {"error": "API客户端初始化失败"}
            
            # 准备参数
            order_params = params or {}
            
            # 创建订单
            order = await self.client.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=amount,
                price=price,
                params=order_params
            )
            
            formatted = {
                "order_id": order['id'],
                "symbol": order['symbol'],
                "type": order['type'],
                "side": order['side'],
                "amount": float(order['amount']),
                "price": float(order['price']) if order.get('price') else None,
                "status": order['status'],
                "timestamp": datetime.now().isoformat()
            }
            
            return formatted
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 创建订单失败: {e}")
            return {"error": str(e)}
    
    async def cancel_order(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """取消订单"""
        try:
            if not self.client:
                await self.initialize()
                if not self.client:
                    return {"error": "API客户端初始化失败"}
            
            result = await self.client.cancel_order(order_id, symbol)
            
            formatted = {
                "order_id": result['id'],
                "symbol": result['symbol'],
                "status": result['status'],
                "timestamp": datetime.now().isoformat()
            }
            
            return formatted
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 取消订单失败: {e}")
            return {"error": str(e)}
    
    async def fetch_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取未成交订单"""
        try:
            if not self.client:
                await self.initialize()
                if not self.client:
                    return [{"error": "API客户端初始化失败"}]
            
            orders = await self.client.fetch_open_orders(symbol)
            
            formatted = []
            for order in orders:
                formatted.append({
                    "order_id": order['id'],
                    "symbol": order['symbol'],
                    "type": order['type'],
                    "side": order['side'],
                    "amount": float(order['amount']),
                    "filled": float(order['filled']),
                    "price": float(order['price']) if order.get('price') else None,
                    "status": order['status'],
                    "timestamp": datetime.fromtimestamp(order['timestamp'] / 1000).isoformat()
                })
            
            return formatted
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 获取未成交订单失败: {e}")
            return [{"error": str(e)}]
    
    async def fetch_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[int] = None,
        limit: Optional[int] = 100
    ) -> List[Dict[str, Any]]:
        """获取订单历史"""
        try:
            if not self.client:
                await self.initialize()
                if not self.client:
                    return [{"error": "API客户端初始化失败"}]
            
            orders = await self.client.fetch_orders(symbol, since, limit)
            
            formatted = []
            for order in orders:
                formatted.append({
                    "order_id": order['id'],
                    "symbol": order['symbol'],
                    "type": order['type'],
                    "side": order['side'],
                    "amount": float(order['amount']),
                    "filled": float(order['filled']),
                    "price": float(order['price']) if order.get('price') else None,
                    "status": order['status'],
                    "timestamp": datetime.fromtimestamp(order['timestamp'] / 1000).isoformat()
                })
            
            return formatted
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 获取订单历史失败: {e}")
            return [{"error": str(e)}]
    
    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆"""
        try:
            if not self.client:
                await self.initialize()
                if not self.client:
                    return {"error": "API客户端初始化失败"}
            
            if self.exchange == "binance":
                result = await self.client.set_leverage(leverage, symbol)
                return {
                    "symbol": symbol,
                    "leverage": leverage,
                    "success": True,
                    "timestamp": datetime.now().isoformat()
                }
            elif self.exchange == "okx":
                result = await self.client.set_leverage(leverage, symbol)
                return {
                    "symbol": symbol,
                    "leverage": leverage,
                    "success": True,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"[{self.exchange}] 设置杠杆失败: {e}")
            return {"error": str(e)}
    
    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """获取ticker数据"""
        try:
            if not self.client:
                await self.initialize()
                if not self.client:
                    return {"error": "API客户端初始化失败"}
            
            ticker = await self.client.fetch_ticker(symbol)
            
            formatted = {
                "symbol": ticker['symbol'],
                "last": float(ticker['last']),
                "bid": float(ticker['bid']),
                "ask": float(ticker['ask']),
                "high": float(ticker['high']),
                "low": float(ticker['low']),
                "volume": float(ticker['quoteVolume']),
                "change_percent": float(ticker['percentage']),
                "timestamp": datetime.now().isoformat()
            }
            
            return formatted
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 获取ticker失败: {e}")
            return {"error": str(e)}
    
    # ==================== 新增：静态版本的方法 ====================
    
    @staticmethod
    async def fetch_account_balance_static(api_key: str, api_secret: str, exchange: str, passphrase: str = "") -> Dict[str, Any]:
        """获取账户余额（静态方法，接收API参数）"""
        try:
            api = ExchangeAPI(exchange, api_key, api_secret, passphrase)
            return await api.fetch_account_balance()
        except Exception as e:
            logger.error(f"[{exchange}] 获取余额失败: {e}")
            return {"error": str(e)}
    
    @staticmethod
    async def fetch_positions_static(api_key: str, api_secret: str, exchange: str, passphrase: str = "") -> List[Dict[str, Any]]:
        """获取持仓（静态方法，接收API参数）"""
        try:
            api = ExchangeAPI(exchange, api_key, api_secret, passphrase)
            return await api.fetch_positions()
        except Exception as e:
            logger.error(f"[{exchange}] 获取持仓失败: {e}")
            return [{"error": str(e)}]
    
    # ==================== 定时刷新服务 ====================
    
    async def start_token_refresh_service(self, brain_instance = None):
        """
        启动令牌刷新服务
        可以作为一个后台任务运行，定期刷新令牌
        
        参数:
            brain_instance: 大脑实例，用于保存新令牌
        """
        logger.info("⏰ 启动令牌刷新服务...")
        
        refresh_interval = 50 * 60  # 50分钟（币安listenKey 60分钟过期）
        
        while True:
            try:
                await asyncio.sleep(refresh_interval)
                
                logger.info("🔄 定时刷新币安listenKey...")
                
                # 使用传入的API
                if not self.api_key or not self.api_secret:
                    logger.error("❌ 没有币安API，跳过刷新")
                    continue
                
                # 如果提供了大脑实例，可以获取当前令牌
                current_token = None
                if brain_instance and hasattr(brain_instance, 'data_manager'):
                    current_token = brain_instance.data_manager.get_binance_token()
                
                if current_token:
                    # 刷新现有令牌
                    result = await self.keep_alive_binance_listen_key(self.api_key, self.api_secret, current_token)
                    
                    if result.get('success'):
                        logger.info("✅ 币安listenKey定时刷新成功")
                        
                        # 如果提供了大脑实例，更新令牌时间戳
                        if brain_instance and hasattr(brain_instance, 'data_manager'):
                            brain_instance.data_manager.update_token_expiry('binance', 60)
                    else:
                        logger.warning(f"⚠️ 定时刷新失败: {result.get('error')}")
                else:
                    # 没有当前令牌，获取新的
                    logger.info("📝 没有当前令牌，获取新的...")
                    result = await self.get_binance_listen_key(self.api_key, self.api_secret)
                    
                    if result.get('success'):
                        new_token = result['listenKey']
                        logger.info(f"✅ 获取新令牌成功: {new_token[:15]}...")
                        
                        # 如果提供了大脑实例，保存新令牌
                        if brain_instance and hasattr(brain_instance, 'data_manager'):
                            await brain_instance.data_manager.save_binance_token(new_token)
                    else:
                        logger.error(f"❌ 获取新令牌失败: {result.get('error')}")
                
            except asyncio.CancelledError:
                logger.info("令牌刷新服务被取消")
                break
            except Exception as e:
                logger.error(f"❌ 令牌刷新服务异常: {e}")
                await asyncio.sleep(60)  # 出错后等待1分钟再试
    
    async def close(self):
        """关闭客户端"""
        try:
            if self.client:
                await self.client.close()
                self.client = None
        except Exception as e:
            logger.error(f"[{self.exchange}] 关闭客户端失败: {e}")