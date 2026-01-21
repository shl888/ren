"""
交易所REST API封装
处理账户、交易、订单等操作，支持私人WebSocket连接
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
from datetime import datetime

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))  # smart_brain目录
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from .auth import get_api_config, generate_binance_signature, generate_okx_signature

logger = logging.getLogger(__name__)

class ExchangeAPI:
    """交易所API封装 - 支持币安listenKey管理"""
    
    def __init__(self, exchange: str):
        self.exchange = exchange
        self.api_config = get_api_config(exchange)
        self.client = None
        
    async def initialize(self):
        """初始化API客户端"""
        try:
            if self.exchange == "binance":
                self.client = ccxt.binance({
                    'apiKey': self.api_config.get('api_key', ''),
                    'secret': self.api_config.get('api_secret', ''),
                    'enableRateLimit': True,
                    'options': {
                        'defaultType': 'future',
                        'adjustForTimeDifference': True,
                    }
                })
            elif self.exchange == "okx":
                self.client = ccxt.okx({
                    'apiKey': self.api_config.get('api_key', ''),
                    'secret': self.api_config.get('api_secret', ''),
                    'password': self.api_config.get('passphrase', ''),
                    'enableRateLimit': True,
                })
            
            # 加载市场数据
            if self.client:
                await self.client.load_markets()
                logger.info(f"[{self.exchange}] API客户端初始化成功")
                return True
                
        except Exception as e:
            logger.error(f"[{self.exchange}] API客户端初始化失败: {e}")
        
        return False
    
    # ==================== 新增：币安私人连接HTTP服务 ====================
    
    @staticmethod
    async def get_binance_listen_key(api_key: str, api_secret: str) -> Dict[str, Any]:
        """
        获取币安私人WebSocket的listenKey（静态方法）
        
        参数:
            api_key: 币安API Key
            api_secret: 币安Secret Key
            
        返回:
            {"listenKey": "xxx"} 或 {"error": "message"}
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
                        
        except Exception as e:
            logger.error(f"❌ [HTTP] 获取币安listenKey异常: {e}")
            return {"success": False, "error": str(e)}
    
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
                        
        except Exception as e:
            logger.error(f"❌ [HTTP] 关闭币安listenKey异常: {e}")
            return {"success": False, "error": str(e)}
    
    # ==================== 原有方法保持不变 ====================
    
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
    
    async def close(self):
        """关闭客户端"""
        try:
            if self.client:
                await self.client.close()
                self.client = None
        except Exception as e:
            logger.error(f"[{self.exchange}] 关闭客户端失败: {e}")