# http_server/trader.py
"""
下单执行器（工人）- 直接HTTP版
- 币安：本地时间 + 漂移量（每10分钟同步一次服务器时间）
- 币安自动补 timestamp + recvWindow + signature
- 欧易自动补签名
- set_sl_tp 自动拆成止损+止盈两个单，并发发送
- 多个交易所/订单并发执行
- 支持真实交易/模拟交易
"""

import asyncio
import logging
import time
import hmac
import hashlib
import base64
import json
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class Trader:
    def __init__(self, brain, use_sandbox: bool = True):
        self.brain = brain
        self.use_sandbox = use_sandbox
        self._executor = ThreadPoolExecutor(max_workers=10)
        
        # 币安时间同步
        self._binance_time_offset = 0  # 服务器时间 - 本地时间（毫秒）
        self._binance_last_sync = 0
        self._binance_sync_interval = 600  # 10分钟
    
        # ✅ 加这条日志
        logger.info(f"👷【下单工人】初始化完成 | 模式: {'模拟交易' if use_sandbox else '真实交易'} | 等待订单...")
        
    async def execute(self, actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        批量执行订单，所有订单并发发送
        """
        if not actions:
            return []
        
        # 1. 如果需要币安操作，先同步时间
        if any(a.get("exchange") == "binance" for a in actions):
            await self._binance_sync_time_if_needed()
        
        # 2. 展开 set_sl_tp 为多个独立订单
        expanded_actions = []
        for action in actions:
            if action.get("exchange") == "binance" and action.get("action") == "set_sl_tp":
                sl_action, tp_action = self._expand_binance_sl_tp(action)
                expanded_actions.append(sl_action)
                expanded_actions.append(tp_action)
            else:
                expanded_actions.append(action)
        
        # 3. 获取所有需要的API凭证
        creds_map = await self._fetch_all_credentials(expanded_actions)
        
        # 4. 所有订单并发发送
        tasks = []
        for action in expanded_actions:
            exchange = action.get("exchange")
            creds = creds_map.get(exchange)
            if not creds:
                tasks.append(self._error_result(action, f"无法获取{exchange}API凭证"))
            else:
                tasks.append(self._send_order(action, creds))
        
        results = await asyncio.gather(*tasks)
        return results
    
    # ========== 币安时间同步 ==========
    
    async def _binance_sync_time_if_needed(self):
        """如果需要，同步币安服务器时间"""
        now = time.time()
        if (self._binance_last_sync == 0 or 
            now - self._binance_last_sync > self._binance_sync_interval):
            await self._binance_sync_time()
    
    async def _binance_sync_time(self):
        """同步币安服务器时间，计算漂移量"""
        try:
            url = "https://fapi.binance.com/fapi/v1/time"
            
            loop = asyncio.get_running_loop()
            
            def _request():
                import requests
                return requests.get(url)
            
            response = await loop.run_in_executor(self._executor, _request)
            data = response.json()
            server_time = data["serverTime"]
            local_time = int(time.time() * 1000)
            self._binance_time_offset = server_time - local_time
            self._binance_last_sync = time.time()
            
            logger.info(f"⏱️【币安时间同步】偏移量: {self._binance_time_offset}ms | "
                       f"服务器: {server_time} | 本地: {local_time}")
        except Exception as e:
            logger.error(f"❌【币安时间同步】失败: {e}，使用本地时间")
            self._binance_time_offset = 0
    
    def _binance_get_timestamp(self) -> int:
        """获取带漂移校正的时间戳"""
        return int(time.time() * 1000) + self._binance_time_offset
    
    # ========== 展开止盈止损 ==========
    
    def _expand_binance_sl_tp(self, action: Dict) -> tuple:
        """展开币安止盈止损为两个独立订单"""
        params = action.get("params", {})
        
        # 止损单
        sl_params = {
            "symbol": params["symbol"],
            "side": params["side"],
            "type": "STOP_MARKET",
            "quantity": params["quantity"],
            "positionSide": params["positionSide"],
            "stopPrice": params["stopLossPrice"],
            "stopPriceType": params.get("stopPriceType", "LAST_PRICE"),
            "reduceOnly": "true"
        }
        sl_action = {
            "exchange": "binance",
            "action": "create_order",
            "params": sl_params,
            "_type": "stop_loss"
        }
        
        # 止盈单
        tp_params = {
            "symbol": params["symbol"],
            "side": params["side"],
            "type": "TAKE_PROFIT_MARKET",
            "quantity": params["quantity"],
            "positionSide": params["positionSide"],
            "stopPrice": params["takeProfitPrice"],
            "stopPriceType": params.get("stopPriceType", "LAST_PRICE"),
            "reduceOnly": "true"
        }
        tp_action = {
            "exchange": "binance",
            "action": "create_order",
            "params": tp_params,
            "_type": "take_profit"
        }
        
        return sl_action, tp_action
    
    # ========== 获取API凭证 ==========
    
    async def _fetch_all_credentials(self, actions: List[Dict]) -> Dict[str, Dict]:
        """并发获取所有需要的API凭证"""
        exchanges = set()
        for action in actions:
            exchanges.add(action.get("exchange"))
        
        tasks = {}
        for exchange in exchanges:
            tasks[exchange] = self.brain.data_manager.get_api_credentials(exchange)
        
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        creds_map = {}
        for i, exchange in enumerate(tasks.keys()):
            creds = results[i]
            if isinstance(creds, Exception) or not creds:
                logger.error(f"❌ 无法获取 {exchange} API凭证: {creds}")
                creds_map[exchange] = None
            else:
                creds_map[exchange] = creds
        
        return creds_map
    
    # ========== 发送订单 ==========
    
    async def _send_order(self, action: Dict, creds: Dict) -> Dict:
        """发送单个订单"""
        exchange = action.get("exchange")
        action_type = action.get("action")
        params = action.get("params", {})
        order_type = action.get("_type", action_type)
        
        try:
            if exchange == "binance":
                result = await self._binance_send(creds, action_type, params)
            elif exchange == "okx":
                result = await self._okx_send(creds, action_type, params)
            else:
                return {"success": False, "error": f"未知交易所: {exchange}"}
            
            return {
                "success": True,
                "exchange": exchange,
                "type": order_type,
                "data": result
            }
        except Exception as e:
            logger.error(f"❌ 发送失败 [{exchange}/{order_type}]: {e}")
            return {
                "success": False,
                "exchange": exchange,
                "type": order_type,
                "error": str(e)
            }
    
    async def _error_result(self, action: Dict, error: str) -> Dict:
        return {
            "success": False,
            "exchange": action.get("exchange"),
            "type": action.get("_type", action.get("action")),
            "error": error
        }
    
    # ========== 币安 ==========
    
    def _binance_get_base_url(self) -> str:
        """获取币安base URL"""
        if self.use_sandbox:
            return "https://testnet.binancefuture.com"
        else:
            return "https://fapi.binance.com"
    
    async def _binance_send(self, creds: Dict, action_type: str, params: Dict) -> Dict:
        api_key = creds.get("api_key")
        api_secret = creds.get("api_secret")
        
        if action_type == "set_leverage":
            return await self._binance_set_leverage(api_key, api_secret, params)
        elif action_type == "create_order":
            return await self._binance_create_order(api_key, api_secret, params)
        else:
            raise Exception(f"未知动作: {action_type}")
    
    async def _binance_set_leverage(self, api_key: str, api_secret: str, params: Dict) -> Dict:
        endpoint = "/fapi/v1/leverage"
        base_url = self._binance_get_base_url()
        
        req_params = {
            "symbol": params["symbol"],
            "leverage": params["leverage"],
            "timestamp": self._binance_get_timestamp(),
            "recvWindow": 5000
        }
        
        return await self._binance_http_request(api_key, api_secret, "POST", base_url, endpoint, req_params)
    
    async def _binance_create_order(self, api_key: str, api_secret: str, params: Dict) -> Dict:
        endpoint = "/fapi/v1/order"
        base_url = self._binance_get_base_url()
        
        # 构建请求参数
        req_params = {
            "symbol": params["symbol"],
            "side": params["side"],
            "type": params["type"],
            "timestamp": self._binance_get_timestamp(),
            "recvWindow": 5000
        }
        
        # 添加数量（如果有）
        if "quantity" in params:
            req_params["quantity"] = params["quantity"]
        
        # 添加持仓方向
        if "positionSide" in params:
            req_params["positionSide"] = params["positionSide"]
        
        # 添加止盈止损价格（用于拆单后的STOP_MARKET和TAKE_PROFIT_MARKET）
        if "stopPrice" in params:
            req_params["stopPrice"] = params["stopPrice"]
        if "stopPriceType" in params:
            req_params["stopPriceType"] = params["stopPriceType"]
        
        # 添加平仓参数（从嵌套 params 里取）
        inner_params = params.get("params", {})
        if inner_params.get("reduceOnly"):
            req_params["reduceOnly"] = "true"
        if inner_params.get("closePosition"):
            req_params["closePosition"] = "true"
            # 平全仓时不能有数量
            if "quantity" in req_params:
                del req_params["quantity"]
        
        return await self._binance_http_request(api_key, api_secret, "POST", base_url, endpoint, req_params)
    
    async def _binance_http_request(self, api_key: str, api_secret: str, method: str,
                                     base_url: str, endpoint: str, params: Dict) -> Dict:
        """执行币安HTTP请求"""
        # 生成签名
        query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
        params["signature"] = signature
        
        url = base_url + endpoint
        headers = {"X-MBX-APIKEY": api_key}
        
        loop = asyncio.get_running_loop()
        
        def _request():
            import requests
            if method == "POST":
                return requests.post(url, data=params, headers=headers)
            else:
                return requests.get(url, params=params, headers=headers)
        
        response = await loop.run_in_executor(self._executor, _request)
        
        try:
            return response.json()
        except:
            return {"raw_response": response.text}
    
    # ========== 欧易 ==========
    
    def _okx_get_base_url(self) -> str:
        """获取欧易base URL"""
        if self.use_sandbox:
            return "https://aws.okx.com"
        else:
            return "https://www.okx.com"
    
    async def _okx_send(self, creds: Dict, action_type: str, params: Dict) -> Dict:
        api_key = creds.get("api_key")
        api_secret = creds.get("api_secret")
        passphrase = creds.get("passphrase", "")
        
        if action_type == "set_leverage":
            return await self._okx_set_leverage(api_key, api_secret, passphrase, params)
        elif action_type == "create_order":
            return await self._okx_create_order(api_key, api_secret, passphrase, params)
        else:
            raise Exception(f"未知动作: {action_type}")
    
    async def _okx_set_leverage(self, api_key: str, api_secret: str, passphrase: str, params: Dict) -> Dict:
        endpoint = "/api/v5/account/set-leverage"
        base_url = self._okx_get_base_url()
        
        body_params = {
            "instId": params["instId"],
            "lever": str(params["lever"]),
            "mgnMode": params["mgnMode"]
        }
        
        return await self._okx_http_request(api_key, api_secret, passphrase, "POST", base_url, endpoint, body_params)
    
    async def _okx_create_order(self, api_key: str, api_secret: str, passphrase: str, params: Dict) -> Dict:
        base_url = self._okx_get_base_url()
        
        # 大脑输出的 params.params（嵌套的）- 直接使用，不做字段转换
        inner_params = params.get("params", {})
        
        # 把 sz 转成字符串
        if "sz" in inner_params and inner_params["sz"] is not None:
            inner_params["sz"] = str(inner_params["sz"])
        
        # 把止盈止损单里的 sz 也转成字符串
        if "attachAlgoOrds" in inner_params:
            for algo in inner_params["attachAlgoOrds"]:
                if "sz" in algo and algo["sz"] is not None:
                    algo["sz"] = str(algo["sz"])
        
        # 判断是否是止盈止损订单
        if "attachAlgoOrds" in inner_params:
            # 止盈止损单
            endpoint = "/api/v5/trade/order-algo"
            # 直接复制 inner_params，不做字段转换
            body_params = dict(inner_params)
        else:
            # 普通订单（开仓/平仓）
            endpoint = "/api/v5/trade/order"
            # 直接复制 inner_params，不做字段转换
            body_params = dict(inner_params)
        
        return await self._okx_http_request(api_key, api_secret, passphrase, "POST", base_url, endpoint, body_params)
    
    async def _okx_http_request(self, api_key: str, api_secret: str, passphrase: str,
                                method: str, base_url: str, endpoint: str, params: Dict) -> Dict:
        """执行欧易HTTP请求"""
        timestamp = str(time.time())
        body = json.dumps(params)
        sign_str = timestamp + method + endpoint + body
        signature = base64.b64encode(
            hmac.new(api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()
        ).decode()
        
        url = base_url + endpoint
        headers = {
            "OK-ACCESS-KEY": api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": passphrase,
            "Content-Type": "application/json"
        }
        
        loop = asyncio.get_running_loop()
        
        def _request():
            import requests
            if method == "POST":
                return requests.post(url, data=body, headers=headers)
            else:
                return requests.get(url, params=params, headers=headers)
        
        response = await loop.run_in_executor(self._executor, _request)
        
        try:
            return response.json()
        except:
            return {"raw_response": response.text}