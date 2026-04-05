# http_server/trader.py
"""
下单执行器（工人）- 消息驱动模式

运行方式：
1. 工人启动后，一直循环等待消息
2. 收到消息（订单参数）后，异步处理
3. 处理完成后，把结果消息发给大脑
4. 空闲时：歇着 / 定时校准币安时间

大脑 <---> 工人 之间通过消息传递，没有调用关系
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
        """
        工人初始化
        
        参数:
            brain: 大脑实例（用于发消息回大脑）
            use_sandbox: 是否使用模拟交易
        """
        self.brain = brain
        self.use_sandbox = use_sandbox
        self._executor = ThreadPoolExecutor(max_workers=10)
        
        # 消息队列：大脑发来的订单放这里
        self._order_queue = asyncio.Queue()
        
        # 币安时间同步
        self._binance_time_offset = 0
        self._binance_last_sync = 0
        self._binance_sync_interval = 600  # 10分钟
        
        # 控制工人运行状态
        self._running = False
        
        logger.info(f"👷【下单工人】初始化完成 | 模式: {'模拟交易' if use_sandbox else '真实交易'}")
    
    # ========== 对外接口（给大脑用） ==========
    
    def send_orders(self, orders: List[Dict]) -> None:
        """
        大脑发数据给工人（不等待，发完就走）
        
        大脑调用这个方法，把订单参数扔给工人，然后继续干自己的事。
        """
        # 把订单数据放入队列，工人会异步处理
        self._order_queue.put_nowait(orders)
        logger.info(f"📤【消息】大脑发来 {len(orders)} 个订单，已放入队列")
    
    # ========== 工人主循环 ==========
    
    async def start(self):
        """启动工人（在后台一直运行）"""
        self._running = True
        logger.info("👷【下单工人】启动，等待接收订单...")
        
        # 启动币安时间同步任务（后台运行）
        asyncio.create_task(self._binance_time_sync_loop())
        
        # 主循环：一直等着收数据
        while self._running:
            try:
                # 等待大脑发来的订单（阻塞在这里，没有订单就歇着）
                orders = await self._order_queue.get()
                
                # 收到订单，异步处理（不阻塞主循环）
                asyncio.create_task(self._process_orders(orders))
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌【下单工人】主循环异常: {e}")
        
        logger.info("👷【下单工人】已停止")
    
    async def stop(self):
        """停止工人"""
        self._running = False
        # 清空队列
        while not self._order_queue.empty():
            try:
                self._order_queue.get_nowait()
            except:
                break
    
    # ========== 处理订单 ==========
    
    async def _process_orders(self, orders: List[Dict]):
        """
        处理收到的订单（内部方法）
        
        1. 展开币安 OCO
        2. 获取 API 凭证
        3. 并发发送到交易所
        4. 收集结果，发回给大脑
        """
        try:
            logger.info(f"🔧【下单工人】开始处理 {len(orders)} 个订单")
            
            # 1. 展开币安 OCO（一个变两个）
            expanded_orders = []
            for order in orders:
                exchange = order.get("exchange")
                order_type = order.get("type")
                
                if exchange == "binance" and order_type == "oco":
                    sl_order, tp_order = self._expand_binance_oco(order)
                    expanded_orders.append(sl_order)
                    expanded_orders.append(tp_order)
                else:
                    expanded_orders.append(order)
            
            # 2. 获取所有需要的 API 凭证
            creds_map = await self._fetch_all_credentials(expanded_orders)
            
            # 3. 所有订单并发发送
            tasks = []
            for order in expanded_orders:
                exchange = order.get("exchange")
                creds = creds_map.get(exchange)
                if not creds:
                    tasks.append(self._error_result(order, f"无法获取 {exchange} API凭证"))
                else:
                    tasks.append(self._send_order(order, creds))
            
            results = await asyncio.gather(*tasks)
            
            # 4. 把结果发回给大脑
            await self._send_results_to_brain(results)
            
            logger.info(f"✅【下单工人】处理完成，共 {len(results)} 个结果已发回大脑")
            
        except Exception as e:
            logger.error(f"❌【下单工人】处理订单异常: {e}")
            # 发生严重错误，也要通知大脑
            await self._send_results_to_brain([{
                "success": False,
                "error": f"工人处理异常: {str(e)}"
            }])
    
    async def _send_results_to_brain(self, results: List[Dict]):
        """把结果消息发给大脑"""
        if hasattr(self.brain, 'on_trader_results'):
            await self.brain.on_trader_results(results)
        else:
            logger.error("❌ 大脑没有实现 on_trader_results 方法，无法接收结果")
    
    # ========== 币安 OCO 展开 ==========
    
    def _expand_binance_oco(self, oco_order: Dict) -> tuple:
        """展开币安 OCO 订单为两个独立订单"""
        orders_list = oco_order.get("orders", [])
        if len(orders_list) != 2:
            logger.error(f"❌ 币安 OCO 需要 2 个订单，实际: {len(orders_list)}")
        
        result_orders = []
        for algo_order in orders_list:
            new_order = {
                "exchange": "binance",
                "type": "algo_order",
                "params": algo_order.copy()
            }
            result_orders.append(new_order)
        
        return result_orders[0], result_orders[1]
    
    # ========== 获取 API 凭证 ==========
    
    async def _fetch_all_credentials(self, orders: List[Dict]) -> Dict[str, Dict]:
        """并发获取所有需要的 API 凭证"""
        exchanges = set()
        for order in orders:
            exchanges.add(order.get("exchange"))
        
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
    
    # ========== 发送订单（路由） ==========
    
    async def _send_order(self, order: Dict, creds: Dict) -> Dict:
        """发送单个订单"""
        exchange = order.get("exchange")
        order_type = order.get("type")
        params = order.get("params", {})
        
        try:
            if exchange == "binance":
                result = await self._binance_send(creds, order_type, params)
            elif exchange == "okx":
                result = await self._okx_send(creds, order_type, params)
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
    
    async def _error_result(self, order: Dict, error: str) -> Dict:
        return {
            "success": False,
            "exchange": order.get("exchange"),
            "type": order.get("type"),
            "error": error
        }
    
    # ========== 币安 ==========
    
    def _binance_get_base_url(self) -> str:
        if self.use_sandbox:
            return "https://testnet.binancefuture.com"
        return "https://fapi.binance.com"
    
    def _binance_get_timestamp(self) -> int:
        return int(time.time() * 1000) + self._binance_time_offset
    
    async def _binance_time_sync_loop(self):
        """后台定时同步币安时间"""
        while self._running:
            try:
                await self._binance_sync_time()
            except Exception as e:
                logger.error(f"❌ 币安时间同步失败: {e}")
            # 每10分钟同步一次
            await asyncio.sleep(self._binance_sync_interval)
    
    async def _binance_sync_time(self):
        """同步币安服务器时间"""
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
            logger.info(f"⏱️【币安时间同步】偏移量: {self._binance_time_offset}ms")
        except Exception as e:
            logger.error(f"❌【币安时间同步】失败: {e}，偏移量保持 {self._binance_time_offset}")
    
    async def _binance_send(self, creds: Dict, order_type: str, params: Dict) -> Dict:
        """币安路由"""
        api_key = creds.get("api_key")
        api_secret = creds.get("api_secret")
        
        if order_type == "set_leverage":
            endpoint = "/fapi/v1/leverage"
            req_params = params.copy()
            req_params["timestamp"] = self._binance_get_timestamp()
            req_params["recvWindow"] = 5000
            return await self._binance_http_request(api_key, api_secret, "POST", endpoint, req_params)
        
        elif order_type == "open_market":
            endpoint = "/fapi/v1/order"
            req_params = params.copy()
            req_params["timestamp"] = self._binance_get_timestamp()
            req_params["recvWindow"] = 5000
            return await self._binance_http_request(api_key, api_secret, "POST", endpoint, req_params)
        
        elif order_type == "algo_order":
            endpoint = "/fapi/v1/algoOrder"
            req_params = params.copy()
            req_params["timestamp"] = self._binance_get_timestamp()
            req_params["recvWindow"] = 5000
            return await self._binance_http_request(api_key, api_secret, "POST", endpoint, req_params)
        
        elif order_type == "close_position":
            endpoint = "/fapi/v1/closePosition"
            req_params = params.copy()
            req_params["timestamp"] = self._binance_get_timestamp()
            req_params["recvWindow"] = 5000
            return await self._binance_http_request(api_key, api_secret, "POST", endpoint, req_params)
        
        else:
            raise Exception(f"币安未知 order_type: {order_type}")
    
    async def _binance_http_request(self, api_key: str, api_secret: str,
                                     method: str, endpoint: str, params: Dict) -> Dict:
        """执行币安 HTTP 请求"""
        base_url = self._binance_get_base_url()
        
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
        # 欧易实盘和模拟盘都用同一个域名，通过请求头 x-simulated-trading 区分
        return "https://www.okx.com"
    
    def _okx_get_simulated_header(self) -> str:
        """根据 use_sandbox 返回模拟盘标识：1=模拟盘，0=实盘"""
        return "1" if self.use_sandbox else "0"
    
    async def _okx_send(self, creds: Dict, order_type: str, params: Dict) -> Dict:
        """欧易路由"""
        api_key = creds.get("api_key")
        api_secret = creds.get("api_secret")
        passphrase = creds.get("passphrase", "")
        
        if order_type == "set_leverage":
            endpoint = "/api/v5/account/set-leverage"
            body_params = params.copy()
            return await self._okx_http_request(api_key, api_secret, passphrase, "POST", endpoint, body_params)
        
        elif order_type == "open_market":
            endpoint = "/api/v5/trade/order"
            body_params = params.copy()
            return await self._okx_http_request(api_key, api_secret, passphrase, "POST", endpoint, body_params)
        
        elif order_type == "oco":
            endpoint = "/api/v5/trade/order-algo"
            body_params = params.copy()
            return await self._okx_http_request(api_key, api_secret, passphrase, "POST", endpoint, body_params)
        
        elif order_type == "close_position":
            endpoint = "/api/v5/trade/close-position"
            body_params = params.copy()
            return await self._okx_http_request(api_key, api_secret, passphrase, "POST", endpoint, body_params)
        
        else:
            raise Exception(f"欧易未知 order_type: {order_type}")
    
    async def _okx_http_request(self, api_key: str, api_secret: str, passphrase: str,
                                 method: str, endpoint: str, params: Dict) -> Dict:
        """执行欧易 HTTP 请求"""
        base_url = self._okx_get_base_url()
        
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
            "Content-Type": "application/json",
            "x-simulated-trading": self._okx_get_simulated_header()   # 根据 use_sandbox 自动设置
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