"""
私人HTTP数据获取器 - 严格零缓存模式
完全模仿private_ws_pool的架构和交互方式
"""
import asyncio
import logging
import time
import hmac
import hashlib
import urllib.parse
from datetime import datetime
from typing import Dict, Any, Optional
import aiohttp

logger = logging.getLogger(__name__)

class PrivateHTTPFetcher:
    def __init__(self):
        self.brain_store = None
        self.running = False
        
        # 任务控制
        self.account_fetched = False  # 账户是否已获取
        self.fetch_tasks = []
        
        # 重试策略（完全模仿连接池）
        self.retry_delays = [2, 4, 8, 16, 32]
        self.current_retry_delay_index = 0
        
        # 币安U本位合约API配置 (使用官方最新v3接口)[citation:1]
        self.BASE_URL = "https://fapi.binance.com"
        self.ACCOUNT_ENDPOINT = "/fapi/v3/account"        # 替代 v2[citation:1]
        self.POSITION_ENDPOINT = "/fapi/v3/positionRisk"  # 替代 v2[citation:1]
        
        logger.info("🔗 [HTTP获取器] 初始化完成（零缓存模式）")

    async def start(self, brain_store):
        """启动 - 完全模仿pool_manager.start()"""
        self.brain_store = brain_store
        self.running = True
        
        # 启动两个独立任务
        account_task = asyncio.create_task(self._fetch_account_once())
        position_task = asyncio.create_task(self._fetch_position_loop())
        
        self.fetch_tasks = [account_task, position_task]
        logger.info("✅ [HTTP获取器] 已启动，账户任务（1次），持仓任务（1秒/次）")
        return True

    async def on_listen_key_updated(self, exchange: str, listen_key: str):
        """接收listenKey更新（保留权限，以备不时之需）"""
        if exchange == 'binance':
            logger.debug(f"📢 [HTTP获取器] 收到{exchange} listenKey更新通知")

    async def _fetch_account_once(self):
        """获取账户资产（严格仅一次）"""
        attempt = 0
        while self.running and not self.account_fetched:
            try:
                # ✅ 关键：每次请求前读取API凭证（零缓存）
                api_key, api_secret = await self._get_fresh_credentials()
                if not api_key or not api_secret:
                    logger.warning("⚠️ [HTTP获取器] 凭证读取失败，10秒后重试")
                    await asyncio.sleep(10)
                    continue
                
                # 准备签名参数
                params = {'timestamp': int(time.time() * 1000)}
                signed_params = self._sign_params(params, api_secret)
                url = f"{self.BASE_URL}{self.ACCOUNT_ENDPOINT}"
                
                # 发送请求
                async with aiohttp.ClientSession() as session:
                    headers = {'X-MBX-APIKEY': api_key}
                    async with session.get(url, params=signed_params, headers=headers) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            await self._push_data('http_account', data)
                            logger.info("✅ [HTTP获取器] 账户资产获取成功（仅此一次）")
                            self.account_fetched = True  # 标记完成，退出循环
                            return
                        else:
                            error_text = await resp.text()
                            logger.error(f"❌ [HTTP获取器] 账户请求失败 HTTP {resp.status}: {error_text}")
                            await self._handle_api_error(resp.status, error_text)
                
            except Exception as e:
                logger.error(f"❌ [HTTP获取器] 获取账户异常: {e}")
                attempt += 1
                wait = self.retry_delays[min(attempt-1, len(self.retry_delays)-1)]
                await asyncio.sleep(wait)

    async def _fetch_position_loop(self):
        """获取持仓盈亏（每秒循环）"""
        while self.running:
            try:
                start_time = time.time()
                
                # ✅ 关键：每次请求前读取API凭证（零缓存）
                api_key, api_secret = await self._get_fresh_credentials()
                if not api_key or not api_secret:
                    logger.warning("⚠️ [HTTP获取器] 持仓请求-凭证读取失败，跳过本次")
                    await asyncio.sleep(1)
                    continue
                
                # 准备签名参数
                params = {'timestamp': int(time.time() * 1000)}
                signed_params = self._sign_params(params, api_secret)
                url = f"{self.BASE_URL}{self.POSITION_ENDPOINT}"
                
                # 发送请求
                async with aiohttp.ClientSession() as session:
                    headers = {'X-MBX-APIKEY': api_key}
                    async with session.get(url, params=signed_params, headers=headers) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            await self._push_data('http_position', data)
                            logger.debug("✅ [HTTP获取器] 持仓盈亏获取成功")
                        else:
                            error_text = await resp.text()
                            logger.error(f"❌ [HTTP获取器] 持仓请求失败 HTTP {resp.status}: {error_text}")
                            await self._handle_api_error(resp.status, error_text)
                
                # 精确控制1秒间隔
                request_duration = time.time() - start_time
                sleep_time = max(0.0, 1.0 - request_duration)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [HTTP获取器] 持仓循环异常: {e}")
                await asyncio.sleep(1)  # 异常时至少等待1秒

    async def _get_fresh_credentials(self):
        """每次从大脑读取新鲜凭证（核心：零缓存）"""
        try:
            if not self.brain_store:
                return None, None
            creds = await self.brain_store.get_api_credentials('binance')
            if creds and creds.get('api_key') and creds.get('api_secret'):
                # 这里可以按需读取listenKey，但HTTP请求不使用它
                # listen_key = await self.brain_store.get_listen_key('binance')
                return creds['api_key'], creds['api_secret']
        except Exception as e:
            logger.error(f"❌ [HTTP获取器] 读取凭证失败: {e}")
        return None, None

    def _sign_params(self, params: Dict, api_secret: str) -> Dict:
        """生成签名（币安API要求）"""
        query = urllib.parse.urlencode(params)
        signature = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        params['signature'] = signature
        return params

    async def _handle_api_error(self, status_code: int, error_text: str):
        """处理API错误（参照币安限流策略）[citation:2][citation:7]"""
        if status_code == 429:  # 请求过于频繁
            logger.warning("⚠️ [HTTP获取器] 触发API限流(429)，持仓任务将暂停10秒")
            await asyncio.sleep(10)  # 保守等待
        elif status_code == 418:  # IP被禁
            logger.error("🚨 [HTTP获取器] IP被暂时禁止(418)，持仓任务将暂停60秒")
            await asyncio.sleep(60)

    async def _push_data(self, data_type: str, raw_data: Dict):
        """推送原始数据到处理模块（不处理）"""
        try:
            from private_data_processing.manager import receive_private_data
            await receive_private_data({
                'exchange': 'binance',
                'data_type': data_type,
                'data': raw_data,
                'timestamp': datetime.now().isoformat(),
                'source': 'http_fetcher'
            })
        except Exception as e:
            logger.error(f"❌ [HTTP获取器] 推送数据失败: {e}")

    async def shutdown(self):
        """关闭（模仿pool_manager）"""
        self.running = False
        for task in self.fetch_tasks:
            task.cancel()
        logger.info("✅ [HTTP获取器] 已关闭")
        