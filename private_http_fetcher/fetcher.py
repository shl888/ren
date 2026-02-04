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
    """
    私人HTTP数据获取器
    模仿PrivateWebSocketPool的架构和接口
    """
    
    def __init__(self):
        # 与private_ws_pool相同的结构
        self.brain_store = None          # DataManager实例
        self.running = False
        
        # API凭证（启动时获取一次）
        self.api_key = None
        self.api_secret = None
        self.listen_key = None
        
        # 任务管理
        self.scheduler_task = None
        self.fetch_tasks = []
        
        # Session复用（优化：避免每次新建连接）
        self.session = None
        
        # 状态标志
        self.account_fetched = False     # 账户是否已获取
        self.account_fetch_success = False  # 账户获取是否成功
        
        # 🔴 重试策略：指数退避
        self.account_retry_delays = [10, 20, 40, 60]  # 共5次尝试（第1次+4次重试）
        self.max_account_retries = 4  # 最多重试4次
        
        # 连接质量统计（模仿pool_manager）
        self.quality_stats = {
            'account_fetch': {
                'total_attempts': 0,
                'success_attempts': 0,
                'last_success': None,
                'last_error': None,
                'success_rate': 100.0,
                'retry_count': 0
            },
            'position_fetch': {
                'total_attempts': 0,
                'success_attempts': 0,
                'last_success': None,
                'last_error': None,
                'success_rate': 100.0
            }
        }
        
        # 🔴 币安API端点配置（模拟交易 vs 真实交易）
        # 当前启用：模拟交易端点（Testnet）
        self.BASE_URL = "https://testnet.binancefuture.com"
        
        # 真实交易端点（需要使用时取消下面的注释，并注释掉上面的模拟端点）
        # self.BASE_URL = "https://fapi.binance.com"
        
        self.ACCOUNT_ENDPOINT = "/fapi/v3/account"        # 账户资产
        self.POSITION_ENDPOINT = "/fapi/v3/positionRisk"  # 持仓盈亏
        
        # 🔴 优化：添加recvWindow配置
        self.RECV_WINDOW = 5000  # 5秒接收窗口
        
        # 🔴 优化：记录当前使用的环境
        self.environment = "testnet" if "testnet" in self.BASE_URL else "live"
        logger.info(f"🔗 [HTTP获取器] 初始化完成（环境: {self.environment} | 指数退避重试 + recvWindow）")
    
    async def start(self, brain_store):
        """
        启动获取器 - 严格按照时序控制
        
        Args:
            brain_store: DataManager实例（与私人连接池相同）
        """
        logger.info(f"🚀 [HTTP获取器] 正在启动（环境: {self.environment} | 指数退避重试 + recvWindow）...")
        
        self.brain_store = brain_store
        self.running = True
        
        # 🔴 优化：创建复用的ClientSession
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)
        
        # 创建统一的调度任务，严格控制时序
        self.scheduler_task = asyncio.create_task(self._controlled_scheduler())
        
        logger.info("✅ [HTTP获取器] 调度器已启动，等待4分钟后执行账户获取")
        return True
    
    async def _controlled_scheduler(self):
        """
        受控调度器 - 严格按照时间顺序执行
        1. 等待4分钟（让其他模块先运行）
        2. 尝试获取账户资产（5次指数退避重试）
        3. 账户成功后再启动持仓任务（低频）
        """
        try:
            # ========== 第一阶段：等待4分钟 ==========
            logger.info("⏳ [HTTP获取器] 第一阶段：等待4分钟，让其他模块先运行...")
            for i in range(240):  # 240秒 = 4分钟
                if not self.running:
                    return
                if i % 60 == 0:  # 每分钟记录一次
                    remaining = 240 - i
                    logger.info(f"⏳ [HTTP获取器] 等待中...剩余{remaining}秒")
                await asyncio.sleep(1)
            
            logger.info("✅ [HTTP获取器] 4分钟等待完成，开始账户获取（5次尝试）")
            
            # ========== 第二阶段：获取账户资产（5次指数退避重试） ==========
            self.account_fetch_success = await self._fetch_account_with_retry()
            
            if self.account_fetch_success:
                logger.info("✅ [HTTP获取器] 账户获取成功，准备启动持仓任务")
                
                # ========== 第三阶段：启动持仓任务（低频） ==========
                # 再等待30秒，确保完全冷却
                logger.info("⏳ [HTTP获取器] 账户成功后冷却30秒...")
                await asyncio.sleep(30)
                
                # 启动持仓任务
                position_task = asyncio.create_task(self._fetch_position_low_freq())
                self.fetch_tasks.append(position_task)
                logger.info("✅ [HTTP获取器] 持仓任务已启动（高频模式：每1秒）")
            else:
                logger.warning("⚠️ [HTTP获取器] 账户获取5次尝试均失败，不启动持仓任务")
                
        except asyncio.CancelledError:
            logger.info("🛑 [HTTP获取器] 调度器被取消")
        except Exception as e:
            logger.error(f"❌ [HTTP获取器] 调度器异常: {e}")
    
    async def _fetch_account_with_retry(self):
        """
        获取账户资产 - 5次指数退避重试
        第1次尝试 + 4次重试（10秒, 20秒, 40秒, 60秒后）
        
        🔴 关键修复：418/401错误立即停止，不再重试
        """
        retry_count = 0
        total_attempts = 0
        
        # 第1次尝试（立即执行）
        logger.info(f"💰 [HTTP获取器] 账户获取第1次尝试...")
        result = await self._fetch_account_single()
        total_attempts += 1
        
        # 🔴 修复：检查是否遇到418（IP封禁）或401（权限错误）
        if result == 'PERMANENT_STOP':
            logger.error("🚨 [HTTP获取器] 遇到不可逆错误（418/401），停止所有重试")
            self.quality_stats['account_fetch']['retry_count'] = 0
            return False
        
        if result == True:
            self.quality_stats['account_fetch']['retry_count'] = 0
            return True
        
        # 4次重试（指数退避）
        while retry_count < self.max_account_retries and self.running:
            delay = self.account_retry_delays[retry_count]
            logger.info(f"⏳ [HTTP获取器] {delay}秒后重试账户获取（第{retry_count + 2}次尝试）...")
            await asyncio.sleep(delay)
            
            logger.info(f"💰 [HTTP获取器] 账户获取第{retry_count + 2}次尝试...")
            result = await self._fetch_account_single()
            total_attempts += 1
            retry_count += 1
            
            # 🔴 修复：检查是否遇到418或401
            if result == 'PERMANENT_STOP':
                logger.error(f"🚨 [HTTP获取器] 第{retry_count}次尝试遇到不可逆错误，停止重试")
                self.quality_stats['account_fetch']['retry_count'] = retry_count
                return False
            
            if result == True:
                self.quality_stats['account_fetch']['retry_count'] = retry_count
                return True
        
        # 所有尝试都失败
        self.quality_stats['account_fetch']['retry_count'] = retry_count
        logger.error(f"❌ [HTTP获取器] 账户获取{total_attempts}次尝试全部失败")
        return False
    
    async def _fetch_account_single(self):
        """
        单次尝试获取账户资产（优化版：添加recvWindow和权重监控）
        
        Returns:
            True: 成功
            False: 失败，可重试
            'PERMANENT_STOP': 遇到不可逆错误（418/401），停止所有重试
        """
        try:
            self.quality_stats['account_fetch']['total_attempts'] += 1
            
            api_key, api_secret = await self._get_fresh_credentials()
            if not api_key or not api_secret:
                logger.warning("⚠️ [HTTP获取器] 凭证读取失败，本次尝试跳过")
                self.quality_stats['account_fetch']['last_error'] = "凭证读取失败"
                return False
            
            # 🔴 优化：添加recvWindow参数（币安API要求）
            params = {
                'timestamp': int(time.time() * 1000),
                'recvWindow': self.RECV_WINDOW  # 5000ms
            }
            signed_params = self._sign_params(params, api_secret)
            url = f"{self.BASE_URL}{self.ACCOUNT_ENDPOINT}"
            
            headers = {'X-MBX-APIKEY': api_key}
            
            # 🔴 优化：使用复用的session
            async with self.session.get(url, params=signed_params, headers=headers) as resp:
                # 🔴 优化：监控权重使用
                used_weight = resp.headers.get('X-MBX-USED-WEIGHT-1M')
                if used_weight:
                    logger.debug(f"📊 [HTTP获取器] 账户请求权重使用: {used_weight}/1200")
                
                if resp.status == 200:
                    data = await resp.json()
                    await self._push_data('http_account', data)
                    
                    self.quality_stats['account_fetch']['success_attempts'] += 1
                    self.quality_stats['account_fetch']['last_success'] = datetime.now().isoformat()
                    self.quality_stats['account_fetch']['last_error'] = None
                    self.quality_stats['account_fetch']['success_rate'] = (
                        self.quality_stats['account_fetch']['success_attempts'] / 
                        self.quality_stats['account_fetch']['total_attempts'] * 100
                    )
                    
                    logger.info("✅ [HTTP获取器] 账户资产获取成功！")
                    self.account_fetched = True
                    return True
                
                else:
                    error_text = await resp.text()
                    error_msg = f"HTTP {resp.status}: {error_text[:100]}"
                    self.quality_stats['account_fetch']['last_error'] = error_msg
                    
                    # 🔴 关键修复：418（IP封禁）- 立即停止所有重试
                    if resp.status == 418:
                        retry_after = int(resp.headers.get('Retry-After', 10))
                        logger.error(f"🚨 [HTTP获取器] IP被封禁(418)，需等待{retry_after}秒")
                        # 🔴 修复：返回特殊标记，让上层知道要停止所有重试
                        return 'PERMANENT_STOP'
                    
                    # 🔴 关键修复：401（API密钥无效或权限不足）- 立即停止
                    if resp.status == 401:
                        logger.error(f"🚨 [HTTP获取器] API密钥无效或权限不足(401)")
                        logger.error(f"   当前环境: {self.environment}")
                        logger.error(f"   请检查：")
                        logger.error(f"   1. API密钥是否匹配当前环境（模拟/真实）")
                        logger.error(f"   2. API密钥是否启用了合约权限")
                        logger.error(f"   3. IP白名单是否正确")
                        return 'PERMANENT_STOP'
                    
                    # 🔴 优化：429（频率限制）- 等待后重试
                    if resp.status == 429:
                        retry_after = int(resp.headers.get('Retry-After', 60))
                        logger.warning(f"⚠️ [HTTP获取器] 触发频率限制(429)，等待{retry_after}秒后重试")
                        await asyncio.sleep(retry_after)
                        return False
                    
                    logger.error(f"❌ [HTTP获取器] 账户请求失败 {error_msg}")
                    return False
                        
        except asyncio.TimeoutError:
            error_msg = "请求超时"
            self.quality_stats['account_fetch']['last_error'] = error_msg
            logger.error(f"⏱️ [HTTP获取器] 账户请求超时")
            return False
        except Exception as e:
            error_msg = str(e)
            self.quality_stats['account_fetch']['last_error'] = error_msg
            logger.error(f"❌ [HTTP获取器] 获取账户异常: {e}")
            return False
    
    async def _fetch_position_low_freq(self):
        """
        高频获取持仓盈亏（优化版：1秒间隔 + recvWindow + 权重监控）
        """
        request_count = 0
        
        # 初始等待
        await asyncio.sleep(30)
        
        while self.running:
            try:
                request_count += 1
                logger.info(f"📊 [HTTP获取器] 第{request_count}次获取持仓...")
                
                self.quality_stats['position_fetch']['total_attempts'] += 1
                
                api_key, api_secret = await self._get_fresh_credentials()
                if not api_key or not api_secret:
                    logger.warning("⚠️ [HTTP获取器] 持仓请求-凭证读取失败")
                    await asyncio.sleep(10)  # 10秒后重试
                    continue
                
                # 🔴 优化：添加recvWindow参数
                params = {
                    'timestamp': int(time.time() * 1000),
                    'recvWindow': self.RECV_WINDOW  # 5000ms
                }
                signed_params = self._sign_params(params, api_secret)
                url = f"{self.BASE_URL}{self.POSITION_ENDPOINT}"
                
                headers = {'X-MBX-APIKEY': api_key}
                
                # 🔴 优化：使用复用的session
                async with self.session.get(url, params=signed_params, headers=headers) as resp:
                    # 🔴 优化：监控权重使用
                    used_weight = resp.headers.get('X-MBX-USED-WEIGHT-1M')
                    if used_weight:
                        logger.debug(f"📊 [HTTP获取器] 持仓请求权重使用: {used_weight}/1200")
                    
                    if resp.status == 200:
                        data = await resp.json()
                        
                        # 🔴 优化：处理V3端点空持仓情况
                        if not data:
                            logger.info("📊 [HTTP获取器] 当前无持仓")
                        else:
                            positions_count = len(data)
                            logger.info(f"✅ [HTTP获取器] 第{request_count}次持仓获取成功，共{positions_count}个持仓")
                        
                        await self._push_data('http_position', data)
                        
                        self.quality_stats['position_fetch']['success_attempts'] += 1
                        self.quality_stats['position_fetch']['last_success'] = datetime.now().isoformat()
                        self.quality_stats['position_fetch']['last_error'] = None
                        self.quality_stats['position_fetch']['success_rate'] = (
                            self.quality_stats['position_fetch']['success_attempts'] / 
                            self.quality_stats['position_fetch']['total_attempts'] * 100
                        )
                        
                        # 🔴 优化：成功后等待1秒（降低频率，减少被封风险）
                        await asyncio.sleep(1)
                        
                    else:
                        error_text = await resp.text()
                        error_msg = f"HTTP {resp.status}: {error_text[:100]}"
                        self.quality_stats['position_fetch']['last_error'] = error_msg
                        logger.error(f"❌ [HTTP获取器] 持仓请求失败 {error_msg}")
                        
                        # 🔴 优化：正确处理418和401（永久停止）
                        if resp.status in [418, 401]:
                            retry_after = int(resp.headers.get('Retry-After', 3600))
                            logger.error(f"🚨 [HTTP获取器] 持仓请求触发严重错误({resp.status})，等待{retry_after}秒后永久停止")
                            await asyncio.sleep(retry_after)
                            # 持仓任务遇到418/401也停止
                            logger.error(f"🚨 [HTTP获取器] 持仓任务永久停止")
                            break
                        
                        # 🔴 优化：正确处理429
                        if resp.status == 429:
                            retry_after = int(resp.headers.get('Retry-After', 60))
                            logger.warning(f"⚠️ [HTTP获取器] 持仓请求触发频率限制(429)，等待{retry_after}秒")
                            await asyncio.sleep(retry_after)
                        else:
                            await asyncio.sleep(10)  # 10秒后重试
                                
            except asyncio.CancelledError:
                break
            except Exception as e:
                error_msg = str(e)
                self.quality_stats['position_fetch']['last_error'] = error_msg
                logger.error(f"❌ [HTTP获取器] 持仓循环异常: {e}")
                await asyncio.sleep(10)  # 10秒后重试
    
    async def on_listen_key_updated(self, exchange: str, listen_key: str):
        """接收listenKey更新（保留权限，以备不时之需）"""
        if exchange == 'binance':
            logger.debug(f"📢 [HTTP获取器] 收到{exchange} listenKey更新通知")
            # 可以在这里更新listen_key，但HTTP请求不使用它
            # self.listen_key = listen_key
    
    async def _get_fresh_credentials(self):
        """每次从大脑读取新鲜凭证（核心：零缓存）"""
        try:
            if not self.brain_store:
                return None, None
            creds = await self.brain_store.get_api_credentials('binance')
            if creds and creds.get('api_key') and creds.get('api_secret'):
                # 这里可以按需读取listenKey，但HTTP请求不使用它
                # self.listen_key = await self.brain_store.get_listen_key('binance')
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
        except ImportError as e:
            logger.error(f"❌ [HTTP获取器] 无法导入私人数据处理模块: {e}")
        except Exception as e:
            logger.error(f"❌ [HTTP获取器] 推送数据失败: {e}")
    
    async def shutdown(self):
        """关闭获取器 - 模仿pool_manager.shutdown()"""
        logger.info("🛑 [HTTP获取器] 正在关闭...")
        self.running = False
        
        # 取消调度任务
        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
        
        # 取消所有获取任务
        for task in self.fetch_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # 🔴 优化：关闭复用的session
        if self.session:
            await self.session.close()
            logger.info("✅ [HTTP获取器] HTTP会话已关闭")
        
        logger.info("✅ [HTTP获取器] 已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取状态信息 - 模仿pool_manager.get_status()
        
        Returns:
            状态字典
        """
        status = {
            'timestamp': datetime.now().isoformat(),
            'running': self.running,
            'account_fetched': self.account_fetched,
            'account_fetch_success': self.account_fetch_success,
            'environment': self.environment,  # 🔴 显示当前环境（testnet/live）
            'quality_stats': self.quality_stats,
            'retry_strategy': {
                'account_retries': f"{self.max_account_retries}次重试",
                'retry_delays': self.account_retry_delays,
                'total_attempts': self.max_account_retries + 1
            },
            'api_config': {
                'recvWindow': self.RECV_WINDOW,  # 🔴 显示recvWindow配置
                'session_reuse': True  # 🔴 显示session复用状态
            },
            'schedule': {
                'account': '启动后4分钟开始，5次指数退避重试',
                'position': '账户成功后30秒开始，每1秒一次'  # 🔴 改为1秒
            },
            'endpoints': {
                'account': self.ACCOUNT_ENDPOINT,
                'position': self.POSITION_ENDPOINT,
                'base_url': self.BASE_URL  # 🔴 显示实际使用的端点
            },
            'data_destination': 'private_data_processing.manager'
        }
        
        return status
