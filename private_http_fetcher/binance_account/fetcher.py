
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
        self.brain_store = None  # DataManager实例
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
        self.account_fetched = False  # 账户是否已获取
        self.account_fetch_success = False  # 账户获取是否成功

        # 🔴 重试策略：指数退避
        self.account_retry_delays = [10, 20, 40, 60]  # 共5次尝试（第1次+4次重试）
        self.max_account_retries = 4  # 最多重试4次

        # 🔴 优化：自适应频率控制（应用到账户数据）
        self.account_check_interval = 1  # 当前检查间隔（秒）
        self.account_high_freq = 1  # 高频：1秒（有持仓时）
        self.account_low_freq = 60  # 低频：60秒（无持仓时）
        self.has_position = False  # 当前是否有持仓
        self.last_log_time = 0  # 上次日志时间
        self.log_interval = 60  # 日志间隔（秒）

        # 🔴 新增：重启机制
        self.restart_attempts = 0  # 重启尝试次数
        self.in_restart_cooldown = False  # 是否在重启冷却中

        # 连接质量统计（模仿pool_manager）
        self.quality_stats = {
            'account_fetch': {
                'total_attempts': 0,
                'success_attempts': 0,
                'last_success': None,
                'last_error': None,
                'success_rate': 100.0,
                'retry_count': 0,
                'restart_count': 0,  # 新增：重启次数统计
                'last_restart': None  # 新增：上次重启时间
            }
        }

        # 🔴 币安API端点配置（模拟交易 vs 真实交易）
        # 当前启用：模拟交易端点（Testnet）
        self.BASE_URL = "https://testnet.binancefuture.com"

        # 真实交易端点（需要使用时取消下面的注释，并注释掉上面的模拟端点）
        # self.BASE_URL = "https://fapi.binance.com"

        self.ACCOUNT_ENDPOINT = "/fapi/v3/account"  # 账户资产

        # 🔴 优化：添加recvWindow配置
        self.RECV_WINDOW = 5000  # 5秒接收窗口

        # 🔴 优化：记录当前使用的环境
        self.environment = "testnet" if "testnet" in self.BASE_URL else "live"
        logger.info(
            f"🔗 [HTTP获取器] 初始化完成（环境: {self.environment} | 自适应频率 | 指数退避重试 + recvWindow + 自动重启）")

    async def start(self, brain_store):
        """
        启动获取器 - 严格按照时序控制

        Args:
            brain_store: DataManager实例（与私人连接池相同）
        """
        logger.info(f"🚀 [HTTP获取器] 正在启动（环境: {self.environment} | 自适应频率）...")

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
        3. 账户成功后启动自适应频率的账户数据获取任务
        """
        while self.running:
            try:
                # 重置重启冷却标志
                self.in_restart_cooldown = False
                
                # ========== 第一阶段：等待4分钟 ==========
                logger.info("⏳ [HTTP获取器] 第一阶段：等待4分钟，让其他模块先运行...")
                for i in range(240):  # 240秒 = 4分钟
                    if not self.running or self.in_restart_cooldown:
                        return
                    if i % 60 == 0:  # 每分钟记录一次
                        remaining = 240 - i
                        logger.info(f"⏳ [HTTP获取器] 等待中...剩余{remaining}秒")
                    await asyncio.sleep(1)

                logger.info("✅ [HTTP获取器] 4分钟等待完成，开始账户获取（5次尝试）")

                # ========== 第二阶段：获取账户资产（5次指数退避重试） ==========
                self.account_fetch_success = await self._fetch_account_with_retry()

                if self.account_fetch_success:
                    logger.info("✅ [HTTP获取器] 账户获取成功，启动自适应频率的账户数据获取任务")

                    # ========== 第三阶段：启动自适应频率的账户数据获取 ==========
                    # 再等待30秒，确保完全冷却
                    logger.info("⏳ [HTTP获取器] 账户成功后冷却30秒...")
                    for i in range(30):
                        if not self.running or self.in_restart_cooldown:
                            break
                        await asyncio.sleep(1)

                    if self.running and not self.in_restart_cooldown:
                        # 🔴 修改：启动自适应频率的账户数据获取任务
                        account_task = asyncio.create_task(
                            self._fetch_account_adaptive_freq())
                        self.fetch_tasks.append(account_task)
                        logger.info(
                            "✅ [HTTP获取器] 自适应频率账户数据获取已启动（有持仓1秒/无持仓60秒）")
                        
                        # 等待任务完成（正常情况下会一直运行直到被取消或遇到错误）
                        await account_task
                        
                        # 如果任务正常结束（非取消），说明遇到了需要重启的错误
                        if self.running and not self.in_restart_cooldown:
                            logger.warning("⚠️ [HTTP获取器] 账户任务结束，触发重启")
                            await self._handle_restart("账户任务结束")
                else:
                    logger.warning("⚠️ [HTTP获取器] 账户获取5次尝试均失败，触发重启")
                    if self.running:
                        await self._handle_restart("账户获取失败")

            except asyncio.CancelledError:
                logger.info("🛑 [HTTP获取器] 调度器被取消")
                break
            except Exception as e:
                logger.error(f"❌ [HTTP获取器] 调度器异常: {e}")
                if self.running and not self.in_restart_cooldown:
                    await self._handle_restart(f"调度器异常: {e}")

    async def _fetch_account_with_retry(self):
        """
        获取账户资产 - 5次指数退避重试
        第1次尝试 + 4次重试（10秒, 20秒, 40秒, 60秒后）
        """
        retry_count = 0
        total_attempts = 0

        # 第1次尝试（立即执行）
        logger.info(f"💰 [HTTP获取器] 账户获取第1次尝试...")
        result = await self._fetch_account_single()
        total_attempts += 1

        # 🔴 检查是否遇到需要重启的错误
        if result == 'NEED_RESTART':
            logger.warning("⚠️ [HTTP获取器] 遇到需要重启的错误，停止当前重试循环")
            self.quality_stats['account_fetch']['retry_count'] = 0
            return False

        if result == True:
            self.quality_stats['account_fetch']['retry_count'] = 0
            return True

        # 4次重试（指数退避）
        while retry_count < self.max_account_retries and self.running and not self.in_restart_cooldown:
            delay = self.account_retry_delays[retry_count]
            logger.info(
                f"⏳ [HTTP获取器] {delay}秒后重试账户获取（第{retry_count + 2}次尝试）...")
            await asyncio.sleep(delay)

            logger.info(f"💰 [HTTP获取器] 账户获取第{retry_count + 2}次尝试...")
            result = await self._fetch_account_single()
            total_attempts += 1
            retry_count += 1

            # 🔴 检查是否遇到需要重启的错误
            if result == 'NEED_RESTART':
                logger.warning(
                    f"⚠️ [HTTP获取器] 第{retry_count}次尝试遇到需要重启的错误")
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
            'NEED_RESTART': 遇到需要重启的错误（418/401），触发重启
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

                if resp.status == 200:
                    data = await resp.json()
                    await self._push_data('http_account', data)

                    self.quality_stats['account_fetch']['success_attempts'] += 1
                    self.quality_stats['account_fetch']['last_success'] = datetime.now(
                    ).isoformat()
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

                    # 🔴 关键修复：418（IP封禁）- 触发重启
                    if resp.status == 418:
                        logger.warning(
                            f"⚠️ [HTTP获取器] IP被封禁(418)，触发重启")
                        return 'NEED_RESTART'

                    # 🔴 关键修复：401（API密钥无效或权限不足）- 触发重启
                    if resp.status == 401:
                        logger.warning(
                            f"⚠️ [HTTP获取器] API密钥无效或权限不足(401)，触发重启")
                        return 'NEED_RESTART'

                    # 🔴 优化：429（频率限制）- 等待后重试（HTTP短连接，无需特殊处理）
                    if resp.status == 429:
                        wait_time = await self._get_retry_after_time(resp)
                        logger.warning(
                            f"⚠️ [HTTP获取器] 触发频率限制(429)，等待{wait_time}秒后重试")
                        await asyncio.sleep(wait_time)
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

    async def _fetch_account_adaptive_freq(self):
        """
        自适应频率获取账户数据（优化版：有持仓1秒/无持仓60秒 + 日志控制 + 自动重启）
        从账户数据本身的positions字段判断是否有持仓
        """
        request_count = 0

        # 初始等待
        await asyncio.sleep(30)

        while self.running and not self.in_restart_cooldown:
            try:
                request_count += 1

                api_key, api_secret = await self._get_fresh_credentials()
                if not api_key or not api_secret:
                    logger.warning("⚠️ [HTTP获取器] 账户请求-凭证读取失败")
                    await asyncio.sleep(self.account_check_interval)
                    continue

                # 🔴 优化：添加recvWindow参数
                params = {
                    'timestamp': int(time.time() * 1000),
                    'recvWindow': self.RECV_WINDOW  # 5000ms
                }
                signed_params = self._sign_params(params, api_secret)
                url = f"{self.BASE_URL}{self.ACCOUNT_ENDPOINT}"

                headers = {'X-MBX-APIKEY': api_key}

                # 🔴 优化：使用复用的session
                async with self.session.get(url, params=signed_params, headers=headers) as resp:

                    if resp.status == 200:
                        data = await resp.json()

                        # 🔴 关键：检查账户数据中是否有持仓
                        positions = data.get('positions', [])
                        # 过滤掉仓位为0的持仓
                        has_position_now = False
                        for pos in positions:
                            position_amt = float(pos.get('positionAmt', '0'))
                            if position_amt != 0:  # 仓位不为0表示有真实持仓
                                has_position_now = True
                                break

                        # 🔴 自适应频率调整
                        if has_position_now:
                            # 有持仓 → 高频模式（1秒）
                            if not self.has_position:
                                # 状态变化：从无持仓变为有持仓
                                logger.info(
                                    f"🚀 [HTTP获取器] 检测到持仓，切换高频模式（1秒）")
                            self.account_check_interval = self.account_high_freq
                            self.has_position = True
                        else:
                            # 无持仓 → 低频模式（60秒）
                            if self.has_position:
                                # 状态变化：从有持仓变为无持仓
                                logger.info(
                                    f"💤 [HTTP获取器] 检测到清仓，切换低频模式（60秒）")
                            self.account_check_interval = self.account_low_freq
                            self.has_position = False

                        # 🔴 优化：日志控制（每分钟只打印1次）
                        current_time = time.time()
                        if current_time - self.last_log_time >= self.log_interval:
                            if has_position_now:
                                positions_count = len(
                                    [p for p in positions if float(p.get('positionAmt', '0')) != 0])
                                logger.info(
                                    f"📊 [HTTP获取器] 当前持仓{positions_count}个 | 高频模式 | 请求次数:{request_count}")
                            else:
                                logger.info(
                                    f"📊 [HTTP获取器] 当前无持仓 | 低频模式 | 请求次数:{request_count}")
                            self.last_log_time = current_time

                        await self._push_data('http_account', data)

                        self.quality_stats['account_fetch']['success_attempts'] += 1
                        self.quality_stats['account_fetch']['total_attempts'] += 1
                        self.quality_stats['account_fetch']['last_success'] = datetime.now(
                        ).isoformat()
                        self.quality_stats['account_fetch']['last_error'] = None
                        self.quality_stats['account_fetch']['success_rate'] = (
                            self.quality_stats['account_fetch']['success_attempts'] /
                            self.quality_stats['account_fetch']['total_attempts'] * 100
                        )

                        # 按当前频率等待
                        await asyncio.sleep(self.account_check_interval)

                    else:
                        error_text = await resp.text()
                        error_msg = f"HTTP {resp.status}: {error_text[:100]}"
                        self.quality_stats['account_fetch']['last_error'] = error_msg

                        # 🔴 正确处理：418/401错误 - 触发重启
                        if resp.status in [418, 401]:
                            logger.warning(
                                f"⚠️ [HTTP获取器] 遇到严重错误({resp.status})，触发重启")
                            asyncio.create_task(self._handle_restart(f"HTTP {resp.status}错误"))
                            return  # 退出当前任务

                        # 🔴 正确处理：429频率限制 - 等待后继续（HTTP短连接，无需特殊处理）
                        elif resp.status == 429:
                            wait_time = await self._get_retry_after_time(resp)
                            logger.warning(
                                f"⚠️ [HTTP获取器] 触发频率限制(429)，等待{wait_time}秒后继续")
                            await asyncio.sleep(wait_time)
                            continue  # 继续循环，无需清理连接

                        # 其他错误
                        else:
                            logger.error(f"❌ [HTTP获取器] 账户请求失败 {error_msg}")
                            await asyncio.sleep(self.account_check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                error_msg = str(e)
                self.quality_stats['account_fetch']['last_error'] = error_msg
                logger.error(f"❌ [HTTP获取器] 账户循环异常: {e}")
                await asyncio.sleep(self.account_check_interval)

    async def _get_retry_after_time(self, resp) -> int:
        """
        从429响应中获取建议的等待时间（简化版）
        
        Args:
            resp: HTTP响应对象
            
        Returns:
            建议等待时间（秒）
        """
        # 1. 优先从响应头获取建议等待时间
        for header in ['Retry-After', 'retry-after']:
            if header in resp.headers:
                try:
                    wait_time = int(resp.headers[header])
                    # 确保在合理范围内（10-300秒）
                    return max(10, min(wait_time, 300))
                except (ValueError, TypeError):
                    continue
        
        # 2. 默认60秒（统一简单处理）
        return 60

    async def _handle_restart(self, reason: str):
        """
        处理重启逻辑 - 所有严重错误都立即重启
        
        🔴 注意：418/401错误需要清理session，因为可能是IP封禁或权限问题
        """
        if self.in_restart_cooldown:
            return
            
        self.in_restart_cooldown = True
        self.restart_attempts += 1
        
        logger.warning(f"🔄 [HTTP获取器] 立即重启（原因: {reason} | 第{self.restart_attempts}次重启）")
        
        # 1. 取消所有fetch任务
        for task in self.fetch_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self.fetch_tasks.clear()
        
        # 2. 🔴 关闭当前session（清理可能的被封禁连接）
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("✅ [HTTP获取器] 重启前清理HTTP会话")
        
        if not self.running:
            return
        
        # 3. 立即重新创建session（不等待）
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)
        
        # 4. 更新统计信息
        self.quality_stats['account_fetch']['restart_count'] = self.restart_attempts
        self.quality_stats['account_fetch']['last_restart'] = datetime.now().isoformat()
        
        # 5. 重置状态标志
        self.account_fetched = False
        self.account_fetch_success = False
        self.has_position = False
        self.account_check_interval = 1
        
        logger.info(f"✅ [HTTP获取器] 重启完成，准备重新执行调度流程（将等待4分钟）")

    async def on_listen_key_updated(self, exchange: str, listen_key: str):
        """接收listenKey更新（保留权限，以备不时之需）"""
        if exchange == 'binance':
            logger.debug(
                f"📢 [HTTP获取器] 收到{exchange} listenKey更新通知")
            # 可以在这里更新listen_key，但HTTP请求不使用它
            # self.listen_key = listen_key

    async def _get_fresh_credentials(self):
        """每次从大脑读取新鲜凭证（核心：零缓存）"""
        try:
            if not self.brain_store:
                return None, None
            creds = await self.brain_store.get_api_credentials('binance')
            if creds and creds.get('api_key') and creds.get('api_secret'):
                return creds['api_key'], creds['api_secret']
        except Exception as e:
            logger.error(f"❌ [HTTP获取器] 读取凭证失败: {e}")
        return None, None

    def _sign_params(self, params: Dict, api_secret: str) -> Dict:
        """生成签名（币安API要求）"""
        query = urllib.parse.urlencode(params)
        signature = hmac.new(api_secret.encode(),
                             query.encode(), hashlib.sha256).hexdigest()
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
        self.in_restart_cooldown = True  # 阻止重启

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
            'environment': self.environment,
            'adaptive_frequency': {
                'current_interval': self.account_check_interval,
                'has_position': self.has_position,
                'high_freq': self.account_high_freq,
                'low_freq': self.account_low_freq
            },
            'restart_info': {
                'restart_attempts': self.restart_attempts,
                'in_restart_cooldown': self.in_restart_cooldown,
                'last_restart': self.quality_stats['account_fetch'].get('last_restart')
            },
            'quality_stats': self.quality_stats,
            'retry_strategy': {
                'account_retries': f"{self.max_account_retries}次重试",
                'retry_delays': self.account_retry_delays,
                'total_attempts': self.max_account_retries + 1
            },
            'api_config': {
                'recvWindow': self.RECV_WINDOW,
                'session_reuse': True
            },
            'schedule': {
                'account': '启动后4分钟开始，5次指数退避重试，然后自适应频率',
                'data_type': '仅获取账户数据（包含持仓信息）',
                'auto_restart': '遇到418/401错误立即重启（无限次）',
                'rate_limit': '429错误等待建议时间后继续（无需特殊处理）'
            },
            'endpoints': {
                'account': self.ACCOUNT_ENDPOINT,
                'base_url': self.BASE_URL
            },
            'data_destination': 'private_data_processing.manager'
        }

        return status
