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
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Set, List
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

        # 连接质量统计（模仿pool_manager）
        self.quality_stats = {
            'account_fetch': {
                'total_attempts': 0,
                'success_attempts': 0,
                'last_success': None,
                'last_error': None,
                'success_rate': 100.0,
                'retry_count': 0
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
        
        # 🔴 新增：资金费查询相关配置（简化版）
        self.INCOME_ENDPOINT = "/fapi/v1/income"  # 资金流水接口
        self.FUNDING_RETRY_INTERVAL = 10  # 🔴 修改：每10秒重试一次
        self.FUNDING_MAX_RETRIES = 5  # 🔴 修改：强制重试5次
        self.FUNDING_QUERY_WINDOW_MS = 300 * 1000  # 5分钟查询窗口
        self.FUNDING_INITIAL_DELAY = 30  # 整点后延迟30秒
        self.last_funding_trigger_hour = -1  # 上次触发资金费查询的UTC小时
        
        logger.info(
            f"🔗 [HTTP获取器] 初始化完成（环境: {self.environment} | 自适应频率 | 整点资金费查询 | 强制重试5次）")

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
                logger.info("✅ [HTTP获取器] 账户获取成功，启动自适应频率的账户数据获取任务")

                # ========== 第三阶段：启动自适应频率的账户数据获取 ==========
                # 再等待30秒，确保完全冷却
                logger.info("⏳ [HTTP获取器] 账户成功后冷却30秒...")
                await asyncio.sleep(30)

                # 🔴 修改：启动自适应频率的账户数据获取任务
                account_task = asyncio.create_task(
                    self._fetch_account_adaptive_freq())
                self.fetch_tasks.append(account_task)
                logger.info(
                    "✅ [HTTP获取器] 自适应频率账户数据获取已启动（有持仓1秒/无持仓60秒 + 整点资金费查询）")
            else:
                logger.warning("⚠️ [HTTP获取器] 账户获取5次尝试均失败，不启动后续任务")

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
            logger.info(
                f"⏳ [HTTP获取器] {delay}秒后重试账户获取（第{retry_count + 2}次尝试）...")
            await asyncio.sleep(delay)

            logger.info(f"💰 [HTTP获取器] 账户获取第{retry_count + 2}次尝试...")
            result = await self._fetch_account_single()
            total_attempts += 1
            retry_count += 1

            # 🔴 修复：检查是否遇到418或401
            if result == 'PERMANENT_STOP':
                logger.error(
                    f"🚨 [HTTP获取器] 第{retry_count}次尝试遇到不可逆错误，停止重试")
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
        单次尝试获取账户资产（优化版：添加recvWindow）

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

                    # 🔴 关键修复：418（IP封禁）- 立即停止所有重试
                    if resp.status == 418:
                        retry_after = int(resp.headers.get('Retry-After', 10))
                        logger.error(
                            f"🚨 [HTTP获取器] IP被封禁(418)，需等待{retry_after}秒")
                        # 🔴 修复：返回特殊标记，让上层知道要停止所有重试
                        return 'PERMANENT_STOP'

                    # 🔴 关键修复：401（API密钥无效或权限不足）- 立即停止
                    if resp.status == 401:
                        logger.error(
                            f"🚨 [HTTP获取器] API密钥无效或权限不足(401)")
                        logger.error(f"   当前环境: {self.environment}")
                        logger.error(f"   请检查：")
                        logger.error(
                            f"   1. API密钥是否匹配当前环境（模拟/真实）")
                        logger.error(
                            f"   2. API密钥是否启用了合约权限")
                        logger.error(f"   3. IP白名单是否正确")
                        return 'PERMANENT_STOP'

                    # 🔴 优化：429（频率限制）- 等待后重试
                    if resp.status == 429:
                        retry_after = int(resp.headers.get('Retry-After', 60))
                        logger.warning(
                            f"⚠️ [HTTP获取器] 触发频率限制(429)，等待{retry_after}秒后重试")
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

    async def _fetch_account_adaptive_freq(self):
        """
        自适应频率获取账户数据（优化版：有持仓1秒/无持仓60秒 + 日志控制）
        从账户数据本身的positions字段判断是否有持仓
        """
        request_count = 0

        # 初始等待
        await asyncio.sleep(30)

        while self.running:
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
                        active_symbols = set()  # 🔴 新增：记录活跃合约符号
                        for pos in positions:
                            position_amt = float(pos.get('positionAmt', '0'))
                            if position_amt != 0:  # 仓位不为0表示有真实持仓
                                has_position_now = True
                                active_symbols.add(pos['symbol'])  # 🔴 新增：记录符号
                        
                        # 🔴 新增：整点资金费查询触发
                        await self._trigger_funding_fetch_if_needed(
                            api_key, api_secret, has_position_now, active_symbols
                        )

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
                                positions_count = len(active_symbols)  # 🔴 修改：使用已计算的活跃数量
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
                        logger.error(f"❌ [HTTP获取器] 账户请求失败 {error_msg}")

                        # 🔴 优化：正确处理418和401（永久停止）
                        if resp.status in [418, 401]:
                            retry_after = int(
                                resp.headers.get('Retry-After', 3600))
                            logger.error(
                                f"🚨 [HTTP获取器] 账户请求触发严重错误({resp.status})，等待{retry_after}秒后永久停止")
                            await asyncio.sleep(retry_after)
                            # 账户任务遇到418/401也停止
                            logger.error(f"🚨 [HTTP获取器] 账户任务永久停止")
                            break

                        # 🔴 优化：正确处理429
                        if resp.status == 429:
                            retry_after = int(
                                resp.headers.get('Retry-After', 60))
                            logger.warning(
                                f"⚠️ [HTTP获取器] 账户请求触发频率限制(429)，等待{retry_after}秒")
                            await asyncio.sleep(retry_after)
                        else:
                            await asyncio.sleep(self.account_check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                error_msg = str(e)
                self.quality_stats['account_fetch']['last_error'] = error_msg
                logger.error(f"❌ [HTTP获取器] 账户循环异常: {e}")
                await asyncio.sleep(self.account_check_interval)

    async def _trigger_funding_fetch_if_needed(self, api_key: str, api_secret: str, 
                                              has_position: bool, active_symbols: Set[str]):
        """
        在整点触发资金费查询。
        规则：每个UTC整点，如果有持仓，则在整点后延迟30秒启动查询任务。
        """
        if not has_position:
            # 无持仓，无需触发，并重置触发标记
            self.last_funding_trigger_hour = -1
            return

        # 获取当前UTC时间
        utc_now = datetime.now(timezone.utc)
        current_hour = utc_now.hour

        # 检查是否已经在本小时触发过
        if current_hour == self.last_funding_trigger_hour:
            return

        # 计算从当前时间到整点已过去的秒数
        minutes_into_hour = utc_now.minute
        seconds_into_hour = minutes_into_hour * 60 + utc_now.second

        # 只在整点后的最初1分钟内进行判断和启动，避免在小时中段误触发
        if seconds_into_hour > 60:
            return

        # 记录触发时间，避免本小时内重复触发
        self.last_funding_trigger_hour = current_hour
        logger.info(f"🕐 [资金费] 在UTC {current_hour:02d}:00:00 检测到持仓，准备触发查询...")

        # 延迟30秒后启动独立的资金费查询任务
        asyncio.create_task(
            self._execute_funding_fetch_task(api_key, api_secret, active_symbols, utc_now)
        )

    async def _execute_funding_fetch_task(self, api_key: str, api_secret: str, 
                                         active_symbols: Set[str], trigger_time: datetime):
        """
        执行资金费查询任务 - 简化版：强制重试5次，直接推送数据
        """
        await asyncio.sleep(self.FUNDING_INITIAL_DELAY)

        logger.info(f"🚀 [资金费] 开始执行资金费查询任务，活跃合约: {active_symbols}")
        logger.info(f"🔍 [资金费] 策略：强制重试5次，每10秒一次，不验证数据完整性")

        retry_count = 0
        max_attempts = self.FUNDING_MAX_RETRIES  # 5次重试

        while retry_count < max_attempts and self.running:
            attempt_num = retry_count + 1
            logger.info(f"🔄 [资金费] 第{attempt_num}次尝试（共{max_attempts}次）")

            success, income_data = await self._fetch_funding_income_single(
                api_key, api_secret, trigger_time
            )

            if success:
                # 🔴 关键修改：无论数据是否完整，都直接推送
                logger.info(f"✅ [资金费] 第{attempt_num}次尝试获取到数据，立即推送")
                
                # 打印原始数据以便调试
                if income_data:
                    logger.info(f"📊 [资金费] 获取到{len(income_data)}条记录")
                    # 打印前几条记录
                    for i, record in enumerate(income_data[:3]):
                        logger.info(f"📊 [资金费] 记录{i+1}: {record}")
                
                # 直接推送原始数据
                await self._push_data('http_funding_income', {
                    'trigger_time': trigger_time.isoformat(),
                    'attempt_number': attempt_num,
                    'total_attempts': max_attempts,
                    'active_symbols': list(active_symbols),
                    'income_data': income_data if income_data else [],
                    'status': 'success' if income_data else 'empty'
                })
                
                logger.info(f"✅ [资金费] 第{attempt_num}次数据已推送")
                
                # 🔴 立即继续下一次尝试，不等待
                retry_count += 1
                if retry_count < max_attempts:
                    logger.info(f"⏳ [资金费] 等待{self.FUNDING_RETRY_INTERVAL}秒后继续下一次尝试...")
                    await asyncio.sleep(self.FUNDING_RETRY_INTERVAL)
                else:
                    logger.info(f"✅ [资金费] 已完成全部{max_attempts}次尝试")
            else:
                # 请求失败
                logger.warning(f"⚠️ [资金费] 第{attempt_num}次尝试失败")
                
                # 即使失败也推送空数据用于记录
                await self._push_data('http_funding_income', {
                    'trigger_time': trigger_time.isoformat(),
                    'attempt_number': attempt_num,
                    'total_attempts': max_attempts,
                    'active_symbols': list(active_symbols),
                    'income_data': [],
                    'status': 'failed'
                })
                
                retry_count += 1
                if retry_count < max_attempts:
                    logger.info(f"⏳ [资金费] 等待{self.FUNDING_RETRY_INTERVAL}秒后继续下一次尝试...")
                    await asyncio.sleep(self.FUNDING_RETRY_INTERVAL)

        logger.info(f"✅ [资金费] 任务完成：执行了{retry_count}次尝试")

    async def _fetch_funding_income_single(self, api_key: str, api_secret: str, 
                                          trigger_time: datetime):
        """
        单次获取资金费历史记录
        🔴 简化版：不验证数据，直接返回
        """
        try:
            current_time_ms = int(time.time() * 1000)
            window_start_ms = current_time_ms - self.FUNDING_QUERY_WINDOW_MS

            # 🔴 测试：去掉incomeType限制，查看所有类型的记录
            params = {
                'timestamp': current_time_ms,
                'recvWindow': self.RECV_WINDOW,
                # 'incomeType': 'FUNDING_FEE',  # 先注释掉，查看所有类型
                'startTime': window_start_ms,
                'limit': 100
            }

            signed_params = self._sign_params(params, api_secret)
            url = f"{self.BASE_URL}{self.INCOME_ENDPOINT}"
            headers = {'X-MBX-APIKEY': api_key}

            # 🔴 添加详细请求日志
            logger.info(f"🔍 [资金费] 发送请求到: {url}")
            logger.info(f"🔍 [资金费] 查询参数: timestamp={params['timestamp']}, "
                       f"startTime={params['startTime']}")

            async with self.session.get(url, params=signed_params, headers=headers) as resp:
                response_text = await resp.text()
                
                if resp.status == 200:
                    try:
                        data = json.loads(response_text)
                        logger.info(f"🔍 [资金费] 请求成功，状态码: {resp.status}")
                        return True, data
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ [资金费] JSON解析失败: {e}")
                        logger.error(f"❌ [资金费] 响应内容: {response_text[:200]}")
                        return False, None
                else:
                    logger.error(f"❌ [资金费] 请求失败: HTTP {resp.status}")
                    logger.error(f"❌ [资金费] 响应内容: {response_text[:200]}")
                    return False, None

        except asyncio.TimeoutError:
            logger.error(f"⏱️ [资金费] 请求超时")
            return False, None
        except Exception as e:
            logger.error(f"❌ [资金费] 请求异常: {e}")
            return False, None

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
            'funding_fetch': {
                'enabled': True,
                'trigger_hour': self.last_funding_trigger_hour,
                'initial_delay': self.FUNDING_INITIAL_DELAY,
                'query_window_ms': self.FUNDING_QUERY_WINDOW_MS,
                'max_retries': self.FUNDING_MAX_RETRIES,
                'retry_interval': self.FUNDING_RETRY_INTERVAL,
                'strategy': '强制重试5次，每10秒一次，直接推送数据不验证'
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
                'funding': '整点触发（仅当有持仓时），延迟30秒，强制重试5次',
                'data_type': '账户数据 + 资金费数据'
            },
            'endpoints': {
                'account': self.ACCOUNT_ENDPOINT,
                'funding_income': self.INCOME_ENDPOINT,
                'base_url': self.BASE_URL
            },
            'data_destination': 'private_data_processing.manager'
        }

        return status