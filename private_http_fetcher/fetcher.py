"""
私人HTTP数据获取器 - 权重监控修正版
专注于准确获取权重相关数据
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
    专注准确获取权重数据
    """
    
    def __init__(self):
        # 与private_ws_pool相同的结构
        self.brain_store = None          # DataManager实例
        self.running = False
        
        # API凭证
        self.api_key = None
        self.api_secret = None
        
        # 任务管理
        self.scheduler_task = None
        self.fetch_tasks = []
        
        # Session复用
        self.session = None
        
        # 状态标志
        self.account_fetched = False
        self.account_fetch_success = False
        
        # 重试策略
        self.account_retry_delays = [10, 20, 40, 60]
        self.max_account_retries = 4
        
        # 自适应频率控制
        self.account_check_interval = 1
        self.account_high_freq = 1
        self.account_low_freq = 60
        self.has_position = False
        self.last_log_time = 0
        self.log_interval = 60
        
        # 🔴 新增：权重数据追踪
        self.weight_data_history = []
        self.weight_debug_mode = True  # 开启详细权重日志
        
        # 连接质量统计
        self.quality_stats = {
            'account_fetch': {
                'total_attempts': 0,
                'success_attempts': 0,
                'last_success': None,
                'last_error': None,
                'success_rate': 100.0,
                'retry_count': 0,
                'weight_records': []  # 新增：记录权重数据
            }
        }
        
        # 币安API端点配置
        self.BASE_URL = "https://testnet.binancefuture.com"
        # self.BASE_URL = "https://fapi.binance.com"
        
        self.ACCOUNT_ENDPOINT = "/fapi/v3/account"
        
        # 配置
        self.RECV_WINDOW = 5000
        
        # 环境
        self.environment = "testnet" if "testnet" in self.BASE_URL else "live"
        logger.info(f"🔗 [HTTP获取器] 初始化完成（环境: {self.environment} | 权重调试模式开启）")
    
    async def start(self, brain_store):
        """
        启动获取器
        """
        logger.info(f"🚀 [HTTP获取器] 正在启动（权重调试模式）...")
        
        self.brain_store = brain_store
        self.running = True
        
        # 创建ClientSession
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)
        
        # 创建调度任务
        self.scheduler_task = asyncio.create_task(self._controlled_scheduler())
        
        logger.info("✅ [HTTP获取器] 调度器已启动，等待4分钟后执行账户获取")
        return True
    
    async def _controlled_scheduler(self):
        """
        受控调度器
        """
        try:
            # ========== 第一阶段：等待4分钟 ==========
            logger.info("⏳ [HTTP获取器] 第一阶段：等待4分钟...")
            for i in range(240):
                if not self.running:
                    return
                if i % 60 == 0:
                    remaining = 240 - i
                    logger.info(f"⏳ [HTTP获取器] 等待中...剩余{remaining}秒")
                await asyncio.sleep(1)
            
            logger.info("✅ [HTTP获取器] 4分钟等待完成，开始账户获取")
            
            # ========== 第二阶段：获取账户资产 ==========
            self.account_fetch_success = await self._fetch_account_with_retry()
            
            if self.account_fetch_success:
                logger.info("✅ [HTTP获取器] 账户获取成功")
                
                # ========== 第三阶段：启动自适应频率获取 ==========
                logger.info("⏳ [HTTP获取器] 账户成功后冷却30秒...")
                await asyncio.sleep(30)
                
                account_task = asyncio.create_task(self._fetch_account_adaptive_freq())
                self.fetch_tasks.append(account_task)
                logger.info("✅ [HTTP获取器] 自适应频率账户数据获取已启动")
            else:
                logger.warning("⚠️ [HTTP获取器] 账户获取失败，不启动后续任务")
                
        except asyncio.CancelledError:
            logger.info("🛑 [HTTP获取器] 调度器被取消")
        except Exception as e:
            logger.error(f"❌ [HTTP获取器] 调度器异常: {e}")
    
    async def _fetch_account_with_retry(self):
        """
        获取账户资产 - 重试机制
        """
        retry_count = 0
        total_attempts = 0
        
        # 第1次尝试
        logger.info(f"💰 [HTTP获取器] 账户获取第1次尝试...")
        result = await self._fetch_account_single()
        total_attempts += 1
        
        if result == 'PERMANENT_STOP':
            logger.error("🚨 [HTTP获取器] 遇到不可逆错误，停止所有重试")
            self.quality_stats['account_fetch']['retry_count'] = 0
            return False
        
        if result == True:
            self.quality_stats['account_fetch']['retry_count'] = 0
            return True
        
        # 重试
        while retry_count < self.max_account_retries and self.running:
            delay = self.account_retry_delays[retry_count]
            logger.info(f"⏳ [HTTP获取器] {delay}秒后重试账户获取（第{retry_count + 2}次尝试）...")
            await asyncio.sleep(delay)
            
            logger.info(f"💰 [HTTP获取器] 账户获取第{retry_count + 2}次尝试...")
            result = await self._fetch_account_single()
            total_attempts += 1
            retry_count += 1
            
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
        单次尝试获取账户资产
        """
        try:
            self.quality_stats['account_fetch']['total_attempts'] += 1
            
            api_key, api_secret = await self._get_fresh_credentials()
            if not api_key or not api_secret:
                logger.warning("⚠️ [HTTP获取器] 凭证读取失败，本次尝试跳过")
                self.quality_stats['account_fetch']['last_error'] = "凭证读取失败"
                return False
            
            # 请求参数
            params = {
                'timestamp': int(time.time() * 1000),
                'recvWindow': self.RECV_WINDOW
            }
            signed_params = self._sign_params(params, api_secret)
            url = f"{self.BASE_URL}{self.ACCOUNT_ENDPOINT}"
            headers = {'X-MBX-APIKEY': api_key}
            
            # 🔴 记录请求开始时间
            request_start = time.time()
            
            async with self.session.get(url, params=signed_params, headers=headers) as resp:
                # 🔴 获取所有相关header
                all_headers = dict(resp.headers)
                
                # 打印所有可能相关的权重header
                logger.info("=" * 60)
                logger.info("⚖️ [权重Header完整列表]:")
                for key, value in sorted(all_headers.items()):
                    key_lower = key.lower()
                    if any(term in key_lower for term in ['weight', 'order', 'rate', 'limit']):
                        logger.info(f"  {key}: {value}")
                
                # 🔴 重点监控这两个header
                used_weight = resp.headers.get('X-MBX-USED-WEIGHT')
                used_weight_1m = resp.headers.get('X-MBX-USED-WEIGHT-1M')
                
                logger.info(f"📊 [重点监控]:")
                logger.info(f"  X-MBX-USED-WEIGHT (单次): {used_weight or '未找到'}")
                logger.info(f"  X-MBX-USED-WEIGHT-1M (累计): {used_weight_1m or '未找到'}")
                
                # 记录权重数据
                weight_record = {
                    'timestamp': datetime.now().isoformat(),
                    'request_start': request_start,
                    'response_time': time.time(),
                    'used_weight': used_weight,
                    'used_weight_1m': used_weight_1m,
                    'all_headers': {k: v for k, v in all_headers.items() 
                                   if any(term in k.lower() for term in ['weight', 'order'])}
                }
                
                self.quality_stats['account_fetch']['weight_records'].append(weight_record)
                
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
                    
                    if resp.status == 418:
                        retry_after = int(resp.headers.get('Retry-After', 10))
                        logger.error(f"🚨 [HTTP获取器] IP被封禁(418)，需等待{retry_after}秒")
                        return 'PERMANENT_STOP'
                    
                    if resp.status == 401:
                        logger.error(f"🚨 [HTTP获取器] API密钥无效或权限不足(401)")
                        return 'PERMANENT_STOP'
                    
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
    
    async def _fetch_account_adaptive_freq(self):
        """
        自适应频率获取账户数据 - 修复权重获取
        """
        request_count = 0
        last_weight_values = {}  # 记录历史权重值
        
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
                
                # 请求参数
                params = {
                    'timestamp': int(time.time() * 1000),
                    'recvWindow': self.RECV_WINDOW
                }
                signed_params = self._sign_params(params, api_secret)
                url = f"{self.BASE_URL}{self.ACCOUNT_ENDPOINT}"
                headers = {'X-MBX-APIKEY': api_key}
                
                # 🔴 记录请求开始时间
                request_start = time.time()
                request_start_str = datetime.fromtimestamp(request_start).strftime('%H:%M:%S.%f')[:-3]
                
                async with self.session.get(url, params=signed_params, headers=headers) as resp:
                    # 🔴 获取所有header
                    all_headers = dict(resp.headers)
                    
                    # 🔴 精准获取权重header
                    used_weight = resp.headers.get('X-MBX-USED-WEIGHT')
                    used_weight_1m = resp.headers.get('X-MBX-USED-WEIGHT-1M')
                    
                    # 记录响应时间
                    response_time = time.time()
                    response_str = datetime.fromtimestamp(response_time).strftime('%H:%M:%S.%f')[:-3]
                    response_delay = response_time - request_start
                    
                    # 🔴 精确打印权重信息
                    logger.info("=" * 60)
                    logger.info(f"🕒 [时间戳] 请求: {request_start_str} | 响应: {response_str} | 延迟: {response_delay:.3f}s")
                    logger.info(f"📊 [权重数据] 请求#{request_count}:")
                    logger.info(f"  X-MBX-USED-WEIGHT: {used_weight or '未找到'}")
                    logger.info(f"  X-MBX-USED-WEIGHT-1M: {used_weight_1m or '未找到'}")
                    
                    # 记录历史数据
                    if used_weight_1m:
                        weight_val = int(used_weight_1m)
                        last_weight_values[request_count] = {
                            'timestamp': request_start,
                            'weight': weight_val,
                            'request_num': request_count
                        }
                        
                        # 🔴 分析变化
                        if len(last_weight_values) >= 2:
                            prev_req = request_count - 1
                            if prev_req in last_weight_values:
                                prev_weight = last_weight_values[prev_req]['weight']
                                time_diff = request_start - last_weight_values[prev_req]['timestamp']
                                weight_diff = weight_val - prev_weight
                                
                                logger.info(f"📈 [变化分析] 间隔: {time_diff:.1f}s | "
                                          f"权重变化: {weight_diff:+d} | "
                                          f"({prev_weight} → {weight_val})")
                    
                    if resp.status == 200:
                        data = await resp.json()
                        
                        # 检查持仓
                        positions = data.get('positions', [])
                        has_position_now = False
                        for pos in positions:
                            position_amt = float(pos.get('positionAmt', '0'))
                            if position_amt != 0:
                                has_position_now = True
                                break
                        
                        # 频率调整
                        if has_position_now:
                            if not self.has_position:
                                logger.info(f"🚀 [HTTP获取器] 检测到持仓，切换高频模式（1秒）")
                            self.account_check_interval = self.account_high_freq
                            self.has_position = True
                        else:
                            if self.has_position:
                                logger.info(f"💤 [HTTP获取器] 检测到清仓，切换低频模式（60秒）")
                            self.account_check_interval = self.account_low_freq
                            self.has_position = False
                        
                        # 控制日志频率
                        current_time = time.time()
                        if current_time - self.last_log_time >= self.log_interval:
                            if has_position_now:
                                positions_count = len([p for p in positions if float(p.get('positionAmt', '0')) != 0])
                                logger.info(f"📊 [HTTP获取器] 当前持仓{positions_count}个 | 高频模式 | 请求次数:{request_count}")
                            else:
                                logger.info(f"📊 [HTTP获取器] 当前无持仓 | 低频模式 | 请求次数:{request_count}")
                            self.last_log_time = current_time
                        
                        await self._push_data('http_account', data)
                        
                        # 更新统计
                        self.quality_stats['account_fetch']['success_attempts'] += 1
                        self.quality_stats['account_fetch']['total_attempts'] += 1
                        self.quality_stats['account_fetch']['last_success'] = datetime.now().isoformat()
                        self.quality_stats['account_fetch']['success_rate'] = (
                            self.quality_stats['account_fetch']['success_attempts'] / 
                            self.quality_stats['account_fetch']['total_attempts'] * 100
                        )
                        
                        await asyncio.sleep(self.account_check_interval)
                        
                    else:
                        error_text = await resp.text()
                        error_msg = f"HTTP {resp.status}: {error_text[:100]}"
                        self.quality_stats['account_fetch']['last_error'] = error_msg
                        logger.error(f"❌ [HTTP获取器] 账户请求失败 {error_msg}")
                        
                        if resp.status in [418, 401]:
                            retry_after = int(resp.headers.get('Retry-After', 3600))
                            logger.error(f"🚨 [HTTP获取器] 账户请求触发严重错误({resp.status})，等待{retry_after}秒后永久停止")
                            await asyncio.sleep(retry_after)
                            break
                        
                        if resp.status == 429:
                            retry_after = int(resp.headers.get('Retry-After', 60))
                            logger.warning(f"⚠️ [HTTP获取器] 账户请求触发频率限制(429)，等待{retry_after}秒")
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
    
    async def _get_fresh_credentials(self):
        """读取新鲜凭证"""
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
        """生成签名"""
        query = urllib.parse.urlencode(params)
        signature = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        params['signature'] = signature
        return params
    
    async def _push_data(self, data_type: str, raw_data: Dict):
        """推送数据"""
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
        """关闭获取器"""
        logger.info("🛑 [HTTP获取器] 正在关闭...")
        self.running = False
        
        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
        
        for task in self.fetch_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        if self.session:
            await self.session.close()
            logger.info("✅ [HTTP获取器] HTTP会话已关闭")
        
        logger.info("✅ [HTTP获取器] 已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取状态信息"""
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
            'quality_stats': self.quality_stats,
            'weight_debug_mode': self.weight_debug_mode,
            'schedule': {
                'account': '启动后4分钟开始，5次指数退避重试，然后自适应频率',
                'data_type': '仅获取账户数据（包含持仓信息）'
            },
            'endpoints': {
                'account': self.ACCOUNT_ENDPOINT,
                'base_url': self.BASE_URL
            },
            'data_destination': 'private_data_processing.manager'
        }
        
        return status