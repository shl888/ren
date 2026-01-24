# listen_key_manager.py
"""
ListenKey管理器 - 推送版本
"""
import asyncio
import logging
import aiohttp
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import re

logger = logging.getLogger(__name__)

class ListenKeyManager:
    """ListenKey生命周期管理器 - 推送模式"""
    
    def __init__(self, brain_store):
        self.brain = brain_store
        
        # 状态管理
        self.running = False
        self.maintenance_task = None
        
        # 🎯 新增：关键时间戳
        self.last_token_time = None  # 上次成功获取/续期令牌的时间
        
        # 配置
        self.renewal_interval = 25 * 60  # 25分钟 = 1500秒
        self.api_check_interval = 5  # 5秒检查API
        
        # HTTP配置
        self.binance_testnet_url = "https://testnet.binancefuture.com/fapi/v1/listenKey"
        
        # 重试配置
        self.max_token_retries = 3
        self.retry_strategies = {
            -1001: ('retry_same', 30, '交易所内部错误'),
            -1003: ('wait_long', 60, '请求频率限制'),
            -1022: ('get_new', 10, '签名错误，需重新获取'),
            -2014: ('wait_long', 300, 'API密钥无效'),
            -2015: ('wait_long', 300, 'API密钥无效或IP限制'),
            'network_error': ('retry_same', 10, '网络错误'),
            'timeout_error': ('retry_same', 15, '连接超时'),
            'default': ('retry_same', 10, '临时错误')
        }
        
        logger.info("🔑 ListenKey管理器初始化完成（推送模式）")
    
    # ==================== 核心维护循环 ====================
    
    async def start(self) -> bool:
        """启动ListenKey管理服务"""
        if self.running:
            logger.warning("ListenKey管理服务已在运行")
            return True
        
        logger.info("🚀 启动ListenKey管理服务...")
        self.running = True
        
        # 启动维护循环
        self.maintenance_task = asyncio.create_task(self._maintenance_loop())
        
        logger.info("✅ ListenKey管理服务已启动")
        return True
    
    async def stop(self):
        """停止ListenKey管理服务"""
        logger.info("🛑 停止ListenKey管理服务...")
        self.running = False
        
        if self.maintenance_task:
            self.maintenance_task.cancel()
            try:
                await self.maintenance_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ ListenKey管理服务已停止")
    
    async def _maintenance_loop(self):
        """基于时间戳的精确续期循环"""
        logger.info("⏰ ListenKey令牌获取维护循环已启动（时间戳精确版）")
        
        # 🎯 首次启动：立即执行，获取初始时间戳
        await self._execute_and_update_timestamp()
        
        while self.running:
            try:
                # 🎯 1. 计算距离下次续期还需等待多久
                wait_seconds = self._calculate_wait_time()
                
                if wait_seconds > 0:
                    # 最多睡5分钟，然后重新检查（避免长时间阻塞）
                    sleep_time = min(wait_seconds, 300)
                    logger.debug(f"⏳ 等待{sleep_time:.0f}秒后检查...")
                    await asyncio.sleep(sleep_time)
                    continue
                
                # 🎯 2. 到达续期时间，执行操作
                logger.info("🕐 到达续期时间，执行令牌操作")
                success = await self._execute_and_update_timestamp()
                
                if not success:
                    # ❌ 操作失败：等待30秒后重试
                    logger.warning("⚠️ 令牌操作失败，30秒后重试")
                    await asyncio.sleep(30)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"续期循环异常: {e}")
                await asyncio.sleep(60)
    
    def _calculate_wait_time(self) -> float:
        """🎯 核心：计算需要等待的时间（基于时间戳）"""
        if not self.last_token_time:
            # 没有时间戳，立即执行
            return 0
        
        now = datetime.now()
        elapsed = (now - self.last_token_time).total_seconds()
        
        # 如果已经超过25分钟，立即执行
        if elapsed >= self.renewal_interval:
            return 0
        
        # 否则等待剩余时间
        wait_time = self.renewal_interval - elapsed
        
        # 避免负数（时钟回拨等情况）
        return max(0, wait_time)
    
    async def _execute_and_update_timestamp(self) -> bool:
        """执行令牌操作，成功后更新时间戳"""
        try:
            # 执行令牌操作
            logger.info("🔍 执行令牌检查流程...")
            success = await self._check_and_renew_keys_with_retry()
            
            if success:
                # 🎯 成功：更新时间戳！
                self.last_token_time = datetime.now()
                logger.info(f"✅ 令牌操作成功，时间戳更新: {self.last_token_time.strftime('%H:%M:%S')}")
                
                # 计算距离下次续期时间
                next_time = self.last_token_time + timedelta(seconds=self.renewal_interval)
                from_now = (next_time - datetime.now()).total_seconds()
                logger.info(f"📅 下次续期时间: {next_time.strftime('%H:%M:%S')} ({from_now/60:.1f}分钟后)")
            else:
                logger.error("❌ 令牌操作失败")
            
            return success
            
        except Exception as e:
            logger.error(f"执行令牌操作异常: {e}")
            return False
    
    # ==================== 令牌操作逻辑 ====================
    
    async def _check_and_renew_keys_with_retry(self) -> bool:
        """检查并续期所有交易所的listenKey"""
        try:
            return await self._check_binance_key_with_retry()
        except Exception as e:
            logger.error(f"检查续期失败: {e}")
            return False
    
    async def _check_binance_key_with_retry(self) -> bool:
        """检查并续期币安listenKey"""
        # 1. 从大脑获取API凭证
        api_creds = await self._get_api_credentials_with_retry('binance')
        if not api_creds:
            logger.warning("⚠️ 无法从大脑获取API凭证，跳过本次令牌检查")
            return False
        
        # 2. 获取大脑当前令牌状态
        current_key = await self.brain.get_listen_key('binance')
        
        # 3. 连接交易所执行令牌操作
        if current_key:
            logger.info(f"🔄 尝试续期现有币安listenKey: {current_key[:5]}...")
            result = await self._execute_token_operation_with_retry(
                'keep_alive', api_creds['api_key'], current_key
            )
        else:
            logger.info("🆕 首次获取币安listenKey")
            result = await self._execute_token_operation_with_retry(
                'get_new', api_creds['api_key']
            )
        
        # 4. 处理结果并推送
        if result['success']:
            new_key = result.get('listenKey', current_key)
            if new_key:
                # ==================== 【推送：HTTP模块将令牌推送给大脑】 ====================
                await self.brain.receive_private_data({
                    'exchange': 'binance',
                    'data_type': 'listen_key',
                    'data': {
                        'listenKey': new_key,
                        'source': 'http_module'
                    }
                })
                logger.info(f"✅ 【推送】币安listenKey已推送给大脑: {new_key[:5]}...")
                return True
            else:
                logger.warning("⚠️ 操作成功但未返回新令牌")
        
        return False
    
    # ==================== 智能重试核心方法 ====================
    
    async def _get_api_credentials_with_retry(self, exchange: str) -> Optional[Dict]:
        """带重试获取API凭证"""
        retry_count = 0
        max_retries = 10  # 最多尝试10次
        
        while self.running and retry_count < max_retries:
            retry_count += 1
            
            api_creds = await self.brain.get_api_credentials(exchange)
            if api_creds and api_creds.get('api_key'):
                logger.debug(f"✅ 第{retry_count}次尝试：成功获取{exchange} API凭证")
                return api_creds
            else:
                if retry_count < max_retries:
                    logger.debug(f"⏳ 第{retry_count}次尝试：{exchange} API凭证未就绪，{self.api_check_interval}秒后重试...")
                    await asyncio.sleep(self.api_check_interval)
                else:
                    logger.warning(f"⚠️ 已尝试{max_retries}次，仍无法获取{exchange} API凭证")
        
        return None
    
    async def _execute_token_operation_with_retry(self, operation: str, api_key: str, 
                                                listen_key: str = None) -> Dict[str, Any]:
        """执行令牌操作（获取/续期）带智能重试"""
        attempts = []
        
        for attempt in range(self.max_token_retries):
            attempt_num = attempt + 1
            logger.info(f"🔄 第{attempt_num}/{self.max_token_retries}次尝试执行令牌操作: {operation}")
            
            try:
                # 🎯 每次重试都重新获取API（可能已更新）
                api_creds = await self.brain.get_api_credentials('binance')
                if not api_creds:
                    return {
                        'success': False,
                        'error': 'API凭证已失效',
                        'attempts': attempts
                    }
                
                # 执行操作
                if operation == 'get_new':
                    result = await self._get_binance_listen_key(api_creds['api_key'])
                else:  # 'keep_alive'
                    result = await self._keep_alive_binance_key(api_creds['api_key'], listen_key)
                
                # 记录尝试
                attempts.append({
                    'attempt': attempt_num,
                    'success': result.get('success', False),
                    'error': result.get('error', ''),
                    'timestamp': datetime.now().isoformat()
                })
                
                if result.get('success'):
                    # ✅ 成功
                    logger.info(f"✅ 第{attempt_num}次尝试成功")
                    return {**result, 'attempts': attempts}
                else:
                    # ❌ 失败：分析错误并决定是否重试
                    error_msg = result.get('error', '')
                    error_code = self._extract_error_code(error_msg)
                    strategy = self._get_retry_strategy(error_code, error_msg)
                    
                    logger.warning(f"⚠️ 第{attempt_num}次尝试失败: {error_msg}")
                    logger.info(f"📋 错误类型: {strategy['reason']}")
                    
                    if attempt_num < self.max_token_retries:
                        # 还有重试机会
                        logger.info(f"⏳ {strategy['delay']}秒后重试...")
                        await asyncio.sleep(strategy['delay'])
                        
                        # 根据策略决定下一步操作
                        if strategy['action'] == 'get_new':
                            # 切换为获取新令牌
                            operation = 'get_new'
                            logger.info("🔄 切换到获取新令牌模式")
                    else:
                        # 重试次数用尽
                        logger.error(f"🚨 所有{self.max_token_retries}次尝试均失败")
                        return {**result, 'attempts': attempts}
                        
            except asyncio.TimeoutError as e:
                # 网络超时
                attempts.append({
                    'attempt': attempt_num,
                    'success': False,
                    'error': f'Timeout: {str(e)}',
                    'timestamp': datetime.now().isoformat()
                })
                
                if attempt_num < self.max_token_retries:
                    strategy = self.retry_strategies['timeout_error']
                    logger.warning(f"⏱️ 第{attempt_num}次尝试超时，{strategy[1]}秒后重试...")
                    await asyncio.sleep(strategy[1])
                else:
                    return {
                        'success': False,
                        'error': f'多次超时: {str(e)}',
                        'attempts': attempts
                    }
                    
            except Exception as e:
                # 其他异常
                attempts.append({
                    'attempt': attempt_num,
                    'success': False,
                    'error': f'Exception: {str(e)}',
                    'timestamp': datetime.now().isoformat()
                })
                
                if attempt_num < self.max_token_retries:
                    strategy = self.retry_strategies['default']
                    logger.error(f"❌ 第{attempt_num}次尝试异常: {e}")
                    await asyncio.sleep(strategy[1])
                else:
                    return {
                        'success': False,
                        'error': f'多次异常: {str(e)}',
                        'attempts': attempts
                    }
        
        # 不应该执行到这里
        return {
            'success': False,
            'error': '未知错误',
            'attempts': attempts
        }
    
    # ==================== 错误处理和分析 ====================
    
    def _extract_error_code(self, error_msg: str) -> int:
        """从错误消息提取币安错误码"""
        if not error_msg:
            return 0
        
        # 尝试匹配JSON格式错误
        json_match = re.search(r'"code":\s*(-?\d+)', error_msg)
        if json_match:
            return int(json_match.group(1))
        
        # 尝试匹配文本格式错误
        code_match = re.search(r'code[:\s]+(-?\d+)', error_msg, re.IGNORECASE)
        if code_match:
            return int(code_match.group(1))
        
        # 根据关键词判断
        if 'API-key' in error_msg and 'invalid' in error_msg:
            return -2014  # API无效
        elif 'Signature' in error_msg or 'signature' in error_msg:
            return -1022  # 签名错误
        elif 'Too many requests' in error_msg or 'rate limit' in error_msg.lower():
            return -1003  # 频率限制
        elif 'Internal error' in error_msg:
            return -1001  # 内部错误
        elif 'timeout' in error_msg.lower() or 'timed out' in error_msg.lower():
            return 'timeout_error'  # 自定义超时错误码
        elif 'network' in error_msg.lower() or 'connection' in error_msg.lower():
            return 'network_error'  # 自定义网络错误码
        
        return 0  # 未知错误
    
    def _get_retry_strategy(self, error_code, error_msg: str) -> Dict[str, Any]:
        """获取重试策略"""
        if error_code in self.retry_strategies:
            strategy = self.retry_strategies[error_code]
            return {
                'action': strategy[0],
                'delay': strategy[1],
                'reason': strategy[2]
            }
        elif isinstance(error_code, str) and error_code in self.retry_strategies:
            strategy = self.retry_strategies[error_code]
            return {
                'action': strategy[0],
                'delay': strategy[1],
                'reason': strategy[2]
            }
        else:
            strategy = self.retry_strategies['default']
            return {
                'action': strategy[0],
                'delay': strategy[1],
                'reason': f'未知错误: {error_msg[:50]}...'
            }
    
    # ==================== HTTP操作方法 ====================
    
    async def _get_binance_listen_key(self, api_key: str) -> Dict[str, Any]:
        """直接HTTP获取币安listenKey"""
        try:
            url = self.binance_testnet_url
            headers = {"X-MBX-APIKEY": api_key}
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, timeout=30) as response:
                    response_text = await response.text()
                    
                    try:
                        data = json.loads(response_text)
                    except json.JSONDecodeError:
                        return {
                            "success": False,
                            "error": f"响应不是有效JSON: {response_text[:100]}..."
                        }
                    
                    if 'listenKey' in data:
                        logger.info("✅ [HTTP] 币安listenKey获取成功")
                        return {"success": True, "listenKey": data['listenKey']}
                    else:
                        error_msg = data.get('msg', 'Unknown error')
                        error_code = data.get('code', 0)
                        logger.error(f"❌ [HTTP] 币安listenKey获取失败 [{error_code}]: {error_msg}")
                        return {
                            "success": False,
                            "error": f"[{error_code}] {error_msg}",
                            "raw_response": response_text
                        }
                        
        except asyncio.TimeoutError:
            return {
                "success": False,
                "error": "请求超时（30秒）"
            }
        except aiohttp.ClientError as e:
            return {
                "success": False,
                "error": f"网络错误: {str(e)}"
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"异常: {str(e)}"
            }
    
    async def _keep_alive_binance_key(self, api_key: str, listen_key: str) -> Dict[str, Any]:
        """直接HTTP延长币安listenKey有效期"""
        try:
            url = self.binance_testnet_url
            headers = {"X-MBX-APIKEY": api_key}
            
            async with aiohttp.ClientSession() as session:
                async with session.put(url, headers=headers, timeout=30) as response:
                    response_text = await response.text()
                    
                    try:
                        data = json.loads(response_text)
                    except json.JSONDecodeError:
                        return {
                            "success": False,
                            "error": f"响应不是有效JSON: {response_text[:100]}..."
                        }
                    
                    if response.status == 200:
                        logger.debug(f"✅ [HTTP] 币安listenKey续期成功: {listen_key[:10]}...")
                        return {"success": True}
                    else:
                        error_msg = data.get('msg', f'HTTP {response.status}')
                        error_code = data.get('code', 0)
                        logger.warning(f"⚠️ [HTTP] 币安listenKey续期失败 [{error_code}]: {error_msg}")
                        return {
                            "success": False,
                            "error": f"[{error_code}] {error_msg}",
                            "raw_response": response_text
                        }
                        
        except asyncio.TimeoutError:
            return {
                "success": False,
                "error": "请求超时（30秒）"
            }
        except aiohttp.ClientError as e:
            return {
                "success": False,
                "error": f"网络错误: {str(e)}"
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"异常: {str(e)}"
            }
    
    # ==================== 公共接口 ====================
    
    async def get_current_key(self, exchange: str) -> Optional[str]:
        """获取当前有效的listenKey - 从大脑获取"""
        return await self.brain.get_listen_key(exchange)
    
    async def force_renew_key(self, exchange: str) -> Optional[str]:
        """强制更新指定交易所的listenKey"""
        logger.info(f"🔄 强制更新{exchange}的listenKey...")
        success = await self._check_binance_key_with_retry()
        if success:
            return await self.brain.get_listen_key(exchange)
        return None
    
    async def get_status(self) -> Dict[str, Any]:
        """获取管理器状态"""
        status = {
            'running': self.running,
            'last_token_time': self.last_token_time.isoformat() if self.last_token_time else None,
            'current_key': await self.brain.get_listen_key('binance'),
            'config': {
                'renewal_interval': self.renewal_interval,
                'api_check_interval': self.api_check_interval,
                'max_token_retries': self.max_token_retries,
            },
            'timestamp': datetime.now().isoformat()
        }
        
        # 计算下次续期时间
        if self.last_token_time:
            next_time = self.last_token_time + timedelta(seconds=self.renewal_interval)
            now = datetime.now()
            seconds_until_next = (next_time - now).total_seconds()
            
            status['next_renewal_time'] = next_time.isoformat()
            status['seconds_until_next'] = max(0, seconds_until_next)
            status['minutes_until_next'] = max(0, seconds_until_next / 60)
        
        return status
