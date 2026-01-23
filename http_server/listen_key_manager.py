# listen_key_manager.py
"""
ListenKey管理器 - 智能重试版
🚨 添加完整的失败重试机制，区分错误类型
"""
import asyncio
import logging
import aiohttp
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
import re

logger = logging.getLogger(__name__)

class ListenKeyManager:
    """ListenKey生命周期管理器 - 智能重试版"""
    
    def __init__(self, brain_store):
        """
        参数:
            brain_store: 大脑数据存储接口
        """
        self.brain = brain_store
        
        # 状态管理
        self.running = False
        self.maintenance_task = None
        
        # 配置
        self.renewal_interval = 25 * 60  # 25分钟正常续期间隔
        self.api_check_interval = 5  # 5秒检查API
        
        # HTTP配置
        self.binance_testnet_url = "https://testnet.binancefuture.com/fapi/v1/listenKey"
        
        # 🎯 新增：重试配置
        self.max_token_retries = 3  # 令牌操作最大重试次数
        self.retry_strategies = {
            # 错误码 -> (操作类型, 延迟秒数, 描述)
            -1001: ('retry_same', 30, '交易所内部错误'),
            -1003: ('wait_long', 60, '请求频率限制'),
            -1022: ('get_new', 10, '签名错误，需重新获取'),
            -2014: ('wait_long', 300, 'API密钥无效'),
            -2015: ('wait_long', 300, 'API密钥无效或IP限制'),
            'network_error': ('retry_same', 10, '网络错误'),
            'timeout_error': ('retry_same', 15, '连接超时'),
            'default': ('retry_same', 10, '临时错误')
        }
        
        logger.info("🔑 ListenKey管理器初始化完成（智能重试版）")
    
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
    
    # ==================== 核心维护循环 ====================
    
    async def _maintenance_loop(self):
        """ListenKey维护主循环 - 智能重试版"""
        logger.info("⏰ ListenKey维护循环已启动（智能重试）")
        
        while self.running:
            try:
                # 🎯 步骤1：检查并获取令牌（带智能重试）
                success = await self._check_and_renew_keys_with_retry()
                
                if success:
                    # ✅ 成功：等待25分钟正常续期
                    logger.info(f"✅ 令牌操作成功，等待{self.renewal_interval/60}分钟后正常续期")
                    await asyncio.sleep(self.renewal_interval)
                else:
                    # ❌ 失败：等待较短时间后重试完整流程
                    logger.warning(f"⚠️ 令牌操作失败，30秒后重试完整流程")
                    await asyncio.sleep(30)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"ListenKey维护循环异常: {e}")
                await asyncio.sleep(60)
    
    async def _check_and_renew_keys_with_retry(self) -> bool:
        """检查并续期所有交易所的listenKey - 带智能重试"""
        try:
            # 🎯 重点：只处理币安（当前唯一需要listenKey的）
            return await self._check_binance_key_with_retry()
        except Exception as e:
            logger.error(f"检查续期失败: {e}")
            return False
    
    async def _check_binance_key_with_retry(self) -> bool:
        """检查并续期币安listenKey - 智能重试版"""
        logger.info("🔍 开始币安令牌检查流程（智能重试）...")
        
        # ============ 阶段1：从大脑模块读取API凭证（带重试） ============
        api_creds = await self._get_api_credentials_with_retry('binance')
        if not api_creds:
            logger.warning("⚠️ 无法获取API凭证，跳过本次令牌检查")
            return False
        
        # ============ 阶段2：读取当前令牌状态 ============
        current_key = await self.brain.get_listen_key('binance')
        
        # ============ 阶段3：连接交易所获取/续期令牌（带智能重试） ============
        if current_key:
            logger.info("🔄 尝试续期现有币安listenKey")
            operation = 'keep_alive'
            result = await self._execute_token_operation_with_retry(
                operation, api_creds['api_key'], current_key
            )
        else:
            logger.info("🆕 首次获取币安listenKey")
            operation = 'get_new'
            result = await self._execute_token_operation_with_retry(
                operation, api_creds['api_key']
            )
        
        # ============ 阶段4：处理操作结果 ============
        if result['success']:
            # ✅ 成功：推送令牌到大脑
            new_key = result.get('listenKey', current_key)
            if new_key:
                await self.brain.save_listen_key('binance', new_key)
                logger.info(f"✅ 币安listenKey已获取/更新: {new_key[:5]}...")
                return True
            else:
                logger.warning("⚠️ 操作成功但未返回新令牌")
                return False
        else:
            # ❌ 失败：已记录错误，返回失败
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
                logger.info(f"✅ 第{retry_count}次尝试：成功获取{exchange} API凭证")
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
        return {
            'running': self.running,
            'current_key': await self.brain.get_listen_key('binance'),
            'config': {
                'renewal_interval': self.renewal_interval,
                'api_check_interval': self.api_check_interval,
                'max_token_retries': self.max_token_retries,
                'binance_url': self.binance_testnet_url
            },
            'retry_strategies': {
                k: {'action': v[0], 'delay': v[1], 'reason': v[2]}
                for k, v in self.retry_strategies.items()
            },
            'implementation': 'direct_http_with_smart_retry',
            'timestamp': datetime.now().isoformat()
        }