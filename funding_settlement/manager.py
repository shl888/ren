"""
资金费率结算管理器 - 带显微镜日志版（正规化改造）
"""
import asyncio
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
import aiohttp
import json
import traceback

# 设置导入路径
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared_data.data_store import data_store

logger = logging.getLogger(__name__)


class FundingSettlementManager:
    BINANCE_FUNDING_RATE_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
    API_WEIGHT_PER_REQUEST = 10
    
    def __init__(self):
        self.last_fetch_time: Optional[float] = None
        self.manual_fetch_count: int = 0
        self.last_manual_fetch_hour: Optional[int] = None
        self.is_auto_fetched: bool = False
        
        logger.info("=" * 60)
        logger.info("✅【历史费率】 FundingSettlementManager 初始化完成")
        logger.info(f"【历史费率】 API端点: {self.BINANCE_FUNDING_RATE_URL}")
        logger.info("=" * 60)
    
    async def fetch_funding_settlement(self, max_retries: int = 3) -> Dict[str, Any]:
        """
        获取币安最近结算周期的资金费率 - 显微镜日志版
        """
        logger.info("=" * 60)
        logger.info("✅【历史费率】 开始获取币安历史资金费率结算数据")
        logger.info(f"【历史费率】时间: {datetime.now().isoformat()}")
        logger.info(f"【历史费率】 最大重试: {max_retries}")
        logger.info("=" * 60)
        
        result = {
            "success": False,
            "error": None,
            "contract_count": 0,
            "filtered_count": 0,
            "weight_used": 0,
            "timestamp": datetime.now().isoformat()
        }
        
        for attempt in range(max_retries):
            logger.info("-" * 50)
            logger.info(f"📡【历史费率】第 {attempt + 1}/{max_retries} 次尝试")
            logger.info("-" * 50)
            
            try:
                # Step 1: 准备参数
                logger.info("【历史费率】Step 1: 准备请求参数")
                params = {"limit": 1000}
                logger.info(f"   参数: {params}")
                
                # Step 2: 创建Session
                logger.info("【历史费率】Step 2: 创建aiohttp Session")
                session_timeout = aiohttp.ClientTimeout(total=30)
                logger.info(f"【历史费率】 超时设置: {session_timeout.total}秒")
                
                async with aiohttp.ClientSession(timeout=session_timeout) as session:
                    logger.info("✅【历史费率】 Session创建成功")
                    
                    # Step 3: 发送请求
                    logger.info("【历史费率】Step 3: 发送HTTP请求")
                    logger.info(f"【历史费率】URL: {self.BINANCE_FUNDING_RATE_URL}")
                    logger.info(f" 【历史费率】方法: GET")
                    
                    async with session.get(
                        self.BINANCE_FUNDING_RATE_URL,
                        params=params
                    ) as response:
                        
                        # Step 4: 检查响应状态
                        logger.info(f"【历史费率】Step 4: 收到HTTP响应")
                        logger.info(f"【历史费率】   状态码: {response.status}")
                        logger.info(f"【历史费率】   响应头: {dict(response.headers)}")
                        
                        # 检查状态码
                        if response.status != 200:
                            error_text = await response.text()
                            logger.error(f"❌【历史费率】 HTTP错误！状态码: {response.status}")
                            logger.error(f"   ❌【历史费率】错误内容: {error_text[:200]}")
                            
                            # 处理 418 状态码（IP被封禁）
                            if response.status == 418:
                                logger.error("💥❌【历史费率】 IP被封禁！币安API限制")
                                logger.error("️⚠️⚠️【历史费率】 建议：等待封禁解除（通常几小时）")
                                result["error"] = "⚠️【历史费率】IP被封禁，请稍后重试"
                                return result  # ✅ 直接返回，不重试
                            elif response.status == 429:
                                logger.error(" ⚠️【历史费率】  原因: API权重超限")
                            elif response.status == 403:
                                logger.error(" ❌【历史费率】  原因: IP被封禁")
                            else:
                                logger.error(f" ❌【历史费率】原因: 未知HTTP错误")
                            
                            result["error"] = f"HTTP {response.status}: {error_text[:100]}"
                            continue  # 重试
                        
                        # Step 5: 解析JSON
                        logger.info("【历史费率】Step 5: 解析JSON响应")
                        try:
                            data = await response.json()
                            logger.info(f"✅ 【历史费率】JSON解析成功，数据类型: {type(data)}")
                            logger.info(f"【历史费率】   数据长度: {len(data)}")
                            
                            if isinstance(data, list) and len(data) == 0:
                                logger.warning("⚠️【历史费率】  API返回空列表！")
                                result["error"] = "⚠️【历史费率】API返回空数据"
                                continue
                            
                            if isinstance(data, dict) and data.get('code'):
                                logger.error(f"❌ A【历史费率】PI返回错误码: {data.get('code')}")
                                logger.error(f"❌【历史费率】错误信息: {data.get('msg')}")
                                result["error"] = f"❌【历史费率】API错误: {data.get('msg')}"
                                continue
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"💥 【历史费率】JSON解析失败！")
                            logger.error(f"   ❌【历史费率】错误: {e}")
                            logger.error(f"   🤔【历史费率】原始响应: {await response.text()[:200]}")
                            result["error"] = "❌【历史费率】JSON解析失败"
                            continue
                        
                        # Step 6: 过滤合约
                        logger.info("🔂【历史费率】Step 6: 过滤USDT永续合约")
                        logger.info(f"   📝【历史费率】原始合约数: {len(data)}")
                        
                        filtered_data = self._filter_usdt_perpetual(data)
                        logger.info(f"✅【历史费率】 过滤完成，USDT合约数: {len(filtered_data)}")
                        
                        if len(filtered_data) == 0:
                            logger.warning("⚠️ 【历史费率】 过滤后没有USDT合约！")
                            logger.warning("   ⚠️【历史费率】检查过滤规则是否正确")
                            result["error"] = "⚠️【历史费率】没有符合条件的USDT合约"
                            continue
                        
                        # Step 7: 推送到data_store
                        logger.info("🔂【历史费率】Step 7: 推送到共享数据模块")
                        await self._push_to_data_store(filtered_data)
                        logger.info("✅【历史费率】 推送成功！")
                        
                        # 成功返回
                        result["success"] = True
                        result["contract_count"] = len(data)
                        result["filtered_count"] = len(filtered_data)
                        result["weight_used"] = self.API_WEIGHT_PER_REQUEST
                        result["contracts"] = list(filtered_data.keys())
                        
                        logger.info("=" * 60)
                        logger.info("🎉【历史费率】 币安历史费率数据获取成功！")
                        logger.info(f"【历史费率】   总合约: {len(data)}")
                        logger.info(f" 【历史费率】USDT合约: {len(filtered_data)}")
                        logger.info(f" 【历史费率】  权重消耗: {self.API_WEIGHT_PER_REQUEST}")
                        logger.info(f"【历史费率】   示例合约: {list(filtered_data.keys())[:3]}")
                        logger.info("=" * 60)
                        
                        # 更新状态
                        self.last_fetch_time = time.time()
                        self.is_auto_fetched = True
                        
                        return result
                
            except aiohttp.ClientError as e:
                logger.error(f"💥❌【历史费率】 网络连接失败！")
                logger.error(f"   ⚠️【历史费率】异常类型: {type(e).__name__}")
                logger.error(f"   ⚠️【历史费率】异常信息: {str(e)}")
                logger.error("   ❌【历史费率】可能原因: 1. 网络不通 2. DNS解析失败 3. 服务器IP被封")
                logger.error("   ⚠️【历史费率】诊断建议: 在服务器上执行: curl https://fapi.binance.com")
                result["error"] = f"❌【历史费率】网络错误: {type(e).__name__}"
                
            except asyncio.TimeoutError:
                logger.error(f"⏰ 【历史费率】请求超时！")
                logger.error(f"⚠️【历史费率】   超时时间: 30秒")
                logger.error("⚠️【历史费率】   可能原因: 服务器到币安网络太慢")
                result["error"] = "⚠️【历史费率】请求超时"
                
            except Exception as e:
                logger.error(f"💥⚠️【历史费率】 未预料的异常！")
                logger.error(f"⚠️【历史费率】   异常类型: {type(e).__name__}")
                logger.error(f"⚠️【历史费率】  异常信息: {str(e)}")
                logger.error("📝【历史费率】 调用栈:")
                logger.error(traceback.format_exc())
                result["error"] = f"⚠️【历史费率】未知错误: {type(e).__name__}"
            
            # 如果不是最后一次，等待后重试
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)
                logger.info(f"⏳ 【历史费率】等待 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)
            else:
                logger.error("=" * 60)
                logger.error("💥❌【历史费率】 所有重试次数已用完，最终失败！")
                logger.error("=" * 60)
        
        return result
    
    def _filter_usdt_perpetual(self, api_response: List[Dict]) -> Dict[str, Dict]:
        """
        过滤USDT永续合约
        """
        filtered = {}
        
        logger.info(" ✅【历史费率】开始过滤...")
        logger.info(f"【历史费率】过滤规则:")
        logger.info(f"【历史费率】1. 以USDT结尾")
        logger.info(f"【历史费率】2. 不以1000开头")
        logger.info(f"【历史费率】3. 不包含':'")
        
        for item in api_response:
            symbol = item.get('symbol', '')
            
            # 检查每个条件并记录
            checks = {
                "endswith(USDT)": symbol.endswith('USDT'),
                "not startswith(1000)": not symbol.startswith('1000'),
                "no ':'": ':' not in symbol
            }
            
            if all(checks.values()):
                processed = {
                    "symbol": symbol,
                    "funding_rate": float(item.get('fundingRate', 0)),
                    "funding_time": item.get('fundingTime'),
                    "next_funding_time": item.get('nextFundingTime'),
                    "raw_data": item
                }
                filtered[symbol] = processed
            else:
                logger.debug(f"⚠️【历史费率】 过滤掉: {symbol} (原因: {checks})")
        
        logger.info(f"✅【历史费率】过滤结果: 保留 {len(filtered)} 个")
        return dict(sorted(filtered.items()))
    
    async def _push_to_data_store(self, filtered_data: Dict[str, Dict]):
        """
        ✅ 推送到共享数据模块：统一存储到market_data（正规化改造）
        """
        try:
            logger.info("🔂【历史费率】清空旧数据...")
            logger.info("   🔂【历史费率】推送新数据...")
            
            for symbol, data in filtered_data.items():
                await data_store.update_market_data(
                    exchange="binance",
                    symbol=symbol,
                    data={
                        "data_type": "funding_settlement",
                        **data
                    }
                )
            
            logger.info(f"【历史费率】 ✅ 推送完成: {len(filtered_data)} 个合约")
        except Exception as e:
            logger.error(f"【历史费率】❌ 推送失败: {e}")
            raise
    
    def can_manually_fetch(self) -> tuple[bool, Optional[str]]:
        """
        检查是否可以手动触发获取
        """
        current_hour = datetime.now().hour
        
        if self.last_manual_fetch_hour != current_hour:
            self.manual_fetch_count = 0
            self.last_manual_fetch_hour = current_hour
        
        if self.manual_fetch_count >= 3:
            return False, f"⚠️【历史费率】1小时内最多获取3次（已使用: {self.manual_fetch_count}/3）"
        
        return True, None
    
    async def manual_fetch(self) -> Dict[str, Any]:
        """
        手动触发获取
        """
        logger.info("=" * 60)
        logger.info("🖱️【历史费率】  收到手动触发请求")
        logger.info("=" * 60)
        
        can_fetch, reason = self.can_manually_fetch()
        
        if not can_fetch:
            logger.warning(f"❌ 【历史费率】被拒绝: {reason}")
            return {
                "success": False,
                "error": reason,
                "timestamp": datetime.now().isoformat()
            }
        
        self.manual_fetch_count += 1
        
        result = await self.fetch_funding_settlement()
        result['triggered_by'] = 'manual'
        result['manual_fetch_count'] = f"{self.manual_fetch_count}/3"
        
        return result
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取模块状态
        """
        current_hour = datetime.now().hour
        
        if self.last_manual_fetch_hour != current_hour:
            manual_count_str = "0/3"
        else:
            manual_count_str = f"{self.manual_fetch_count}/3"
        
        return {
            "last_fetch_time": datetime.fromtimestamp(self.last_fetch_time).isoformat() if self.last_fetch_time else None,
            "is_auto_fetched": self.is_auto_fetched,
            "manual_fetch_count": manual_count_str,
            "api_weight_per_request": self.API_WEIGHT_PER_REQUEST
        }
