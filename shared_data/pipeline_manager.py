#!/usr/bin/env python3
"""
PipelineManager 完整版
功能：1. 控制流向 2. 管理流水线 3. 推送成品数据
核心：历史费率按合约控制，每个合约只流出1次
优化：初始化只调用一次，日志优化
"""

import asyncio
from typing import Dict, Any, Optional, Callable
import logging
import time

# 5个步骤
from shared_data.step1_filter import Step1Filter
from shared_data.step2_fusion import Step2Fusion
from shared_data.step3_align import Step3Align
from shared_data.step4_calc import Step4Calc
from shared_data.step5_cross_calc import Step5CrossCalc, CrossPlatformData

logger = logging.getLogger(__name__)

def log_data_process(module: str, action: str, message: str, level: str = "INFO"):
    """统一的数据处理日志格式"""
    prefix = f"[数据处理][{module}][{action}]"
    full_message = f"{prefix} {message}"
    
    if level == "INFO":
        logger.info(full_message)
    elif level == "ERROR":
        logger.error(full_message)
    elif level == "WARNING":
        logger.warning(full_message)
    elif level == "DEBUG":
        logger.debug(full_message)

class FlowInstructions:
    """流向指令集 - 按合约控制历史费率"""
    
    # 控制开关
    MARKET_FLOW_ENABLED = True
    ACCOUNT_FLOW_ENABLED = True
    ORDER_FLOW_ENABLED = True
    
    # ✅ 按合约控制历史费率流出（每个合约只流出1次）
    _history_flowed_contracts = set()  # 已流出历史费率的合约集合
    
    # 系统状态
    _system_initialized = False
    _pipeline_started = False
    
    @classmethod
    def initialize_system(cls):
        """系统初始化 - 只调用一次"""
        if not cls._system_initialized:
            log_data_process("系统启动", "初始化", "开始初始化数据处理系统...")
            log_data_process("管理员指令", "设置", f"市场数据流出: {'启用' if cls.MARKET_FLOW_ENABLED else '禁用'}")
            log_data_process("管理员指令", "设置", f"账户数据流出: {'启用' if cls.ACCOUNT_FLOW_ENABLED else '禁用'}")
            log_data_process("管理员指令", "设置", f"订单数据流出: {'启用' if cls.ORDER_FLOW_ENABLED else '禁用'}")
            log_data_process("管理员指令", "状态", f"历史费率控制: 按合约控制，每个合约只流出1次")
            log_data_process("管理员指令", "状态", f"已流出合约数: {len(cls._history_flowed_contracts)}")
            
            cls._system_initialized = True
            log_data_process("系统启动", "完成", "数据处理系统初始化完成")
    
    @classmethod
    def start_pipeline(cls):
        """启动流水线"""
        if not cls._pipeline_started:
            log_data_process("流水线启动", "步骤1", "数据过滤步骤启动成功")
            log_data_process("流水线启动", "步骤2", "数据融合步骤启动成功")
            log_data_process("流水线启动", "步骤3", "跨平台对齐步骤启动成功")
            log_data_process("流水线启动", "步骤4", "单平台计算步骤启动成功")
            log_data_process("流水线启动", "步骤5", "跨平台计算步骤启动成功")
            log_data_process("流水线启动", "完成", "5步流水线全部启动成功")
            cls._pipeline_started = True
    
    @classmethod
    def should_flow_market_data(cls, exchange: str, symbol: str, data_type: str, data: dict) -> bool:
        """市场数据是否应该流出 - 按合约控制历史费率"""
        # 检查总开关
        if not cls.MARKET_FLOW_ENABLED:
            log_data_process("管理员指令", "阻止", f"市场数据流出已禁用，阻止 {exchange}.{symbol}.{data_type}", "DEBUG")
            return False
        
        # ✅ 核心：历史费率控制（每个合约只流出1次）
        if data_type == "funding_settlement":
            # 检查这个合约的历史费率是否已流出过
            if symbol in cls._history_flowed_contracts:
                funding_time = data.get('funding_time', 'unknown')
                log_data_process("管理员指令", "历史", 
                               f"{exchange}.{symbol} 的历史费率已流出过，阻止（结算时间: {funding_time}）", 
                               "DEBUG")
                return False  # ❌ 这个合约已流出过
            
            # ✅ 首次流出这个合约的历史费率
            cls._history_flowed_contracts.add(symbol)
            funding_time = data.get('funding_time', 'unknown')
            log_data_process("管理员指令", "历史", 
                           f"{exchange}.{symbol} 的历史费率首次流出（结算时间: {funding_time}）")
            return True  # ✅ 首次流出
        
        # ✅ 实时数据正常流出（只记录重要数据）
        if data_type in ["funding_rate", "mark_price", "ticker"]:
            log_data_process("数据流动", "原始数据", f"{exchange}.{symbol}.{data_type}", "DEBUG")
        return True
    
    @classmethod
    def should_flow_account_data(cls, exchange: str, data_type: str, data: dict) -> bool:
        """账户数据是否应该流出"""
        if not cls.ACCOUNT_FLOW_ENABLED:
            log_data_process("管理员指令", "阻止", f"账户数据流出已禁用，阻止 {exchange}.{data_type}", "DEBUG")
            return False
        
        log_data_process("数据流动", "账户数据", f"{exchange}.{data_type}", "DEBUG")
        return True
    
    @classmethod
    def should_flow_order_data(cls, exchange: str, order_id: str, data: dict) -> bool:
        """订单数据是否应该流出"""
        if not cls.ORDER_FLOW_ENABLED:
            log_data_process("管理员指令", "阻止", f"订单数据流出已禁用，阻止 {exchange}.{order_id}", "DEBUG")
            return False
        
        log_data_process("数据流动", "订单数据", f"{exchange}.{order_id}", "DEBUG")
        return True
    
    @classmethod
    def get_flow_destination(cls, data_type: str) -> str:
        """获取流向目标"""
        # 市场数据 → 流水线
        if data_type.startswith(("ticker", "funding_rate", "mark_price", "funding_settlement")):
            return "pipeline"
        # 账户/订单数据 → 直接大脑
        elif data_type.startswith(("account", "position", "order", "trade")):
            return "brain"
        # 默认 → 流水线
        return "pipeline"
    
    @classmethod
    def get_status(cls) -> Dict[str, Any]:
        """获取指令状态"""
        return {
            "system_initialized": cls._system_initialized,
            "pipeline_started": cls._pipeline_started,
            "market_flow_enabled": cls.MARKET_FLOW_ENABLED,
            "account_flow_enabled": cls.ACCOUNT_FLOW_ENABLED,
            "order_flow_enabled": cls.ORDER_FLOW_ENABLED,
            "history_control": "按合约控制（每个合约1次）",
            "history_flowed_contracts_count": len(cls._history_flowed_contracts),
            "history_flowed_contracts": list(sorted(cls._history_flowed_contracts))[:10]
        }

class PipelineManager:
    """流水线管理器 - 只处理逻辑，不统计条数"""
    
    _instance: Optional['PipelineManager'] = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def instance(cls) -> 'PipelineManager':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self, brain_callback: Optional[Callable] = None):
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self.brain_callback = brain_callback
        
        # 5个步骤
        self.step1 = Step1Filter()
        self.step2 = Step2Fusion()
        self.step3 = Step3Align()
        self.step4 = Step4Calc()
        self.step5 = Step5CrossCalc()
        
        # 处理锁
        self.processing_lock = asyncio.Lock()
        self.lock_timeout = 30.0
        
        # 异步推送
        self._async_push_enabled = True
        self._active_push_tasks = set()
        
        # ✅ 状态记录（按合约）
        self.processed_symbols = set()  # 已处理的合约
        
        self.running = False
        
        # ✅ 初始化系统（只调用一次）
        FlowInstructions.initialize_system()
        
        log_data_process("流水线", "启动", "PipelineManager初始化完成")
        self._initialized = True
    
    async def start(self):
        """启动流水线"""
        if self.running:
            return
        
        self.running = True
        FlowInstructions.start_pipeline()
        log_data_process("流水线", "启动", "流水线开始运行...")
    
    async def stop(self):
        """停止流水线"""
        if not self.running:
            return
        
        log_data_process("流水线", "停止", "正在停止...")
        self.running = False
        
        if self._active_push_tasks:
            await asyncio.gather(*self._active_push_tasks, return_exceptions=True)
        
        await asyncio.sleep(0.5)
        log_data_process("流水线", "停止", "已停止")
    
    async def ingest_data(self, data: Dict[str, Any]) -> bool:
        """流水线处理入口"""
        try:
            symbol = data.get('symbol', 'unknown')
            
            # 快速分类
            data_type = data.get("data_type", "")
            if data_type.startswith(("ticker", "funding_rate", "mark_price")):
                await self._process_market_data(data)
            elif data_type.startswith(("account", "position", "order", "trade")):
                await self._process_account_data(data)
            
            # ✅ 记录处理的合约
            self.processed_symbols.add(symbol)
            
            return True
            
        except Exception as e:
            symbol = data.get('symbol', 'N/A')
            log_data_process("流水线", "错误", f"处理失败: {symbol} - {e}", "ERROR")
            return False
    
    async def _process_market_data(self, data: Dict[str, Any]):
        """市场数据处理：5步流水线"""
        # Step1: 提取
        step1_results = self.step1.process([data])
        if not step1_results:
            return
        
        # Step2: 融合
        step2_results = self.step2.process(step1_results)
        if not step2_results:
            return
        
        # Step3: 对齐
        step3_results = self.step3.process(step2_results)
        if not step3_results:
            return
        
        # Step4: 计算
        step4_results = self.step4.process(step3_results)
        if not step4_results:
            return
        
        # Step5: 跨平台计算（成品数据）
        final_results = self.step5.process(step4_results)
        if not final_results:
            return
        
        # 成品数据推送给大脑
        if self.brain_callback:
            for result in final_results:
                if isinstance(result, CrossPlatformData):
                    result_dict = self._crossplatform_to_dict(result)
                    
                    if self._async_push_enabled:
                        self._push_async(result_dict)
                    else:
                        try:
                            await self.brain_callback(result_dict)
                        except Exception as e:
                            log_data_process("流水线", "错误", f"成品数据推送失败: {e}", "ERROR")
                    
                    symbol = result.symbol
                    log_data_process("流水线", "成品", f"{symbol} 套利数据生成完成")
    
    def _crossplatform_to_dict(self, data: CrossPlatformData) -> Dict[str, Any]:
        """CrossPlatformData转换为字典"""
        return {
            "symbol": data.symbol,
            "price_diff": data.price_diff,
            "price_diff_percent": data.price_diff_percent,
            "rate_diff": data.rate_diff,
            "okx_price": data.okx_price,
            "okx_funding_rate": data.okx_funding_rate,
            "binance_price": data.binance_price,
            "binance_funding_rate": data.binance_funding_rate,
            "okx_period_seconds": data.okx_period_seconds,
            "okx_countdown_seconds": data.okx_countdown_seconds,
            "okx_last_settlement": data.okx_last_settlement,
            "okx_current_settlement": data.okx_current_settlement,
            "okx_next_settlement": data.okx_next_settlement,
            "binance_period_seconds": data.binance_period_seconds,
            "binance_countdown_seconds": data.binance_countdown_seconds,
            "binance_last_settlement": data.binance_last_settlement,
            "binance_current_settlement": data.binance_current_settlement,
            "binance_next_settlement": data.binance_next_settlement,
            "metadata": data.metadata,
            "source": "pipeline_final_result",
            "timestamp": time.time()
        }
    
    async def _process_account_data(self, data: Dict[str, Any]):
        """账户数据处理 → 直接推送给大脑"""
        if self.brain_callback:
            account_dict = {
                **data,
                "source": "data_store_account_direct",
                "timestamp": time.time()
            }
            
            if self._async_push_enabled:
                self._push_async_account(account_dict)
            else:
                try:
                    await self.brain_callback(account_dict)
                except Exception as e:
                    log_data_process("流水线", "错误", f"账户数据推送失败: {e}", "ERROR")
    
    def _push_async(self, result_dict: Dict[str, Any]):
        """异步推送成品数据"""
        if not self.brain_callback:
            return
        
        async def safe_push():
            try:
                await self.brain_callback(result_dict)
            except Exception as e:
                log_data_process("流水线", "错误", f"异步推送失败: {e}", "ERROR")
            finally:
                self._active_push_tasks.discard(task)
        
        task = asyncio.create_task(safe_push())
        self._active_push_tasks.add(task)
    
    def _push_async_account(self, account_dict: Dict[str, Any]):
        """异步推送账户数据"""
        if not self.brain_callback:
            return
        
        async def safe_push_account():
            try:
                await self.brain_callback(account_dict)
            except Exception as e:
                log_data_process("流水线", "错误", f"异步账户推送失败: {e}", "ERROR")
            finally:
                self._active_push_tasks.discard(task)
        
        task = asyncio.create_task(safe_push_account())
        self._active_push_tasks.add(task)
    
    def get_status(self) -> Dict[str, Any]:
        """获取状态信息"""
        return {
            "running": self.running,
            "processed_contracts": len(self.processed_symbols),
            "active_tasks": len(self._active_push_tasks),
            "instructions_status": FlowInstructions.get_status()
        }