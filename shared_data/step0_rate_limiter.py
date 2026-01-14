"""
第一步之前：币安历史费率数据限流器
功能：只针对 binance_funding_settlement 数据类型，限制其进入后续流水线的次数
✅ 修复：按数据类型计数，不是按数据条数
"""

import logging
import time
from typing import Dict, List, Any
from collections import defaultdict

logger = logging.getLogger(__name__)

class Step0RateLimiter:
    """
    步骤0：币安历史费率数据限流器（严密版）
    
    职责：
    1. 只过滤 binance_funding_settlement 数据类型
    2. 限制该数据类型最多通过N次（按数据类型出现次数，不是数据条数）
    3. 其他4种数据类型永远放行
    4. 不修改任何数据内容，只决定是否放行
    
    严密逻辑：
    - 每次放水时，检查本次是否有币安历史费率数据
    - 如果没有（量=0）：直接放行所有数据，不计数
    - 如果有（量>0）：放行所有数据，计数+1
    - 累计计数达到限制次数后：拦截该类型所有数据
    - 其他4种数据类型永远不受影响
    """
    
    def __init__(self, limit_times: int = 10):
        """
        初始化限流器
        
        Args:
            limit_times: 币安历史费率数据最大放行次数，默认10次
                         ✅ 按数据类型出现次数计数，不是数据条数
        """
        # 限流配置
        self.limit_times = limit_times
        
        # 状态记录
        self.binance_funding_passed = 0      # 已放行的币安历史费率数据类型次数
        self.binance_funding_blocked = False # 是否已拦截该类型
        self.total_passed = 0                # 总共放行的数据条数
        self.total_blocked = 0               # 总共拦截的数据条数
        
        # 统计信息
        self.stats = defaultdict(int)
        self.stats_update_interval = 60      # 统计日志输出间隔(秒)
        self.last_stats_log_time = 0
        
        # 调试信息
        self.debug_mode = True  # 设置为False可关闭调试日志
        self.last_process_time = 0
        
        logger.info(f"✅【流水线步骤0】初始化完成，币安历史费率数据限流 {limit_times} 次（按数据类型计数）")
    
    def process(self, raw_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        处理原始数据流，过滤币安历史费率数据
        
        ✅ 严密逻辑：
        1. 先检查本次是否有币安历史费率数据
        2. 如果没有（量=0）：直接放行，不计数
        3. 如果有（量>0）：放行，计数+1（只计1次！）
        4. 达到限制后：拦截该类型所有数据
        
        Args:
            raw_items: DataStore放出的原始数据列表
            
        Returns:
            过滤后的数据列表（币安历史费率数据可能被移除）
        """
        # 调试信息
        if self.debug_mode:
            current_time = time.time()
            time_since_last = current_time - self.last_process_time if self.last_process_time > 0 else 0
            self.last_process_time = current_time
            
            logger.debug(f"🔍【Step0调试】收到 {len(raw_items)} 条数据，距离上次: {time_since_last:.3f}秒")
        
        # 如果收到空数据，直接返回
        if not raw_items:
            if self.debug_mode:
                logger.debug("🔍【Step0调试】收到空数据，直接返回")
            return []
        
        # ✅ 步骤1：检查本次是否有币安历史费率数据
        has_binance_funding = False
        binance_funding_items = []
        binance_funding_symbols = set()
        
        for item in raw_items:
            exchange = item.get('exchange', '')
            data_type = item.get('data_type', '')
            
            # ✅ 严密匹配：忽略大小写，去除空格
            is_binance_funding = (
                str(exchange).strip().lower() == 'binance' and 
                str(data_type).strip().lower() == 'funding_settlement'
            )
            
            if is_binance_funding:
                has_binance_funding = True
                binance_funding_items.append(item)
                symbol = item.get('symbol', 'unknown')
                binance_funding_symbols.add(symbol)
        
        # 调试信息：显示检查结果
        if self.debug_mode and has_binance_funding:
            logger.debug(f"🔍【Step0调试】发现币安历史费率数据: {len(binance_funding_items)}条")
            logger.debug(f"🔍【Step0调试】涉及合约: {list(binance_funding_symbols)[:5]}{'...' if len(binance_funding_symbols) > 5 else ''}")
        
        # ✅ 步骤2：如果没有币安历史费率数据，直接放行（不计数！）
        if not has_binance_funding:
            self.total_passed += len(raw_items)
            
            if self.debug_mode and len(raw_items) > 0:
                # 显示本次有哪些数据类型（仅调试）
                type_counter = defaultdict(int)
                for item in raw_items:
                    key = f"{item.get('exchange', 'unknown')}_{item.get('data_type', 'unknown')}"
                    type_counter[key] += 1
                
                type_info = ', '.join([f"{k}:{v}" for k, v in type_counter.items()])
                logger.debug(f"🔍【Step0调试】无币安历史费率，直接放行。数据类型: {type_info}")
            
            return raw_items
        
        # ✅ 步骤3：有币安历史费率数据，进行限流判断
        
        # 情况A：该类型已被拦截
        if self.binance_funding_blocked:
            # 拦截所有币安历史费率数据，放行其他数据
            filtered_items = [
                item for item in raw_items 
                if not (
                    str(item.get('exchange', '')).strip().lower() == 'binance' and 
                    str(item.get('data_type', '')).strip().lower() == 'funding_settlement'
                )
            ]
            
            blocked_count = len(raw_items) - len(filtered_items)
            self.total_blocked += blocked_count
            self.total_passed += len(filtered_items)
            self.stats['binance_funding_blocked'] += blocked_count
            
            if self.debug_mode:
                logger.debug(f"🔍【Step0调试】已拦截，过滤掉 {blocked_count} 条币安历史费率数据")
                logger.debug(f"🔍【Step0调试】放行 {len(filtered_items)} 条其他数据")
            
            return filtered_items
        
        # 情况B：已达到限制次数，需要开始拦截
        if self.binance_funding_passed >= self.limit_times:
            self.binance_funding_blocked = True
            
            # 拦截所有币安历史费率数据
            filtered_items = [
                item for item in raw_items 
                if not (
                    str(item.get('exchange', '')).strip().lower() == 'binance' and 
                    str(item.get('data_type', '')).strip().lower() == 'funding_settlement'
                )
            ]
            
            blocked_count = len(raw_items) - len(filtered_items)
            self.total_blocked += blocked_count
            self.total_passed += len(filtered_items)
            self.stats['binance_funding_blocked'] += blocked_count
            
            logger.warning(f"🛑【流水线步骤0】币安历史费率数据已达到{self.limit_times}次限制，开始拦截")
            
            if self.debug_mode:
                logger.debug(f"🔍【Step0调试】达到限制，拦截 {blocked_count} 条币安历史费率数据")
                logger.debug(f"🔍【Step0调试】累计放行: {self.binance_funding_passed} 次")
            
            return filtered_items
        
        # ✅ 情况C：正常放行，计数+1（只计1次！）
        self.binance_funding_passed += 1
        self.stats['binance_funding_passed'] += 1
        self.total_passed += len(raw_items)
        
        # 输出本次放行信息
        if self.debug_mode:
            logger.debug(f"✅【Step0调试】放行币安历史费率数据，累计 {self.binance_funding_passed}/{self.limit_times} 次")
            logger.debug(f"🔍【Step0调试】本次数据量: {len(binance_funding_items)} 条，涉及 {len(binance_funding_symbols)} 个合约")
        
        # 定期输出统计信息
        self._log_processing_stats(len(binance_funding_items), len(raw_items))
        
        return raw_items
    
    def _log_processing_stats(self, binance_funding_count: int, total_input_count: int) -> None:
        """记录处理统计信息"""
        current_time = time.time()
        
        # 定期输出详细统计
        if current_time - self.last_stats_log_time > self.stats_update_interval:
            self.last_stats_log_time = current_time
            
            stats_lines = []
            stats_lines.append(f"📊【流水线步骤0】统计报告:")
            stats_lines.append(f" 【流水线步骤0】 累计放行次数: {self.binance_funding_passed}/{self.limit_times}")
            stats_lines.append(f" 【流水线步骤0】 拦截状态: {'已拦截' if self.binance_funding_blocked else '放行中'}")
            stats_lines.append(f"  【流水线步骤0】总放行数据条数: {self.total_passed}")
            stats_lines.append(f"  【流水线步骤0】总拦截数据条数: {self.total_blocked}")
            
            logger.info("\n".join(stats_lines))
    
    def get_status(self) -> Dict[str, Any]:
        """获取步骤0的当前状态"""
        return {
            "binance_funding_limit": self.limit_times,
            "binance_funding_passed": self.binance_funding_passed,
            "binance_funding_blocked": self.binance_funding_blocked,
            "total_passed": self.total_passed,
            "total_blocked": self.total_blocked,
            "remaining_passes": max(0, self.limit_times - self.binance_funding_passed),
            "is_active": True,
            "stats": dict(self.stats)
        }
    
    def reset_limit(self) -> None:
        """重置限流器状态（用于测试或特殊情况）"""
        old_passed = self.binance_funding_passed
        old_blocked = self.binance_funding_blocked
        
        self.binance_funding_passed = 0
        self.binance_funding_blocked = False
        
        logger.warning(f"🔄【流水线步骤0】限流器状态已重置（原:通过{old_passed}次, 拦截:{old_blocked}）")
    
    def update_limit(self, new_limit: int) -> None:
        """更新限制次数"""
        old_limit = self.limit_times
        self.limit_times = new_limit
        
        # 如果新限制比已通过的次数大，解除拦截状态
        if new_limit > self.binance_funding_passed and self.binance_funding_blocked:
            self.binance_funding_blocked = False
            logger.info(f"🔄【流水线步骤0】限制从{old_limit}次调整为{new_limit}次，解除拦截状态")
        else:
            logger.info(f"📝【流水线步骤0】限制从{old_limit}次调整为{new_limit}次")
        
        # 如果新限制小于已通过次数，立即拦截
        if new_limit <= self.binance_funding_passed and not self.binance_funding_blocked:
            self.binance_funding_blocked = True
            logger.warning(f"🛑【流水线步骤0】新限制{new_limit}小于已通过{self.binance_funding_passed}，立即拦截")
    
    def enable_debug(self, enabled: bool = True):
        """启用或禁用调试模式"""
        self.debug_mode = enabled
        status = "启用" if enabled else "禁用"
        logger.info(f"🔧【流水线步骤0】调试模式{status}")