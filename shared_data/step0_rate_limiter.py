"""
第一步之前：币安历史费率数据限流器
功能：只针对 binance_funding_settlement 数据类型，限制其进入后续流水线的次数
"""

import logging
from typing import Dict, List, Any
from collections import defaultdict

logger = logging.getLogger(__name__)

class Step0RateLimiter:
    """
    步骤0：币安历史费率数据限流器
    
    职责：
    1. 只过滤 binance_funding_settlement 数据类型
    2. 限制该数据类型最多通过10次
    3. 其他4种数据类型永远放行
    4. 不修改任何数据内容，只决定是否放行
    
    工作逻辑：
    - 前10次放水：放行所有5种数据
    - 第11次开始：只放行4种数据（排除币安历史费率）
    - 每次判断都是针对整个数据类型，不是单个合约
    """
    
    def __init__(self, limit_times: int = 100):
        """
        初始化限流器
        
        Args:
            limit_times: 币安历史费率数据最大放行次数，默认10次
        """
        # 限流配置
        self.limit_times = limit_times
        
        # 状态记录
        self.binance_funding_passed = 0      # 已放行的币安历史费率数据次数
        self.binance_funding_blocked = False # 是否已超过限制
        self.total_passed = 0                # 总共放行的数据条数
        self.total_blocked = 0               # 总共拦截的数据条数
        
        # 统计信息
        self.stats = defaultdict(int)
        self.stats_update_interval = 60      # 统计日志输出间隔(秒)
        self.last_stats_log_time = 0
        
        logger.info(f"✅【流水线步骤0】初始化完成，币安历史费率数据限流 {limit_times} 次")
    
    def process(self, raw_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        处理原始数据流，过滤币安历史费率数据
        
        Args:
            raw_items: DataStore放出的原始数据列表
            
        Returns:
            过滤后的数据列表（币安历史费率数据可能被移除）
        """
        if not raw_items:
            return []
        
        # 统计数据
        incoming_stats = self._count_incoming_items(raw_items)
        
        # 过滤处理
        filtered_items = []
        
        for item in raw_items:
            exchange = item.get('exchange', '').lower()
            data_type = item.get('data_type', '').lower()
            
            # 判断是否是币安历史费率数据
            is_binance_funding = (exchange == 'binance' and 
                                 data_type == 'funding_settlement')
            
            if is_binance_funding:
                # 检查是否已超过限制
                if self.binance_funding_blocked:
                    self.total_blocked += 1
                    self.stats['binance_funding_blocked'] += 1
                    continue  # 不放行
                
                # 检查是否达到限制
                if self.binance_funding_passed >= self.limit_times:
                    self.binance_funding_blocked = True
                    self.total_blocked += 1
                    self.stats['binance_funding_blocked'] += 1
                    logger.info(f"🛑【流水线步骤0】币安历史费率数据已达到{self.limit_times}次限制，开始拦截")
                    continue  # 不放行
                
                # 放行并计数
                self.binance_funding_passed += 1
                self.stats['binance_funding_passed'] += 1
            
            # 放行数据（无论什么类型）
            filtered_items.append(item)
            self.total_passed += 1
        
        # 输出过滤结果统计
        self._log_processing_stats(incoming_stats, len(filtered_items))
        
        return filtered_items
    
    def _count_incoming_items(self, raw_items: List[Dict]) -> Dict[str, int]:
        """统计输入数据中各类型的数量"""
        counts = defaultdict(int)
        for item in raw_items:
            exchange = item.get('exchange', 'unknown')
            data_type = item.get('data_type', 'unknown')
            key = f"{exchange}_{data_type}"
            counts[key] += 1
        return counts
    
    def _log_processing_stats(self, incoming_counts: Dict[str, int], 
                             output_count: int) -> None:
        """记录处理统计信息"""
        import time
        
        current_time = time.time()
        
        # 定期输出详细统计
        if current_time - self.last_stats_log_time > self.stats_update_interval:
            self.last_stats_log_time = current_time
            
            # 统计输入数据
            total_input = sum(incoming_counts.values())
            
            # 统计币安历史费率数据
            binance_funding_key = 'binance_funding_settlement'
            binance_funding_input = incoming_counts.get(binance_funding_key, 0)
            
            # 构建统计信息
            stats_lines = []
            stats_lines.append(f"📊【流水线步骤0】处理统计:")
            stats_lines.append(f"  输入数据: {total_input} 条")
            stats_lines.append(f"  输出数据: {output_count} 条")
            stats_lines.append(f"  过滤数据: {total_input - output_count} 条")
            stats_lines.append(f"  币安历史费率数据:")
            stats_lines.append(f"    - 本次输入: {binance_funding_input} 条")
            stats_lines.append(f"    - 累计放行: {self.binance_funding_passed} 次")
            stats_lines.append(f"    - 限流状态: {'已拦截' if self.binance_funding_blocked else '放行中'}")
            
            # 其他数据类型统计
            other_types = [k for k in incoming_counts.keys() if k != binance_funding_key]
            if other_types:
                stats_lines.append(f"  其他数据类型:")
                for data_type in sorted(other_types):
                    count = incoming_counts[data_type]
                    stats_lines.append(f"    - {data_type}: {count} 条")
            
            logger.info("\n".join(stats_lines))
    
    def get_status(self) -> Dict[str, Any]:
        """获取步骤0的当前状态"""
        return {
            "binance_funding_limit": self.limit_times,
            "binance_funding_passed": self.binance_funding_passed,
            "binance_funding_blocked": self.binance_funding_blocked,
            "total_passed": self.total_passed,
            "total_blocked": self.total_blocked,
            "is_active": True,
            "stats": dict(self.stats)
        }
    
    def reset_limit(self) -> None:
        """重置限流器状态（用于测试或特殊情况）"""
        self.binance_funding_passed = 0
        self.binance_funding_blocked = False
        logger.warning("🔄【流水线步骤0】限流器状态已重置")
    
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