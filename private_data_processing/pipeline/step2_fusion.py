"""
第二步：融合更新
职责：
1. 预先创建空容器并缓存（binance/okx）
2. 收到数据直接更新对应容器
3. 返回副本给调度器
4. 平仓时清空交易字段
"""
import time
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


# 成品数据模板
TRADE_TEMPLATE = {
    "交易所": None,
    "账户资产额": None,
    "资产币种": None,
    "保证金模式": None,
    "保证金币种": None,
    "开仓合约名": None,
    "开仓方向": None,
    "开仓执行方式": None,
    "开仓价": None,
    "持仓币数": None,
    "持仓张数": None,
    "合约面值": None,
    "开仓价仓位价值": None,
    "杠杆": None,
    "开仓保证金": None,
    "开仓手续费": None,
    "开仓手续费币种": None,
    "开仓时间": None,
    "标记价": None,
    "标记价涨跌幅": None,
    "标记价保证金": None,
    "标记价仓位价值": None,
    "标记价浮盈": None,
    "标记价浮盈百分比": None,
    "最新价": None,
    "最新价涨跌幅": None,
    "最新价保证金": None,
    "最新价仓位价值": None,
    "最新价浮盈": None,
    "最新价浮盈百分比": None,
    "止损触发方式": None,
    "止损触发价": None,
    "止损幅度": None,
    "止盈触发方式": None,
    "止盈触发价": None,
    "止盈幅度": None,
    "本次资金费": 0,
    "累计资金费": 0,
    "资金费结算次数": 0,
    "平均资金费率": None,
    "本次资金费结算时间": None,
    "平仓执行方式": None,
    "平仓价": None,
    "平仓价涨跌幅": None,
    "平仓价仓位价值": None,
    "平仓手续费": None,
    "平仓手续费币种": None,
    "平仓收益": None,
    "平仓收益率": None,
    "平仓时间": None,
}


class Step2Fusion:
    """第二步：融合更新"""
    
    def __init__(self):
        # 预先创建容器缓存
        self.containers = {
            "binance": TRADE_TEMPLATE.copy(),
            "okx": TRADE_TEMPLATE.copy(),
        }
        self.containers["binance"]["交易所"] = "binance"
        self.containers["okx"]["交易所"] = "okx"
        
        logger.info("✅【step2】容器缓存已创建: binance, okx")
    
    def process(self, extracted_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理提取后的数据
        
        Args:
            extracted_data: step1提取的字段
            
        Returns:
            更新后的成品数据副本，None表示无效
        """
        exchange = extracted_data.get('交易所')
        if not exchange or exchange not in self.containers:
            return None
        
        # 获取原始容器
        container = self.containers[exchange]
        
        # 检查平仓事件
        event_type = extracted_data.get('event_type', '')
        if event_type in ["_06_触发止损", "_08_触发止盈", "_10_主动平仓"]:
            self._clear_trade_data(container)
        
        # 覆盖式更新原始容器
        for key, value in extracted_data.items():
            if key in container and value is not None:
                container[key] = value
        
        # 返回副本给调度器
        return container.copy()
    
    def _clear_trade_data(self, container: Dict):
        """清空交易相关字段"""
        trade_fields = [
            "开仓合约名", "开仓方向", "开仓执行方式", "开仓价", "持仓币数",
            "持仓张数", "合约面值", "开仓价仓位价值", "杠杆", "开仓保证金",
            "开仓手续费", "开仓手续费币种", "开仓时间",
            "标记价", "标记价涨跌幅", "标记价保证金", "标记价仓位价值",
            "标记价浮盈", "标记价浮盈百分比",
            "最新价", "最新价涨跌幅", "最新价保证金", "最新价仓位价值",
            "最新价浮盈", "最新价浮盈百分比",
            "止损触发方式", "止损触发价", "止损幅度",
            "止盈触发方式", "止盈触发价", "止盈幅度",
            "本次资金费", "累计资金费", "资金费结算次数", "平均资金费率", "本次资金费结算时间",
            "平仓执行方式", "平仓价", "平仓价涨跌幅", "平仓价仓位价值",
            "平仓手续费", "平仓手续费币种", "平仓收益", "平仓收益率", "平仓时间"
        ]
        
        for field in trade_fields:
            if field in container:
                if field in ["本次资金费", "累计资金费", "资金费结算次数"]:
                    container[field] = 0
                else:
                    container[field] = None
        
        logger.info(f"🧹【{container['交易所']}】平仓清空交易数据")
    
    def get_container(self, exchange: str) -> Optional[Dict]:
        """获取指定交易所的容器副本（调试用）"""
        if exchange in self.containers:
            return self.containers[exchange].copy()
        return None
        