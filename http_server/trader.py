# http_server/trader.py
"""
下单执行器（工人）

================================================================================
职责
================================================================================
只做一件事：把大脑给的参数原样发给交易所

- 不保存任何状态
- 不包含任何业务逻辑
- 不知道什么是开仓、平仓、方向、数量
- 大脑给什么参数，就发什么参数

================================================================================
密钥读取
================================================================================
每次发单时，通过 self.brain.data_manager.get_api_credentials() 按需读取
不保存密钥，用完即弃

================================================================================
同步/异步处理
================================================================================
CCXT 是同步库，会阻塞事件循环
使用 asyncio.get_running_loop().run_in_executor() 放到线程池中运行

================================================================================
模拟/真实交易切换
================================================================================
当前使用模拟交易接口（testnet / sandbox）
真实交易接口已注释，调试完成后取消注释即可切换
================================================================================
"""

import asyncio
import logging
import ccxt
from typing import Dict, Any, Optional

# 获取日志记录器
logger = logging.getLogger(__name__)


class Trader:
    """
    下单执行器（工人）
    
    设计原则：
        1. 只转发，不解释 - 大脑给什么参数就发什么参数
        2. 按需读取密钥 - 每次发单时从大脑存储区读取，用完不保存
        3. 不包含业务逻辑 - 不知道什么是开仓、平仓、方向
        4. 线程池运行 - CCXT 是同步库，放到线程池避免阻塞事件循环
    """
    
    def __init__(self, brain):
        """
        初始化工人
        
        Args:
            brain: 大脑实例引用，用于：
                   - 读取密钥：self.brain.data_manager.get_api_credentials()
                   - 读取其他数据（如需要）
        """
        self.brain = brain
        logger.info("👷【下单工人】初始化完成，已获得密钥读取权限")
    
    # ========================================================================
    # 内部方法：线程池执行器
    # ========================================================================
    
    async def _run_sync(self, func, *args, **kwargs):
        """
        在线程池中运行同步函数
        
        CCXT 的 create_order() 是同步的，会阻塞事件循环。
        使用 run_in_executor 将其放到线程池中运行，避免阻塞。
        
        Args:
            func: 要执行的同步函数
            *args, **kwargs: 函数参数
        
        Returns:
            函数执行结果
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,  # None 表示使用默认线程池
            lambda: func(*args, **kwargs)
        )
    
    # ========================================================================
    # 交易接口（开仓/平仓共用）
    # ========================================================================
    
    async def place_order(self, exchange: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        开仓或平仓
        
        大脑调用这个方法，传入完整的订单参数。
        工人不关心 params 里是什么，只负责原样转发。
        
        Args:
            exchange: 交易所名称，可选值：
                      - 'binance': 币安
                      - 'okx': 欧易
            params: 完整的订单参数，由大脑组装，包含：
                    - symbol: 合约名，如 'BTCUSDT'
                    - side: 方向，'buy' 或 'sell'
                    - type: 订单类型，'market' 或 'limit'
                    - amount: 数量
                    - price: 限价单价格（type='limit' 时需要）
                    - reduceOnly: 是否只减仓（平仓时用）
                    - closePosition: 是否清仓（币安专用，不需要 amount）
                    - 其他交易所特定参数
        
        Returns:
            交易所返回的原始订单结果，格式由交易所决定
        
        Example:
            # 大脑调用示例
            result = await trader.place_order('binance', {
                'symbol': 'BTCUSDT',
                'side': 'buy',
                'type': 'market',
                'amount': 0.01,
                'leverage': 20  # 杠杆需要提前设置好
            })
        """
        logger.info(f"👷【下单工人】收到交易请求: {exchange}")
        logger.info(f"   参数: {params}")
        
        try:
            # ------------------------------------------------------------------
            # 步骤1：按需读取密钥
            # 从大脑的 data_manager 存储区读取，不保存
            # ------------------------------------------------------------------
            api_creds = await self.brain.data_manager.get_api_credentials(exchange)
            if not api_creds:
                error_msg = f"无法获取{exchange}的API凭证"
                logger.error(f"❌【下单工人】{error_msg}")
                return {
                    "success": False,
                    "error": error_msg,
                    "exchange": exchange
                }
            
            # ------------------------------------------------------------------
            # 步骤2：创建交易所连接
            # 根据交易所名称创建对应的 CCXT 实例
            # 当前使用模拟交易接口，真实接口已注释
            # ------------------------------------------------------------------
            exchange_instance = self._create_exchange(exchange, api_creds)
            if not exchange_instance:
                error_msg = f"创建{exchange}连接失败"
                logger.error(f"❌【下单工人】{error_msg}")
                return {
                    "success": False,
                    "error": error_msg,
                    "exchange": exchange
                }
            
            # ------------------------------------------------------------------
            # 步骤3：发单到交易所
            # CCXT 是同步库，放到线程池中运行，避免阻塞事件循环
            # ------------------------------------------------------------------
            result = await self._run_sync(
                exchange_instance.create_order,
                **params
            )
            
            logger.info(f"✅【下单工人】交易请求成功: {exchange}")
            logger.info(f"   返回结果: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌【下单工人】交易请求失败: {exchange}, 错误: {e}")
            return {
                "success": False,
                "error": str(e),
                "exchange": exchange,
                "params": params
            }
    
    # ========================================================================
    # 止损止盈专用接口
    # ========================================================================
    
    async def set_sl_tp(self, exchange: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        设置止损止盈
        
        大脑调用这个方法，传入完整的止损止盈参数。
        工人只负责原样转发，不关心参数内容。
        
        Args:
            exchange: 交易所名称，'binance' 或 'okx'
            params: 止损止盈参数，由大脑组装，包含：
                    - symbol: 合约名
                    - stopLossPrice: 止损触发价（可选）
                    - takeProfitPrice: 止盈触发价（可选）
                    - 其他交易所特定参数
        
        Returns:
            交易所返回的结果
        
        Example:
            # 大脑调用示例
            result = await trader.set_sl_tp('binance', {
                'symbol': 'BTCUSDT',
                'stopLossPrice': 49000,
                'takeProfitPrice': 52000
            })
        """
        logger.info(f"👷【下单工人】收到止损止盈请求: {exchange}")
        logger.info(f"   参数: {params}")
        
        try:
            # 按需读取密钥
            api_creds = await self.brain.data_manager.get_api_credentials(exchange)
            if not api_creds:
                return {
                    "success": False,
                    "error": f"无法获取{exchange}的API凭证",
                    "exchange": exchange
                }
            
            # 创建交易所连接
            exchange_instance = self._create_exchange(exchange, api_creds)
            if not exchange_instance:
                return {
                    "success": False,
                    "error": f"创建{exchange}连接失败",
                    "exchange": exchange
                }
            
            # 发单到交易所（线程池中运行）
            result = await self._run_sync(
                exchange_instance.create_order,
                **params
            )
            
            logger.info(f"✅【下单工人】止损止盈设置成功: {exchange}")
            logger.info(f"   返回结果: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌【下单工人】止损止盈设置失败: {exchange}, 错误: {e}")
            return {
                "success": False,
                "error": str(e),
                "exchange": exchange,
                "params": params
            }
    
    # ========================================================================
    # 内部方法：创建交易所连接
    # ========================================================================
    
    def _create_exchange(self, exchange: str, api_creds: Dict[str, Any]) -> Optional[Any]:
        """
        创建交易所连接实例
        
        这是一个同步方法，不涉及网络 IO，只做对象创建。
        所以不需要放到线程池中运行。
        
        Args:
            exchange: 交易所名称
            api_creds: API凭证字典，包含：
                       - api_key: API密钥
                       - api_secret: API密钥密文
                       - passphrase: 欧易专用
        Returns:
            CCXT交易所实例，失败返回 None
        
        模拟/真实交易切换说明：
        ================================================================
        当前使用的是【模拟交易接口】，用于调试。
        
        要切换到真实交易接口，请：
        1. 注释掉当前 return 的代码块
        2. 取消注释下面"真实交易接口"的代码块
        
        币安模拟网：testnet.binancefuture.com
        币安真实网：fapi.binance.com
        
        欧易模拟盘：sandbox: True
        欧易真实盘：sandbox: False
        ================================================================
        """
        try:
            if exchange == 'binance':
                # ============================================================
                # 币安 - 模拟交易接口（调试用）
                # ============================================================
                exchange_instance = ccxt.binance({
                    'apiKey': api_creds['api_key'],
                    'secret': api_creds['api_secret'],
                    'options': {
                        'defaultType': 'future',  # 合约交易（USDT-M）
                    }
                })
                # 使用官方推荐的 sandbox 模式设置测试网
                exchange_instance.set_sandbox_mode(True)
                return exchange_instance
                
                # ============================================================
                # 币安 - 真实交易接口（调试完成后启用）
                # ============================================================
                # return ccxt.binance({
                #     'apiKey': api_creds['api_key'],
                #     'secret': api_creds['api_secret'],
                #     'options': {
                #         'defaultType': 'future',
                #     }
                # })
                
            elif exchange == 'okx':
                # ============================================================
                # 欧易 - 模拟交易接口（调试用）
                # ============================================================
                return ccxt.okx({
                    'apiKey': api_creds['api_key'],
                    'secret': api_creds['api_secret'],
                    'password': api_creds.get('passphrase', ''),  # 欧易需要 passphrase
                    'options': {
                        'defaultType': 'swap',   # 永续合约
                        'sandbox': True,         # 模拟盘模式
                    }
                })
                
                # ============================================================
                # 欧易 - 真实交易接口（调试完成后启用）
                # ============================================================
                # return ccxt.okx({
                #     'apiKey': api_creds['api_key'],
                #     'secret': api_creds['api_secret'],
                #     'password': api_creds.get('passphrase', ''),
                #     'options': {
                #         'defaultType': 'swap',
                #         'sandbox': False,
                #     }
                # })
                
            else:
                logger.error(f"❌【下单工人】不支持的交易所: {exchange}")
                return None
                
        except Exception as e:
            logger.error(f"❌【下单工人】创建{exchange}连接失败: {e}")
            return None