"""
数据库存储区 - Turso数据库
==================================================
【文件职责】
这个文件只做一件事：接收调度器推送的数据，然后写入Turso数据库。

【重要提醒】
1. 这个文件不提供任何读取接口（修复区如果需要读数据库，应该直接连接数据库）
2. 这个文件只与调度器对话，不与任何其他文件对话
3. 所有表字段都是中文，SQL语句必须用中文字段名
4. 数据库会完整保存所有字段，空值就是 null，不需要过滤

【表结构】
数据库里有两张表，都在同一个Turso数据库里：

1. active_positions（持仓表）
   - 作用：存储当前正在持仓的数据
   - 特点：覆盖更新，每个交易所只能有一条数据
   - 举例：币安开仓BTC → 写入一条；币安平仓 → 删除这条

2. closed_positions（历史表）
   - 作用：永久保存所有已平仓记录
   - 特点：追加写入，永不删除，永不覆盖
   - 举例：每次平仓都新增一条记录，几年后还能查到

【调用关系】
调度器 (scheduler.py) 
    ↓ 推送 {tag, data}  （tag是"已平仓"或"持仓完整"）
数据库 (database.py) 
    ↓ 根据tag执行不同逻辑
Turso数据库

【处理逻辑】
- 收到"已平仓"标签：两步走
  1. 写入历史表（永久保存）
  2. 清理持仓表（只清理该交易所的，腾出位置）

- 收到"持仓完整"标签：一步走
  1. 写入持仓表（覆盖更新，旧数据自动被替换）

- 收不到"空仓"标签：调度器已经禁止推送空仓数据
==================================================
"""

import os
import requests
import logging
from typing import Dict, Any, List, Optional

# 配置日志
logger = logging.getLogger(__name__)


class Database:
    """
    数据库操作类
    ==================================================
    这个类负责所有数据库写入操作，不提供任何读取接口。
    所有方法都是私有的（_开头），对外只暴露 handle_data 一个入口。
    ==================================================
    """
    
    def __init__(self):
        """
        初始化数据库连接
        ==================================================
        做了四件事：
        1. 从环境变量读取数据库URL和令牌
        2. 验证配置是否存在
        3. 测试数据库连接是否正常
        4. 初始化数据库表（如果表不存在就创建）
        ==================================================
        """
        # ----- 第1步：从环境变量读取配置 -----
        # 为什么用环境变量？因为数据库令牌是敏感信息，不能写死在代码里
        self.url = os.getenv('TURSO_DATABASE_URL')
        self.token = os.getenv('TURSO_DATABASE_TOKEN')
        
        # ----- 第2步：验证配置是否存在 -----
        # 如果环境变量没设置，直接报错，不让程序继续运行
        if not self.url or not self.token:
            raise ValueError(
                "❌ 环境变量 TURSO_DATABASE_URL 和 TURSO_DATABASE_TOKEN 必须设置\n"
                "请设置:\n"
                "  export TURSO_DATABASE_URL=https://你的数据库.turso.io\n"
                "  export TURSO_DATABASE_TOKEN=你的令牌"
            )
        
        logger.info("✅ 数据库配置加载成功")
        
        # ----- 第3步：测试数据库连接 -----
        # 确保数据库真的能连上，避免后续操作失败
        if not self.test_connection():
            raise ConnectionError("❌ 无法连接到数据库，请检查网络和令牌")
        
        # ----- 第4步：初始化数据库表 -----
        # 启动时自动检测表是否存在，不存在就创建
        # 这样部署新环境时就不用手动建表了
        self._init_database()
    
    # ==================== 对外唯一入口 ====================
    
    async def handle_data(self, tag: str, data: Dict[str, Any]):
        """
        接收调度器推送的数据 - 这是数据库文件的唯一入口
        ==================================================
        参数说明：
            tag: 数据标签，只能是"已平仓"或"持仓完整"
            data: 完整的原始数据，字段全是中文
        
        处理逻辑：
            根据tag的值，执行不同的数据库操作
        
        注意事项：
            1. 这个方法是被调度器调用的，不要在其他地方调用
            2. 空仓标签的数据不会到这里，因为调度器禁止推送
            3. 这个方法内部调用了私有方法，不直接操作SQL
        ==================================================
        
        :param tag: 数据标签（已平仓/持仓完整）
        :param data: 完整的原始数据（字段全是中文）
        """
        try:
            # ----- 第1步：从数据中提取交易所信息 -----
            # 为什么要提取交易所？因为后续清理持仓时需要知道清理哪个交易所
            exchange = data.get('交易所')
            if not exchange:
                logger.error("❌ 数据中没有'交易所'字段，无法处理")
                return
            
            # ----- 第2步：根据标签执行不同逻辑 -----
            if tag == '已平仓':
                # 已平仓数据要做两件事：
                # 1. 写入历史区（永久保存，以后查账用）
                # 2. 清理持仓区（腾出位置给下次开仓）
                logger.info(f"📦 收到已平仓数据: {exchange}")
                await self._handle_closed(data, exchange)
                
            elif tag == '持仓完整':
                # 持仓完整数据：覆盖更新到持仓区
                contract = data.get('开仓合约名', 'unknown')
                logger.info(f"📦 收到持仓完整数据: {exchange} - {contract}")
                await self._handle_active(data)
                
            else:
                # 理论上不会走到这里，因为调度器只会推送这两种标签
                logger.warning(f"⚠️ 收到未知标签: {tag}")
                
        except Exception as e:
            logger.error(f"❌ 处理数据失败: {e}", exc_info=True)
    
    # ==================== 内部处理方法 ====================
    
    async def _handle_closed(self, data: Dict[str, Any], exchange: str):
        """
        处理已平仓数据
        ==================================================
        做了两件事，顺序很重要：
        1. 先写入历史表（永久保存）
        2. 再清理持仓表（只清理这个交易所）
        
        为什么先写历史再清理？
            如果先清理，万一写入历史失败，数据就丢了。
            先写历史，即使清理失败，至少数据还在历史表里。
        ==================================================
        
        :param data: 已平仓的完整数据
        :param exchange: 交易所名称（binance/okx）
        """
        # ----- 第1步：写入历史表（追加）-----
        # 历史表是永久保存的，所以用INSERT，不是INSERT OR REPLACE
        await self._insert_closed_position(data)
        
        # ----- 第2步：清理持仓表（只清理该交易所）-----
        # 清理时必须指定交易所，绝对不能写DELETE FROM active_positions
        # 那样会把所有交易所的持仓都删掉
        await self._delete_active_position(exchange)
        
        logger.info(f"✅ 已平仓处理完成: {exchange}")
    
    async def _handle_active(self, data: Dict[str, Any]):
        """
        处理持仓完整数据
        ==================================================
        做一件事：覆盖更新到持仓区
        
        为什么是覆盖更新？
            因为持仓区每个交易所只能有一条数据。
            新数据来了，旧数据自动被替换，不需要先删除。
        ==================================================
        
        :param data: 持仓完整的完整数据
        """
        await self._save_active_position(data)
    
    # ==================== 实际数据库操作 ====================
    # 以下方法都是私有的，只被上面的处理方法调用
    # 所有SQL语句都直接写在方法里，这样逻辑更清晰
    
    async def _save_active_position(self, data: Dict[str, Any]):
        """
        持仓区：覆盖更新
        ==================================================
        SQL说明：
            使用 INSERT OR REPLACE 实现覆盖更新
            如果主键id存在，就更新；不存在，就插入
        
        字段说明：
            data字典的key必须和表字段完全一致（都是中文）
            例如：data['交易所']、data['开仓合约名']
        
        重要说明：
            - 数据库会完整保存所有字段，空值就是 null
            - 不需要过滤任何字段
            - 字段数量和占位符数量必须完全一致
        
        id生成规则：
            如果数据里没有id，就用"交易所_合约名_开仓时间"拼一个
            这样能保证每条数据都有唯一的主键
        ==================================================
        
        :param data: 数据字典，key必须全是中文
        """
        # ----- 第1步：确保数据有id字段 -----
        # 表的主键是id，所以每条数据必须有id
        if 'id' not in data:
            exchange = data.get('交易所', 'unknown')
            contract = data.get('开仓合约名', 'unknown')
            open_time = data.get('开仓时间', '')
            data['id'] = f"{exchange}_{contract}_{open_time}"
            logger.debug(f"🔑 生成id: {data['id']}")
        
        # ----- 第2步：构建SQL语句 -----
        # fields是所有字段名（中文）
        fields = list(data.keys())
        
        # 🔍 调试：打印字段数量和字段列表
        logger.info(f"📊 持仓表 - 字段数量: {len(fields)}")
        logger.info(f"📋 持仓表 - 字段列表: {fields}")
        
        # 生成占位符：有多少个字段就有多少个问号
        placeholders = ','.join(['?' for _ in fields])
        
        # values是对应的值（包括null值）
        values = [data.get(f) for f in fields]
        
        # 🔍 调试：确认字段数量匹配
        placeholders_count = len(placeholders.split(','))
        logger.info(f"📊 持仓表 - 占位符数量: {placeholders_count}")
        logger.info(f"📊 持仓表 - 值数量: {len(values)}")
        
        if len(fields) != len(values):
            logger.error(f"❌ 持仓表 - 字段数量不匹配! fields={len(fields)}, values={len(values)}")
            # 打印不匹配的详细信息
            for i, field in enumerate(fields):
                if i < len(values):
                    logger.error(f"  字段[{i}]: {field} = {values[i]}")
                else:
                    logger.error(f"  字段[{i}]: {field} = (无对应值)")
            return
        
        if len(fields) != placeholders_count:
            logger.error(f"❌ 持仓表 - 字段数量和占位符数量不匹配! fields={len(fields)}, placeholders={placeholders_count}")
            return
        
        # 组装SQL
        sql = f"""
            INSERT OR REPLACE INTO active_positions 
            ({','.join(fields)}) 
            VALUES ({placeholders})
        """
        
        # 🔍 打印完整SQL用于调试
        logger.debug(f"📝 持仓表 SQL: {sql}")
        logger.debug(f"📝 持仓表 值: {values}")
        
        # ----- 第3步：执行SQL -----
        self._run_sql(sql, values)
        logger.debug(f"💾 持仓已保存: {data.get('交易所')} - {data.get('开仓合约名')}")
    
    async def _insert_closed_position(self, data: Dict[str, Any]):
        """
        历史区：追加写入
        ==================================================
        SQL说明：
            使用 INSERT 实现追加写入
            每次执行都会在表里新增一行，不会覆盖旧数据
        
        注意事项：
            历史表没有主键约束，可以重复写入
            但实际业务中，同一笔平仓不会重复推送
        
        重要说明：
            - 数据库会完整保存所有字段，空值就是 null
            - 不需要过滤任何字段
            - 字段数量和占位符数量必须完全一致
        ==================================================
        
        :param data: 数据字典，key必须全是中文
        """
        fields = list(data.keys())
        
        # 🔍 调试：打印字段数量和字段列表
        logger.info(f"📊 历史表 - 字段数量: {len(fields)}")
        logger.info(f"📋 历史表 - 字段列表: {fields}")
        
        placeholders = ','.join(['?' for _ in fields])
        values = [data.get(f) for f in fields]
        
        # 🔍 调试：确认字段数量匹配
        placeholders_count = len(placeholders.split(','))
        logger.info(f"📊 历史表 - 占位符数量: {placeholders_count}")
        logger.info(f"📊 历史表 - 值数量: {len(values)}")
        
        if len(fields) != len(values):
            logger.error(f"❌ 历史表 - 字段数量不匹配! fields={len(fields)}, values={len(values)}")
            # 打印不匹配的详细信息
            for i, field in enumerate(fields):
                if i < len(values):
                    logger.error(f"  字段[{i}]: {field} = {values[i]}")
                else:
                    logger.error(f"  字段[{i}]: {field} = (无对应值)")
            return
        
        if len(fields) != placeholders_count:
            logger.error(f"❌ 历史表 - 字段数量和占位符数量不匹配! fields={len(fields)}, placeholders={placeholders_count}")
            return
        
        sql = f"""
            INSERT INTO closed_positions 
            ({','.join(fields)}) 
            VALUES ({placeholders})
        """
        
        # 🔍 打印完整SQL用于调试
        logger.debug(f"📝 历史表 SQL: {sql}")
        logger.debug(f"📝 历史表 值: {values}")
        
        self._run_sql(sql, values)
        logger.debug(f"📜 历史已写入: {data.get('交易所')} - {data.get('开仓合约名')} 平仓时间:{data.get('平仓时间')}")
    
    async def _delete_active_position(self, exchange: str):
        """
        清理持仓区 - 只清理指定交易所
        ==================================================
        重要警告：
            这个方法必须传入exchange参数
            绝对禁止写成：DELETE FROM active_positions
            那样会把所有交易所的持仓都删掉，造成数据丢失！
        
        执行时机：
            当某个交易所平仓时调用，清理该交易所的旧持仓
            为新开仓腾出位置
        ==================================================
        
        :param exchange: 'binance' 或 'okx'，不能为空
        """
        # ----- 安全检查 -----
        if not exchange:
            logger.error("❌ 清理持仓必须传入交易所参数，本次操作已取消")
            return
        
        # ----- 执行删除 -----
        sql = "DELETE FROM active_positions WHERE 交易所 = ?"
        self._run_sql(sql, [exchange])
        logger.info(f"🧹 持仓已清理: {exchange}")
    
    # ==================== SQL执行基础方法 ====================
    
    def _run_sql(self, sql: str, params: List = None) -> Dict:
        """
        执行SQL语句 - 所有数据库操作最终都走这个方法
        ==================================================
        参数转换规则：
            None → null类型
            int/float → integer类型
            str → text类型
            其他类型 → 转成str再作为text类型
        
        返回值格式（Turso标准返回）：
            {
                "results": [
                    {
                        "type": "execute",
                        "result": {
                            "cols": [...],  # 列信息
                            "rows": [...],  # 数据行
                            "affected_row_count": 数字  # 影响的行数
                        }
                    }
                ]
            }
        ==================================================
        
        :param sql: SQL语句（字段名用中文）
        :param params: 参数列表
        :return: Turso API返回的原始结果
        """
        if params is None:
            params = []
        
        # ----- 第1步：转换参数为Turso API要求的格式 -----
        args = []
        for p in params:
            if p is None:
                args.append({"type": "null", "value": None})
            elif isinstance(p, (int, float)):
                # 整数和浮点数都作为integer类型
                args.append({"type": "integer", "value": p})
            elif isinstance(p, str):
                args.append({"type": "text", "value": p})
            else:
                # 其他类型（如bool、dict等）转成字符串
                args.append({"type": "text", "value": str(p)})
        
        # ----- 第2步：构建请求体 -----
        payload = {
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": sql,
                        "args": args
                    }
                }
            ]
        }
        
        # ----- 第3步：发送HTTP请求到Turso -----
        try:
            response = requests.post(
                f"{self.url}/v2/pipeline",
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=10  # 10秒超时，防止卡死
            )
            response.raise_for_status()  # 如果返回4xx/5xx，抛出异常
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.error(f"❌ 数据库请求超时: {sql[:50]}...")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ 数据库请求失败: {e}\nSQL: {sql}")
            raise
        except Exception as e:
            logger.error(f"❌ 未知错误: {e}\nSQL: {sql}")
            raise
    
    # ==================== 连接测试 ====================
    
    def test_connection(self) -> bool:
        """
        测试数据库连接是否正常
        ==================================================
        执行一个简单的SQL查询，验证：
            1. 网络连接正常
            2. 令牌有效
            3. 数据库服务正常
        
        如果测试失败，程序应该停止启动，避免后续操作全部失败。
        ==================================================
        
        :return: True=连接正常，False=连接失败
        """
        try:
            # 执行最简单的SQL：SELECT 1
            result = self._run_sql("SELECT 1")
            
            # 检查返回结果
            if result and 'results' in result:
                logger.info("✅ 数据库连接测试成功")
                return True
            else:
                logger.error(f"❌ 数据库连接测试返回异常结果: {result}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 数据库连接测试失败: {e}")
            return False
    
    # ==================== 初始化/建表 ====================
    
    def _init_database(self):
        """
        初始化数据库
        ==================================================
        在程序启动时自动调用，确保数据库表存在。
        如果表已存在，不会重复创建（用了IF NOT EXISTS）。
        
        做了三件事：
        1. 获取当前所有表
        2. 如果持仓表不存在，创建它
        3. 如果历史表不存在，创建它
        4. 创建索引（提高查询速度）
        ==================================================
        """
        try:
            # ----- 第1步：获取现有表 -----
            tables = self._get_tables()
            logger.debug(f"当前数据库中的表: {tables}")
            
            # ----- 第2步：检查并创建持仓表 -----
            if 'active_positions' not in tables:
                self._create_active_positions_table()
                logger.info("✅ 创建持仓表 active_positions")
            
            # ----- 第3步：检查并创建历史表 -----
            if 'closed_positions' not in tables:
                self._create_closed_positions_table()
                logger.info("✅ 创建历史表 closed_positions")
            
            # ----- 第4步：创建索引 -----
            # 索引用于提高查询速度，重复执行无害
            self._create_indexes()
            
            logger.info("✅ 数据库初始化完成")
            
        except Exception as e:
            logger.error(f"❌ 数据库初始化失败: {e}")
            raise  # 初始化失败就让程序退出，不要继续运行
    
    def _get_tables(self) -> List[str]:
        """
        获取当前数据库中的所有表名
        ==================================================
        执行SQLite的系统表查询，返回所有用户表的名字。
        
        SQL说明：
            SELECT name FROM sqlite_master 
            WHERE type='table'
            这条语句查询SQLite的系统表，返回所有表名
        ==================================================
        
        :return: 表名列表，例如 ['active_positions', 'closed_positions']
        """
        sql = "SELECT name FROM sqlite_master WHERE type='table';"
        result = self._run_sql(sql)
        
        tables = []
        if result and 'results' in result:
            rows = result['results'][0].get('result', {}).get('rows', [])
            for row in rows:
                if row and len(row) > 0:
                    # row[0]是表名，它的结构是 {"type": "text", "value": "表名"}
                    tables.append(row[0].get('value'))
        
        return tables
    
    def _create_active_positions_table(self):
        """
        创建持仓表（覆盖更新）
        ==================================================
        表名：active_positions
        特点：使用INSERT OR REPLACE写入，自动覆盖旧数据
        
        字段说明：
            所有字段都是中文，与数据字段完全对应
            id是主键，确保每条数据唯一
            updated_at自动记录更新时间
        ==================================================
        """
        sql = """
        CREATE TABLE IF NOT EXISTS active_positions (
            -- ===== 基础信息 =====
            id TEXT PRIMARY KEY,                    -- 主键：交易所_合约名_开仓时间
            交易所 TEXT NOT NULL,                    -- binance/okx
            账户资产额 REAL,                          -- 账户总资产
            资产币种 TEXT,                            -- USDT等
            
            -- ===== 开仓信息 =====
            保证金模式 TEXT,                           -- 全仓/逐仓
            保证金币种 TEXT,                           -- USDT等
            开仓合约名 TEXT,                           -- BTCUSDT
            开仓方向 TEXT,                             -- LONG/SHORT
            开仓执行方式 TEXT,                          -- 市价/限价
            开仓价 REAL,                              -- 开仓价格
            持仓币数 REAL,                             -- 持仓数量（币）
            持仓张数 REAL,                             -- 持仓数量（张）
            合约面值 REAL,                             -- 每张合约的面值
            开仓价仓位价值 REAL,                        -- 开仓价 * 持仓币数
            杠杆 REAL,                                -- 杠杆倍数
            开仓保证金 REAL,                           -- 开仓时占用的保证金
            开仓手续费 REAL,                           -- 开仓手续费
            开仓手续费币种 TEXT,                        -- 手续费币种
            开仓时间 DATETIME,                         -- 开仓时间
            
            -- ===== 标记价相关（用于计算） =====
            标记价 REAL,                               -- 当前标记价格
            标记价涨跌盈亏幅 REAL,                       -- 基于标记价的盈亏百分比
            标记价保证金 REAL,                          -- 基于标记价计算的保证金
            标记价仓位价值 REAL,                         -- 标记价 * 持仓币数
            标记价浮盈 REAL,                            -- 基于标记价的浮动盈亏
            标记价浮盈百分比 REAL,                       -- 基于标记价的盈亏百分比
            
            -- ===== 最新价相关（用于显示） =====
            最新价 REAL,                               -- 当前最新成交价
            最新价涨跌盈亏幅 REAL,                       -- 基于最新价的盈亏百分比
            最新价保证金 REAL,                          -- 基于最新价计算的保证金
            最新价仓位价值 REAL,                         -- 最新价 * 持仓币数
            最新价浮盈 REAL,                            -- 基于最新价的浮动盈亏
            最新价浮盈百分比 REAL,                       -- 基于最新价的盈亏百分比
            
            -- ===== 止损止盈 =====
            止损触发方式 TEXT,                           -- 价格/幅度
            止损触发价 REAL,                            -- 止损触发价格
            止损幅度 REAL,                              -- 止损百分比
            止盈触发方式 TEXT,                           -- 价格/幅度
            止盈触发价 REAL,                            -- 止盈触发价格
            止盈幅度 REAL,                              -- 止盈百分比
            
            -- ===== 资金费 =====
            本次资金费 REAL DEFAULT 0,                  -- 最近一次资金费
            累计资金费 REAL DEFAULT 0,                  -- 累计资金费总和
            资金费结算次数 INTEGER DEFAULT 0,            -- 结算次数
            平均资金费率 REAL,                           -- 平均费率
            本次资金费结算时间 DATETIME,                 -- 最近结算时间
            
            -- ===== 平仓信息（持仓期间为空） =====
            平仓执行方式 TEXT,                           -- 平仓方式
            平仓价 REAL,                                -- 平仓价格
            平仓价涨跌盈亏幅 REAL,                        -- 平仓盈亏百分比
            平仓价仓位价值 REAL,                         -- 平仓价 * 持仓币数
            平仓手续费 REAL,                             -- 平仓手续费
            平仓手续费币种 TEXT,                          -- 手续费币种
            平仓收益 REAL,                               -- 平仓盈亏金额
            平仓收益率 REAL,                              -- 平仓收益率
            平仓时间 DATETIME,                           -- 平仓时间
            
            -- ===== 时间戳 =====
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP  -- 最后更新时间
        );
        """
        self._run_sql(sql)
    
    def _create_closed_positions_table(self):
        """
        创建历史表（追加写入）
        ==================================================
        表名：closed_positions
        特点：使用INSERT写入，每次都是新增记录
        
        字段说明：
            与持仓表几乎一样，但多了created_at
            没有主键，可以有多条相同id的记录（不同时间平仓）
        ==================================================
        """
        sql = """
        CREATE TABLE IF NOT EXISTS closed_positions (
            -- ===== 基础信息 =====
            id TEXT,                                   -- 可以是重复的（多次开平仓）
            交易所 TEXT NOT NULL,                    -- binance/okx
            账户资产额 REAL,                          -- 平仓时的账户资产
            资产币种 TEXT,                            -- USDT等
            
            -- ===== 开仓信息 =====
            保证金模式 TEXT,
            保证金币种 TEXT,
            开仓合约名 TEXT,
            开仓方向 TEXT,
            开仓执行方式 TEXT,
            开仓价 REAL,
            持仓币数 REAL,
            持仓张数 REAL,
            合约面值 REAL,
            开仓价仓位价值 REAL,
            杠杆 REAL,
            开仓保证金 REAL,
            开仓手续费 REAL,
            开仓手续费币种 TEXT,
            开仓时间 DATETIME,
            
            -- ===== 标记价相关 =====
            标记价 REAL,
            标记价涨跌盈亏幅 REAL,
            标记价保证金 REAL,
            标记价仓位价值 REAL,
            标记价浮盈 REAL,
            标记价浮盈百分比 REAL,
            
            -- ===== 最新价相关 =====
            最新价 REAL,
            最新价涨跌盈亏幅 REAL,
            最新价保证金 REAL,
            最新价仓位价值 REAL,
            最新价浮盈 REAL,
            最新价浮盈百分比 REAL,
            
            -- ===== 止损止盈 =====
            止损触发方式 TEXT,
            止损触发价 REAL,
            止损幅度 REAL,
            止盈触发方式 TEXT,
            止盈触发价 REAL,
            止盈幅度 REAL,
            
            -- ===== 资金费 =====
            本次资金费 REAL DEFAULT 0,
            累计资金费 REAL DEFAULT 0,
            资金费结算次数 INTEGER DEFAULT 0,
            平均资金费率 REAL,
            本次资金费结算时间 DATETIME,
            
            -- ===== 平仓信息（历史表必须有值） =====
            平仓执行方式 TEXT,
            平仓价 REAL,
            平仓价涨跌盈亏幅 REAL,
            平仓价仓位价值 REAL,
            平仓手续费 REAL,
            平仓手续费币种 TEXT,
            平仓收益 REAL,
            平仓收益率 REAL,
            平仓时间 DATETIME,
            
            -- ===== 时间戳 =====
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP  -- 记录写入时间
        );
        """
        self._run_sql(sql)
    
    def _create_indexes(self):
        """
        创建索引
        ==================================================
        索引的作用：加快查询速度
        虽然这个文件不提供查询接口，但修复区会直接查数据库
        所以索引还是需要的
        
        注意事项：
            使用IF NOT EXISTS，重复执行不会报错
        ==================================================
        """
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_active_exchange ON active_positions(交易所);",
            "CREATE INDEX IF NOT EXISTS idx_active_contract ON active_positions(开仓合约名);",
            "CREATE INDEX IF NOT EXISTS idx_closed_exchange ON closed_positions(交易所);",
            "CREATE INDEX IF NOT EXISTS idx_closed_time ON closed_positions(平仓时间);",
            "CREATE INDEX IF NOT EXISTS idx_closed_id ON closed_positions(id);"
        ]
        
        for sql in indexes:
            try:
                self._run_sql(sql)
                logger.debug(f"✅ 索引创建/已存在: {sql[:40]}...")
            except Exception as e:
                # 索引创建失败不影响主要功能，只记录警告
                logger.warning(f"⚠️ 索引创建失败（可能已存在）: {e}")