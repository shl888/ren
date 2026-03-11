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

2. closed_positions（历史表）
   - 作用：永久保存所有已平仓记录
   - 特点：追加写入，永不删除，永不覆盖

【调用关系】
调度器 (scheduler.py) 
    ↓ 推送 {tag, data}
数据库 (database.py) 
    ↓ 根据tag执行不同逻辑
Turso数据库

【重要原则 - 数据写入】
数据库文件只做一件事：字段名匹配，值原样写入。

1. 字段名一对一：数据里的字段名和表字段名完全对应
2. 值是什么类型就用什么类型标记：
   - int → integer
   - float → float
   - str → text
   - None → null
3. 不做任何字段类型判断，不关心字段的业务含义
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
        self.url = os.getenv('TURSO_DATABASE_URL')
        self.token = os.getenv('TURSO_DATABASE_TOKEN')
        
        # ----- 第2步：验证配置是否存在 -----
        if not self.url or not self.token:
            raise ValueError(
                "❌ 环境变量 TURSO_DATABASE_URL 和 TURSO_DATABASE_TOKEN 必须设置\n"
                "请设置:\n"
                "  export TURSO_DATABASE_URL=https://你的数据库.turso.io\n"
                "  export TURSO_DATABASE_TOKEN=你的令牌"
            )
        
        logger.info("✅ 数据库配置加载成功")
        
        # ----- 第3步：测试数据库连接 -----
        if not self.test_connection():
            raise ConnectionError("❌ 无法连接到数据库，请检查网络和令牌")
        
        # ----- 第4步：初始化数据库表 -----
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
        ==================================================
        
        :param tag: 数据标签（已平仓/持仓完整）
        :param data: 完整的原始数据（字段全是中文）
        """
        try:
            # ----- 第1步：从数据中提取交易所信息 -----
            exchange = data.get('交易所')
            if not exchange:
                logger.error("❌ 数据中没有'交易所'字段，无法处理")
                return
            
            # ----- 第2步：根据标签执行不同逻辑 -----
            if tag == '已平仓':
                logger.info(f"📦 收到已平仓数据: {exchange}")
                await self._handle_closed(data, exchange)
                
            elif tag == '持仓完整':
                contract = data.get('开仓合约名', 'unknown')
                logger.info(f"📦 收到持仓完整数据: {exchange} - {contract}")
                await self._handle_active(data)
                
            else:
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
        ==================================================
        
        :param data: 已平仓的完整数据
        :param exchange: 交易所名称（binance/okx）
        """
        # ----- 第1步：写入历史表（追加）-----
        await self._insert_closed_position(data)
        
        # ----- 第2步：清理持仓表（只清理该交易所）-----
        await self._delete_active_position(exchange)
        
        logger.info(f"✅ 已平仓处理完成: {exchange}")
    
    async def _handle_active(self, data: Dict[str, Any]):
        """
        处理持仓完整数据
        ==================================================
        做一件事：覆盖更新到持仓区
        ==================================================
        
        :param data: 持仓完整的完整数据
        """
        await self._save_active_position(data)
    
    # ==================== 实际数据库操作 ====================
    
    async def _save_active_position(self, data: Dict[str, Any]):
        """
        持仓区：覆盖更新
        ==================================================
        SQL说明：
            使用 INSERT OR REPLACE 实现覆盖更新
        
        字段说明：
            data字典的key必须和表字段完全一致（都是中文）
        
        id生成规则：
            如果数据里没有id，就用"交易所_合约名_开仓时间"拼一个
        ==================================================
        
        :param data: 数据字典，key必须全是中文
        """
        # ----- 第1步：确保数据有id字段 -----
        if 'id' not in data:
            exchange = data.get('交易所', 'unknown')
            contract = data.get('开仓合约名', 'unknown')
            open_time = data.get('开仓时间', '')
            data['id'] = f"{exchange}_{contract}_{open_time}"
            logger.debug(f"🔑 生成id: {data['id']}")
        
        # ----- 第2步：构建SQL语句 -----
        fields = list(data.keys())
        
        logger.info(f"📊 持仓表 - 字段数量: {len(fields)}")
        logger.info(f"📋 持仓表 - 字段列表: {fields}")
        
        placeholders = ','.join(['?' for _ in fields])
        values = [data.get(f) for f in fields]
        
        placeholders_count = len(placeholders.split(','))
        logger.info(f"📊 持仓表 - 占位符数量: {placeholders_count}")
        logger.info(f"📊 持仓表 - 值数量: {len(values)}")
        
        if len(fields) != len(values):
            logger.error(f"❌ 持仓表 - 字段数量不匹配! fields={len(fields)}, values={len(values)}")
            return
        
        sql = f"""
            INSERT OR REPLACE INTO active_positions 
            ({','.join(fields)}) 
            VALUES ({placeholders})
        """
        
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
        ==================================================
        
        :param data: 数据字典，key必须全是中文
        """
        fields = list(data.keys())
        
        logger.info(f"📊 历史表 - 字段数量: {len(fields)}")
        logger.info(f"📋 历史表 - 字段列表: {fields}")
        
        placeholders = ','.join(['?' for _ in fields])
        values = [data.get(f) for f in fields]
        
        placeholders_count = len(placeholders.split(','))
        logger.info(f"📊 历史表 - 占位符数量: {placeholders_count}")
        logger.info(f"📊 历史表 - 值数量: {len(values)}")
        
        if len(fields) != len(values):
            logger.error(f"❌ 历史表 - 字段数量不匹配! fields={len(fields)}, values={len(values)}")
            return
        
        sql = f"""
            INSERT INTO closed_positions 
            ({','.join(fields)}) 
            VALUES ({placeholders})
        """
        
        logger.debug(f"📝 历史表 SQL: {sql}")
        logger.debug(f"📝 历史表 值: {values}")
        
        self._run_sql(sql, values)
        logger.debug(f"📜 历史已写入: {data.get('交易所')} - {data.get('开仓合约名')} 平仓时间:{data.get('平仓时间')}")
    
    async def _delete_active_position(self, exchange: str):
        """
        清理持仓区 - 只清理指定交易所
        ==================================================
        重要警告：
            必须传入exchange参数，绝对不能删除所有持仓
        ==================================================
        
        :param exchange: 'binance' 或 'okx'，不能为空
        """
        if not exchange:
            logger.error("❌ 清理持仓必须传入交易所参数，本次操作已取消")
            return
        
        sql = "DELETE FROM active_positions WHERE 交易所 = ?"
        self._run_sql(sql, [exchange])
        logger.info(f"🧹 持仓已清理: {exchange}")
    
    # ==================== SQL执行基础方法 ====================
    
    def _run_sql(self, sql: str, params: List = None) -> Dict:
        """
        执行SQL语句 - Turso API 最终修正版
        ==================================================
        【Turso API 格式要求 - 关键规则】
        
        Turso 的 value 字段类型必须与 type 标记严格匹配：
        
        1. null: {"type": "null"}
           - 不需要 value 字段
           
        2. integer: {"type": "integer", "value": 20}
           - value 可以是 Python int 或 str（如 20 或 "20"）
           
        3. float: {"type": "float", "value": 92045.29}
           - 【关键】value 必须是 Python float 类型（数字）
           - 不能是字符串 "92045.29"，否则报错：expected f64
           
        4. text: {"type": "text", "value": "okx"}
           - value 必须是字符串
        
        【修复历史】
        - 第1次修复：bool 判断顺序 + 强制原生类型转换（失败）
        - 第2次修复：所有 value 转字符串（失败，float不能是字符串）
        - 第3次修复（当前）：float 保持数字类型，其他按需处理
        
        【类型处理逻辑】
        - None → null
        - bool → integer (0/1)，必须在 int 之前判断
        - int → integer，value 为 int 类型
        - float → float，value 为 float 类型（不能转字符串！）
        - str → text
        - 其他 → 尝试转 float，不行转 str
        ==================================================
        
        :param sql: SQL语句（字段名用中文）
        :param params: 参数列表
        :return: Turso API返回的原始结果
        """
        if params is None:
            params = []
        
        # ----- 构建 args 数组（Turso API格式）-----
        args = []
        
        for idx, p in enumerate(params):
            logger.debug(f"参数[{idx}]: 值={p}, 原始类型={type(p).__name__}")
            
            if p is None:
                # null 类型不需要 value 字段
                args.append({"type": "null"})
                logger.debug(f"   → null")
                
            elif isinstance(p, bool):
                # 【关键】bool 必须在 int 之前判断！因为 isinstance(True, int) == True
                # Turso 没有 bool 类型，用 integer 0/1 表示
                bool_val = 1 if p else 0
                args.append({"type": "integer", "value": bool_val})
                logger.debug(f"   → bool转integer: {p} → {bool_val}")
                
            elif isinstance(p, int):
                # 整数：value 为 int 类型（Turso 也接受字符串，但用数字更规范）
                native_int = int(p)
                args.append({"type": "integer", "value": native_int})
                logger.debug(f"   → integer: {native_int}")
                
            elif isinstance(p, float):
                # 【关键修复】浮点数：value 必须是 float 类型，不能是字符串！
                # 如果转字符串 "92045.29"，Turso 报错：expected f64
                native_float = float(p)
                args.append({"type": "float", "value": native_float})
                logger.debug(f"   → float: {native_float}")
                
            elif isinstance(p, str):
                # 字符串：value 为 str 类型
                args.append({"type": "text", "value": p})
                logger.debug(f"   → text: '{p[:50]}...'")
                
            else:
                # 其他类型（numpy、decimal、datetime等）：先尝试转 float，不行转 str
                try:
                    num_val = float(p)
                    args.append({"type": "float", "value": num_val})
                    logger.warning(f"⚠️ 未知类型 {type(p).__name__}，转为float: {num_val}")
                except (ValueError, TypeError):
                    str_val = str(p)
                    args.append({"type": "text", "value": str_val})
                    logger.warning(f"⚠️ 未知类型 {type(p).__name__}，转为text: '{str_val[:50]}...'")
        
        # ----- 构建请求体 -----
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
        
        logger.debug(f"📤 发送SQL请求到Turso")
        logger.debug(f"   SQL: {sql[:100]}...")
        logger.debug(f"   参数数量: {len(args)}")
        
        # ----- 发送HTTP请求到Turso -----
        try:
            import json
            
            # 序列化为JSON，确保数字类型正确编码
            json_data = json.dumps(payload, ensure_ascii=False)
            
            response = requests.post(
                f"{self.url}/v2/pipeline",
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json"
                },
                data=json_data,
                timeout=10
            )
            response.raise_for_status()
            
            logger.debug(f"📥 收到数据库响应: 状态码={response.status_code}")
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.error(f"❌ 数据库请求超时: {sql[:50]}...")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ 数据库请求失败: {e}\nSQL: {sql[:200]}...")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                    logger.error(f"❌ 错误详情: {error_detail}")
                except:
                    logger.error(f"❌ 响应内容: {e.response.text[:500]}")
            raise
        except Exception as e:
            logger.error(f"❌ 未知错误: {e}\nSQL: {sql[:200]}...")
            raise
    
    # ==================== 连接测试 ====================
    
    def test_connection(self) -> bool:
        """
        测试数据库连接是否正常
        ==================================================
        执行一个简单的SQL查询，验证连接
        ==================================================
        
        :return: True=连接正常，False=连接失败
        """
        try:
            result = self._run_sql("SELECT 1")
            
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
        确保数据库表存在，如果不存在就创建
        ==================================================
        """
        try:
            tables = self._get_tables()
            logger.debug(f"当前数据库中的表: {tables}")
            
            if 'active_positions' not in tables:
                self._create_active_positions_table()
                logger.info("✅ 创建持仓表 active_positions")
            
            if 'closed_positions' not in tables:
                self._create_closed_positions_table()
                logger.info("✅ 创建历史表 closed_positions")
            
            self._create_indexes()
            
            logger.info("✅ 数据库初始化完成")
            
        except Exception as e:
            logger.error(f"❌ 数据库初始化失败: {e}")
            raise
    
    def _get_tables(self) -> List[str]:
        """
        获取当前数据库中的所有表名
        ==================================================
        执行SQLite的系统表查询
        ==================================================
        
        :return: 表名列表
        """
        sql = "SELECT name FROM sqlite_master WHERE type='table';"
        result = self._run_sql(sql)
        
        tables = []
        if result and 'results' in result:
            rows = result['results'][0].get('result', {}).get('rows', [])
            for row in rows:
                if row and len(row) > 0:
                    tables.append(row[0].get('value'))
        
        return tables
    
    def _create_active_positions_table(self):
        """
        创建持仓表
        """
        sql = """
        CREATE TABLE IF NOT EXISTS active_positions (
            id TEXT PRIMARY KEY,
            交易所 TEXT NOT NULL,
            账户资产额 REAL,
            资产币种 TEXT,
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
            标记价 REAL,
            标记价涨跌盈亏幅 REAL,
            标记价保证金 REAL,
            标记价仓位价值 REAL,
            标记价浮盈 REAL,
            标记价浮盈百分比 REAL,
            最新价 REAL,
            最新价涨跌盈亏幅 REAL,
            最新价保证金 REAL,
            最新价仓位价值 REAL,
            最新价浮盈 REAL,
            最新价浮盈百分比 REAL,
            止损触发方式 TEXT,
            止损触发价 REAL,
            止损幅度 REAL,
            止盈触发方式 TEXT,
            止盈触发价 REAL,
            止盈幅度 REAL,
            本次资金费 REAL DEFAULT 0,
            累计资金费 REAL DEFAULT 0,
            资金费结算次数 INTEGER DEFAULT 0,
            平均资金费率 REAL,
            本次资金费结算时间 DATETIME,
            平仓执行方式 TEXT,
            平仓价 REAL,
            平仓价涨跌盈亏幅 REAL,
            平仓价仓位价值 REAL,
            平仓手续费 REAL,
            平仓手续费币种 TEXT,
            平仓收益 REAL,
            平仓收益率 REAL,
            平仓时间 DATETIME,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        self._run_sql(sql)
    
    def _create_closed_positions_table(self):
        """
        创建历史表
        """
        sql = """
        CREATE TABLE IF NOT EXISTS closed_positions (
            id TEXT,
            交易所 TEXT NOT NULL,
            账户资产额 REAL,
            资产币种 TEXT,
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
            标记价 REAL,
            标记价涨跌盈亏幅 REAL,
            标记价保证金 REAL,
            标记价仓位价值 REAL,
            标记价浮盈 REAL,
            标记价浮盈百分比 REAL,
            最新价 REAL,
            最新价涨跌盈亏幅 REAL,
            最新价保证金 REAL,
            最新价仓位价值 REAL,
            最新价浮盈 REAL,
            最新价浮盈百分比 REAL,
            止损触发方式 TEXT,
            止损触发价 REAL,
            止损幅度 REAL,
            止盈触发方式 TEXT,
            止盈触发价 REAL,
            止盈幅度 REAL,
            本次资金费 REAL DEFAULT 0,
            累计资金费 REAL DEFAULT 0,
            资金费结算次数 INTEGER DEFAULT 0,
            平均资金费率 REAL,
            本次资金费结算时间 DATETIME,
            平仓执行方式 TEXT,
            平仓价 REAL,
            平仓价涨跌盈亏幅 REAL,
            平仓价仓位价值 REAL,
            平仓手续费 REAL,
            平仓手续费币种 TEXT,
            平仓收益 REAL,
            平仓收益率 REAL,
            平仓时间 DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        self._run_sql(sql)
    
    def _create_indexes(self):
        """
        创建索引
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
                logger.warning(f"⚠️ 索引创建失败（可能已存在）: {e}")
