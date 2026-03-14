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
   - id生成：交易所_合约名_开仓时间
   - 幂等性：根据id判断首次写入还是静默更新

2. closed_positions（历史表）
   - 作用：永久保存所有已平仓记录
   - 特点：追加写入，永不删除，永不覆盖
   - id生成：交易所_合约名_平仓时间
   - 幂等性：根据id去重，避免重复写入

【调用关系】
调度器 (scheduler.py) 
    ↓ 推送 {tag, data}
数据库 (database.py) 
    ↓ 根据tag执行不同逻辑
Turso数据库

【重要原则】
Turso API 要求所有值都必须是字符串类型
所以不管来的是什么类型的数据，都统一转成字符串
==================================================
"""

import os
import requests
import logging
import json
import time
from typing import Dict, Any, List, Optional

# 配置日志 - 统一前缀
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
        """
        # ----- 第1步：从环境变量读取配置 -----
        self.url = os.getenv('TURSO_DATABASE_URL')
        self.token = os.getenv('TURSO_DATABASE_TOKEN')
        
        # ----- 第2步：验证配置是否存在 -----
        if not self.url or not self.token:
            raise ValueError(
                "❌ 【数据库】环境变量 TURSO_DATABASE_URL 和 TURSO_DATABASE_TOKEN 必须设置\n"
                "请设置:\n"
                "  export TURSO_DATABASE_URL=https://你的数据库.turso.io\n"
                "  export TURSO_DATABASE_TOKEN=你的令牌"
            )
        
        logger.info("✅ 【数据库】数据库配置加载成功")
        logger.info(f"🔗 【数据库】连接URL: {self.url}")
        
        # ----- 第3步：测试数据库连接 -----
        if not self.test_connection():
            raise ConnectionError("❌ 【数据库】无法连接到数据库，请检查网络和令牌")
        
        # ----- 第4步：初始化数据库表 -----
        self._init_database()
        
        # ----- 第5步：初始化日志记录集合 -----
        self._logged_active_ids = set()
        logger.info("✅ 【数据库】日志控制集合初始化完成")
        
        # ----- 第6步：初始化最后日志时间记录 -----
        self._last_log_time = 0
        self._log_interval = 60
        logger.info(f"✅ 【数据库】日志时间控制初始化完成，间隔{self._log_interval}秒")
    
    # ==================== 对外唯一入口 ====================
    
    async def handle_data(self, tag: str, data: Dict[str, Any]):
        """
        接收调度器推送的数据 - 这是数据库文件的唯一入口
        """
        try:
            exchange = data.get('交易所')
            if not exchange:
                logger.error("❌ 【数据库】数据中没有'交易所'字段，无法处理")
                return
            
            if tag == '已平仓':
                logger.info(f"📦 【数据库】收到已平仓数据: {exchange}")
                await self._handle_closed(data, exchange)
                
            elif tag == '持仓完整':
                contract = data.get('开仓合约名', 'unknown')
                
                current_time = time.time()
                time_since_last_log = current_time - self._last_log_time
                
                if time_since_last_log >= self._log_interval:
                    logger.info(f"📦 【数据库】收到持仓完整数据: {exchange} - {contract}")
                    self._last_log_time = current_time
                else:
                    logger.debug(f"📦 【数据库】收到持仓完整数据: {exchange} - {contract} (已抑制)")
                
                await self._handle_active(data)
                
            else:
                logger.warning(f"⚠️ 【数据库】收到未知标签: {tag}")
                
        except Exception as e:
            logger.error(f"❌ 【数据库】处理数据失败: {e}", exc_info=True)
    
    # ==================== 内部处理方法 ====================
    
    async def _handle_closed(self, data: Dict[str, Any], exchange: str):
        """处理已平仓数据"""
        await self._insert_closed_position(data)
        await self._delete_active_position(exchange)
        logger.info(f"✅ 【数据库】已平仓处理完成: {exchange}")
    
    async def _handle_active(self, data: Dict[str, Any]):
        """处理持仓完整数据"""
        await self._save_active_position(data)
    
    # ==================== 实际数据库操作 ====================
    
    async def _save_active_position(self, data: Dict[str, Any]):
        """持仓区：覆盖更新（日志控制版本）"""
        if 'id' not in data or not data['id']:
            exchange = data.get('交易所', 'unknown')
            contract = data.get('开仓合约名', 'unknown')
            open_time = data.get('开仓时间', '')
            data['id'] = f"{exchange}_{contract}_{open_time}"
            logger.debug(f"🔑 【数据库】持仓表生成id: {data['id']}")
        
        record_id = data['id']
        exchange = data.get('交易所', 'unknown')
        contract = data.get('开仓合约名', 'unknown')
        
        should_log_info = record_id not in self._logged_active_ids
        
        fields = list(data.keys())
        placeholders = ','.join(['?' for _ in fields])
        values = [data.get(f) for f in fields]
        
        sql = f"""
            INSERT OR REPLACE INTO active_positions 
            ({','.join(fields)}) 
            VALUES ({placeholders})
        """
        
        logger.debug(f"📝 【数据库】持仓表 SQL: {sql}")
        logger.debug(f"📝 【数据库】持仓表 值: {values}")
        
        self._run_sql(sql, values)
        
        if should_log_info:
            logger.info(f"✅ 【数据库】成功写入持仓区{exchange}数据 - {contract}（首次）")
            self._logged_active_ids.add(record_id)
    
    async def _insert_closed_position(self, data: Dict[str, Any]):
        """历史区：追加写入（带幂等性保护）"""
        if 'id' not in data or not data['id']:
            exchange = data.get('交易所', 'unknown')
            contract = data.get('开仓合约名', 'unknown')
            close_time = data.get('平仓时间', '')
            data['id'] = f"{exchange}_{contract}_{close_time}"
            logger.debug(f"🔑 【数据库】历史表生成id: {data['id']}")
        
        record_id = data['id']
        
        if self._check_exists_by_id(record_id):
            logger.info(f"⏭️ 【数据库】历史区已存在记录，跳过写入: {record_id}")
            return
        
        fields = list(data.keys())
        placeholders = ','.join(['?' for _ in fields])
        values = [data.get(f) for f in fields]
        
        sql = f"INSERT OR REPLACE INTO closed_positions ({','.join(fields)}) VALUES ({placeholders})"
        
        logger.debug(f"📝 【数据库】历史表 SQL: {sql}")
        logger.debug(f"📝 【数据库】历史表 值: {values}")
        
        self._run_sql(sql, values)
        
        exchange = data.get('交易所', 'unknown')
        contract = data.get('开仓合约名', 'unknown')
        close_time = data.get('平仓时间', 'unknown')
        logger.info(f"✅ 【数据库】成功写入历史区{exchange}数据 - {contract} 平仓时间:{close_time}")
    
    def _check_exists_by_id(self, record_id: str) -> bool:
        """根据 id 检查历史记录是否已存在"""
        if not record_id:
            return False
        
        sql = "SELECT 1 FROM closed_positions WHERE id = ? LIMIT 1"
        
        try:
            result = self._run_sql(sql, [record_id])
            
            if result and 'results' in result:
                results_list = result.get('results', [])
                if results_list and len(results_list) > 0:
                    rows = results_list[0].get('rows', [])
                    return len(rows) > 0
            return False
            
        except Exception as e:
            logger.error(f"❌ 【数据库】检查历史记录存在性失败: {e}")
            return False
    
    async def _delete_active_position(self, exchange: str):
        """清理持仓区 - 只清理指定交易所"""
        if not exchange:
            logger.error("❌ 【数据库】清理持仓必须传入交易所参数，本次操作已取消")
            return
        
        sql = "DELETE FROM active_positions WHERE 交易所 = ?"
        self._run_sql(sql, [exchange])
        
        logger.info(f"✅ 【数据库】成功清除持仓区{exchange}数据")
    
    # ==================== SQL执行基础方法 ====================
    
    def _run_sql(self, sql: str, params: List = None) -> Dict:
        """执行SQL语句"""
        if params is None:
            params = []
        
        args = []
        for p in params:
            if p is None:
                args.append({"type": "null", "value": None})
            else:
                args.append({"type": "text", "value": str(p)})
        
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
        
        try:
            response = requests.post(
                f"{self.url}/v2/pipeline",
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=10
            )
            
            if response.status_code != 200:
                try:
                    error_detail = response.json()
                    logger.error(f"❌ 【数据库】Turso返回错误: {error_detail}")
                except:
                    logger.error(f"❌ 【数据库】Turso返回错误状态码: {response.status_code}")
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"❌ 【数据库】请求失败: {e}")
            raise
    
    # ==================== 表名查询（精确解析版）====================
    
    def _get_tables(self) -> List[str]:
        """
        获取当前数据库中的所有表名 - 精确解析版
        ==================================================
        根据 Turso API 的标准返回格式精确解析，只返回真正的用户表。
        
        Turso API 返回格式示例：
        {
            "results": [
                {
                    "type": "ok",
                    "result": {
                        "cols": [...],
                        "rows": [
                            [{"type": "text", "value": "active_positions"}],
                            [{"type": "text", "value": "closed_positions"}],
                            [{"type": "text", "value": "sqlite_sequence"}]  # 系统表会被过滤
                        ]
                    }
                }
            ]
        }
        
        解析路径：results[0].result.rows[*][0].value
        ==================================================
        """
        sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        
        try:
            # 执行查询
            result = self._run_sql(sql)
            
            # 调试日志 - 只在DEBUG级别打印完整返回，避免INFO日志混乱
            logger.debug("🔍 【数据库调试】========== Turso原始返回 START ==========")
            logger.debug(json.dumps(result, ensure_ascii=False, indent=2))
            logger.debug("🔍 【数据库调试】========== Turso原始返回 END ==========")
            
            tables = []
            
            # 防御性编程：检查返回结果是否为字典
            if not isinstance(result, dict):
                logger.error(f"❌ 【数据库】查询表名返回不是字典，实际类型: {type(result)}")
                return tables
            
            # 步骤1：检查是否有 results 数组
            if 'results' not in result:
                logger.error("❌ 【数据库】返回结果中没有 'results' 字段")
                return tables
            
            results_list = result['results']
            if not isinstance(results_list, list) or len(results_list) == 0:
                logger.error("❌ 【数据库】'results' 不是数组或为空")
                return tables
            
            # 步骤2：取第一个结果对象（通常只有一个）
            first_result = results_list[0]
            if not isinstance(first_result, dict):
                logger.error("❌ 【数据库】results[0] 不是字典")
                return tables
            
            # 步骤3：检查是否有 result 字段
            if 'result' not in first_result:
                logger.error("❌ 【数据库】results[0] 中没有 'result' 字段")
                return tables
            
            result_data = first_result['result']
            if not isinstance(result_data, dict):
                logger.error("❌ 【数据库】result 字段不是字典")
                return tables
            
            # 步骤4：检查是否有 rows 数组
            if 'rows' not in result_data:
                logger.error("❌ 【数据库】result 中没有 'rows' 字段")
                return tables
            
            rows = result_data['rows']
            if not isinstance(rows, list):
                logger.error("❌ 【数据库】rows 不是数组")
                return tables
            
            # 步骤5：遍历每一行数据
            for row_index, row in enumerate(rows):
                if not isinstance(row, list) or len(row) == 0:
                    logger.debug(f"⏭️ 第 {row_index} 行格式不正确，跳过")
                    continue
                
                # 每一行的第一个单元格就是表名
                cell = row[0]
                if not isinstance(cell, dict):
                    logger.debug(f"⏭️ 第 {row_index} 行第一个单元格不是字典")
                    continue
                
                # 获取 value 字段，这就是表名
                if 'value' not in cell:
                    logger.debug(f"⏭️ 第 {row_index} 行单元格中没有 'value' 字段")
                    continue
                
                table_name = cell['value']
                
                # 过滤掉 SQLite 系统表（以 sqlite_ 开头的都是系统表）
                if table_name and isinstance(table_name, str) and not table_name.startswith('sqlite_'):
                    tables.append(table_name)
                    logger.debug(f"✅ 找到用户表: {table_name}")
            
            # 去重并排序（理论上不会重复，但为了安全还是去重）
            tables = list(set(tables))
            tables.sort()
            
            # 记录找到的表数量（只记录非空结果）
            if tables:
                logger.info(f"📋 【数据库】找到 {len(tables)} 个用户表: {tables}")
            else:
                logger.info("📋 【数据库】当前数据库中没有用户表")
            
            return tables
            
        except Exception as e:
            logger.error(f"❌ 【数据库】查询表名失败: {e}", exc_info=True)
            return []
    
    # ==================== 连接测试 ====================
    
    def test_connection(self) -> bool:
        """测试数据库连接是否正常"""
        try:
            result = self._run_sql("SELECT 1")
            
            # 精确验证连接是否成功
            if (result and 
                isinstance(result, dict) and 
                'results' in result and 
                len(result['results']) > 0 and
                'result' in result['results'][0]):
                logger.info("✅ 【数据库】连接测试成功")
                return True
            else:
                logger.error(f"❌ 【数据库】连接测试返回异常结果")
                return False
                
        except Exception as e:
            logger.error(f"❌ 【数据库】连接测试失败: {e}")
            return False
    
    # ==================== 初始化/建表 ====================
    
    def _init_database(self):
        """初始化数据库 - 确保必要的表存在"""
        try:
            # 先查表，记录建表前的状态（用于调试）
            tables_before = self._get_tables()
            if tables_before:
                logger.info(f"📋 【数据库】初始化前已有用户表: {tables_before}")
            else:
                logger.info("📋 【数据库】初始化前没有用户表，准备创建")
            
            # 建表（IF NOT EXISTS 保证安全，不会覆盖已有表）
            self._create_active_positions_table()
            self._create_closed_positions_table()
            self._create_indexes()
            
            # 验证持仓区表是否创建成功
            try:
                self._run_sql("SELECT COUNT(*) FROM active_positions LIMIT 1")
                logger.info("✅ 【数据库】持仓区表验证成功")
            except Exception as e:
                logger.error(f"❌ 【数据库】持仓区表验证失败: {e}")
                raise
            
            # 验证历史区表是否创建成功
            try:
                self._run_sql("SELECT COUNT(*) FROM closed_positions LIMIT 1")
                logger.info("✅ 【数据库】历史区表验证成功")
            except Exception as e:
                logger.error(f"❌ 【数据库】历史区表验证失败: {e}")
                raise
            
            # 再查表，看建表后的状态
            tables_after = self._get_tables()
            if tables_after:
                logger.info(f"📋 【数据库】初始化后所有用户表: {tables_after}")
            
            logger.info("✅ 【数据库】初始化完成")
            
        except Exception as e:
            logger.error(f"❌ 【数据库】初始化失败: {e}")
            raise
    
    def _create_active_positions_table(self):
        """创建持仓区表"""
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
        logger.debug("📝 【数据库】执行创建持仓表SQL")
    
    def _create_closed_positions_table(self):
        """创建历史区表"""
        sql = """
        CREATE TABLE IF NOT EXISTS closed_positions (
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
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        self._run_sql(sql)
        logger.debug("📝 【数据库】执行创建历史表SQL")
    
    def _create_indexes(self):
        """创建索引以提高查询性能"""
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
                logger.debug(f"📝 【数据库】索引创建/已存在: {sql[:40]}...")
            except Exception as e:
                logger.warning(f"⚠️ 【数据库】索引创建失败: {e}")