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
   - 幂等性：根据id判断首次写入还是静默更新（2024-03-11新增）

2. closed_positions（历史表）
   - 作用：永久保存所有已平仓记录
   - 特点：追加写入，永不删除，永不覆盖
   - id生成：交易所_合约名_平仓时间
   - 幂等性：根据id去重，避免20秒倒计时期间的重复写入（2024-03-11修复）

【调用关系】
调度器 (scheduler.py) 
    ↓ 推送 {tag, data}
数据库 (database.py) 
    ↓ 根据tag执行不同逻辑
Turso数据库

【重要原则 - 2024-03-11 最终修复】
Turso API 要求所有值都必须是字符串类型
所以不管来的是什么类型的数据，都统一转成字符串

【修复历史】
- 2024-03-11：修复历史区id为空问题，增加id去重
- 2024-03-11：修复持仓区日志刷屏问题，首次写入才打印日志
- 2024-03-11：优化持仓区日志，同ID只打印一次成功日志，之后完全静默
- 2024-03-11：添加收到数据日志的时间控制，每分钟最多打印一条
==================================================
"""

import os
import requests
import logging
import json
import time
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
        做了六件事：
        1. 从环境变量读取数据库URL和令牌
        2. 验证配置是否存在
        3. 测试数据库连接是否正常
        4. 初始化数据库表（如果表不存在就创建）
        5. 初始化日志记录集合（用于控制日志输出）
        6. 初始化最后日志时间记录（用于控制收到数据日志频率）
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
        
        logger.info("✅ 【数据库】数据库配置加载成功")
        
        # ----- 第3步：测试数据库连接 -----
        if not self.test_connection():
            raise ConnectionError("❌【数据库】 无法连接到数据库，请检查网络和令牌")
        
        # ----- 第4步：初始化数据库表 -----
        self._init_database()
        
        # ----- 第5步：初始化日志记录集合 -----
        self._logged_active_ids = set()
        logger.info("✅【数据库】 日志控制集合初始化完成")
        
        # ----- 第6步：初始化最后日志时间记录 -----
        self._last_log_time = 0  # 上一次打印收到数据日志的时间戳
        self._log_interval = 60  # 日志打印间隔（秒）
        logger.info(f"✅【数据库】 日志时间控制初始化完成，间隔{self._log_interval}秒")
    
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
            
        【日志控制 - 2024-03-11新增】
        - 收到数据日志每分钟最多打印一条，避免刷屏
        - 使用时间戳控制，精确到秒
        ==================================================
        
        :param tag: 数据标签（已平仓/持仓完整）
        :param data: 完整的原始数据（字段全是中文）
        """
        try:
            # ----- 第1步：从数据中提取交易所信息 -----
            exchange = data.get('交易所')
            if not exchange:
                logger.error("❌【数据库】 数据中没有'交易所'字段，无法处理")
                return
            
            # ----- 第2步：根据标签执行不同逻辑 -----
            if tag == '已平仓':
                # 已平仓数据一直打印，因为频率低
                logger.info(f"📦【数据库】 收到已平仓数据: {exchange}")
                await self._handle_closed(data, exchange)
                
            elif tag == '持仓完整':
                contract = data.get('开仓合约名', 'unknown')
                
                # ========== 【新增】时间控制：每分钟最多打印一条 ==========
                current_time = time.time()
                time_since_last_log = current_time - self._last_log_time
                
                if time_since_last_log >= self._log_interval:
                    # 距离上次打印已超过间隔，打印日志
                    logger.info(f"📦【数据库】 收到持仓完整数据: {exchange} - {contract}")
                    self._last_log_time = current_time
                else:
                    # 间隔内，只打印debug日志
                    logger.debug(f"📦【数据库】 收到持仓完整数据: {exchange} - {contract} (已抑制)")
                
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
        1. 先写入历史表（永久保存，带幂等性保护）
        2. 再清理持仓表（只清理这个交易所）
        
        日志输出：
            - 成功写入历史区欧易数据 / 成功写入历史区币安数据
            - 成功清除持仓区欧易数据 / 成功清除持仓区币安数据
            - 或跳过写入（如果id已存在）
        ==================================================
        
        :param data: 已平仓的完整数据
        :param exchange: 交易所名称（binance/okx）
        """
        # ----- 第1步：写入历史表（追加，带幂等性保护）-----
        # 已平仓数据永久保存到历史区，id=交易所_合约名_平仓时间
        await self._insert_closed_position(data)
        
        # ----- 第2步：清理持仓表（只清理该交易所）-----
        # 该交易所的持仓已平仓，从活跃持仓表中移除
        await self._delete_active_position(exchange)
        
        logger.info(f"✅【数据库】 已平仓处理完成: {exchange}")
    
    async def _handle_active(self, data: Dict[str, Any]):
        """
        处理持仓完整数据
        ==================================================
        做一件事：覆盖更新到持仓区（带日志控制）
        ==================================================
        
        :param data: 持仓完整的完整数据
        """
        await self._save_active_position(data)
    
    # ==================== 实际数据库操作 ====================
    
    async def _save_active_position(self, data: Dict[str, Any]):
        """
        持仓区：覆盖更新（日志控制版本）
        ==================================================
        【日志控制逻辑 - 2024-03-11 优化版】
        - 同一个ID只打印一次"首次写入"的成功日志
        - 之后相同ID的更新完全不打印任何日志（完全静默）
        
        【实现方式】
        使用内存集合 _logged_active_ids 记录已打印日志的ID
        ==================================================
        
        :param data: 数据字典，key必须全是中文
        """
        # ----- 第1步：确保数据有id字段 -----
        if 'id' not in data or not data['id']:
            exchange = data.get('交易所', 'unknown')
            contract = data.get('开仓合约名', 'unknown')
            open_time = data.get('开仓时间', '')
            data['id'] = f"{exchange}_{contract}_{open_time}"
            logger.debug(f"🔑 【数据库】持仓表生成id: {data['id']}")
        
        record_id = data['id']
        exchange = data.get('交易所', 'unknown')
        contract = data.get('开仓合约名', 'unknown')
        
        # ========== 判断这个ID是否已经打印过日志 ==========
        should_log_info = record_id not in self._logged_active_ids
        
        # ----- 第2步：构建SQL语句 -----
        fields = list(data.keys())
        placeholders = ','.join(['?' for _ in fields])
        values = [data.get(f) for f in fields]
        
        sql = f"""
            INSERT OR REPLACE INTO active_positions 
            ({','.join(fields)}) 
            VALUES ({placeholders})
        """
        
        logger.debug(f"📝 【数据库】持仓表 SQL: {sql}")
        logger.debug(f"📝【数据库】 持仓表 值: {values}")
        
        # ----- 第3步：执行SQL -----
        self._run_sql(sql, values)
        
        # ========== 只对首次写入打印日志 ==========
        if should_log_info:
            # 第一次遇到这个ID，打印info日志，并记录到集合
            logger.info(f"✅【数据库】 成功写入持仓区{exchange}数据 - {contract}（首次）")
            self._logged_active_ids.add(record_id)
        # 非首次写入：完全静默，不打印任何日志
    
    def _check_active_exists_by_id(self, record_id: str) -> bool:
        """
        根据 id 检查持仓记录是否已存在（同步方法）
        ==================================================
        用于日志控制，判断是首次写入还是覆盖更新
        
        【注意】这是同步方法，不需要async/await
        因为底层_run_sql也是同步的（使用requests）
        
        :param record_id: 记录唯一标识（交易所_合约名_开仓时间）
        :return: True=已存在（覆盖更新），False=不存在（首次写入）
        """
        if not record_id:
            return False
        
        sql = "SELECT 1 FROM active_positions WHERE id = ? LIMIT 1"
        
        try:
            result = self._run_sql(sql, [record_id])
            if result and 'results' in result:
                rows = result['results'][0].get('result', {}).get('rows', [])
                exists = len(rows) > 0
                if exists:
                    logger.debug(f"🔍【数据库】 持仓表发现已存在id: {record_id}")
                return exists
            return False
        except Exception as e:
            logger.error(f"❌【数据库】 检查持仓记录存在性失败: {e}")
            return False  # 出错时假设不存在，允许打印首次写入日志
    
    async def _insert_closed_position(self, data: Dict[str, Any]):
        """
        历史区：追加写入（带幂等性保护）
        ==================================================
        【防重复逻辑 - 2024-03-11新增】
        使用 id 字段（交易所_合约名_平仓时间）作为唯一键
        写入前检查是否已存在相同 id，避免20秒倒计时期间的重复数据
        
        【id 生成规则】
        历史区 id = 交易所_合约名_平仓时间
        例如：okx_BTCUSDT_2026.03.11 18:17:00
        【注意】与持仓区不同，持仓区用的是开仓时间！
        
        【去重判断】
        - 如果 id 已存在 → 跳过写入（重复数据），输出日志：⏭️ 历史区已存在记录，跳过写入
        - 如果 id 不存在 → 正常写入（新记录），输出日志：✅ 成功写入历史区...
        
        【关键修复】2024-03-11 19:49
        修复了去重不生效的问题：_check_exists_by_id改为同步方法，调用时去掉await
        ==================================================
        
        :param data: 数据字典，key必须全是中文
        """
        # ========== 第1步：确保数据有 id 字段（用平仓时间！）==========
        if 'id' not in data or not data['id']:
            exchange = data.get('交易所', 'unknown')
            contract = data.get('开仓合约名', 'unknown')
            # 【关键】历史区用平仓时间，不是开仓时间！
            close_time = data.get('平仓时间', '')
            data['id'] = f"{exchange}_{contract}_{close_time}"
            logger.debug(f"🔑 【数据库】历史表生成id: {data['id']}")
        
        record_id = data['id']
        
        # ========== 第2步：检查是否已存在相同 id（幂等性保护）==========
        # 【关键修复】去掉await，因为_check_exists_by_id是普通方法
        if self._check_exists_by_id(record_id):
            logger.info(f"⏭️【数据库】 历史区已存在记录，跳过写入: {record_id}")
            return
        
        # ========== 第3步：构建SQL并写入 ==========
        fields = list(data.keys())
        placeholders = ','.join(['?' for _ in fields])
        values = [data.get(f) for f in fields]
        
        # 【修复】INSERT → INSERT OR REPLACE，实现去重
        sql = f"INSERT OR REPLACE INTO closed_positions ({','.join(fields)}) VALUES ({placeholders})"
        
        logger.debug(f"📝 历史表 SQL: {sql}")
        logger.debug(f"📝 历史表 值: {values}")
        
        self._run_sql(sql, values)
        
        # ========== 第4步：输出成功日志 ==========
        exchange = data.get('交易所', 'unknown')
        contract = data.get('开仓合约名', 'unknown')
        close_time = data.get('平仓时间', 'unknown')
        logger.info(f"✅【数据库】 成功写入历史区{exchange}数据 - {contract} 平仓时间:{close_time}")
    
    def _check_exists_by_id(self, record_id: str) -> bool:
        """
        根据 id 检查历史记录是否已存在（同步方法）
        ==================================================
        用于幂等性保护，避免重复写入
        
        【关键修复】2024-03-11 19:49
        改为普通同步方法（去掉async），因为底层_run_sql是同步的
        调用时不需要await，否则判断总是返回False，导致重复写入
        
        :param record_id: 记录唯一标识（交易所_合约名_平仓时间）
        :return: True=已存在，False=不存在
        """
        if not record_id:
            return False  # id为空无法判断，允许写入
        
        sql = "SELECT 1 FROM closed_positions WHERE id = ? LIMIT 1"
        
        try:
            result = self._run_sql(sql, [record_id])
            
            if result and 'results' in result:
                rows = result['results'][0].get('result', {}).get('rows', [])
                exists = len(rows) > 0
                if exists:
                    logger.debug(f"🔍【数据库】 历史表发现重复记录 id: {record_id}")
                return exists
                
            return False
            
        except Exception as e:
            logger.error(f"❌ 检查历史记录存在性失败: {e}")
            # 出错时默认允许写入（避免阻塞正常流程）
            return False
    
    async def _delete_active_position(self, exchange: str):
        """
        清理持仓区 - 只清理指定交易所
        ==================================================
        重要警告：
            必须传入exchange参数，绝对不能删除所有持仓
        
        使用场景：
            - 已平仓时清理对应交易所的持仓记录
        
        日志输出：
            - 成功清除持仓区{交易所}数据
        ==================================================
        
        :param exchange: 'binance' 或 'okx'，不能为空
        """
        if not exchange:
            logger.error("❌【数据库】 清理持仓必须传入交易所参数，本次操作已取消")
            return
        
        sql = "DELETE FROM active_positions WHERE 交易所 = ?"
        self._run_sql(sql, [exchange])
        
        # 输出成功日志
        logger.info(f"✅【数据库】 成功清除持仓区{exchange}数据")
    
    # ==================== SQL执行基础方法 ====================
    # 【最终修复】2024-03-11 - 强制所有值转字符串
    
    def _run_sql(self, sql: str, params: List = None) -> Dict:
        """
        执行SQL语句 - 最终修复版本
        ==================================================
        【问题】Turso API要求所有值都必须是字符串类型
        【解决方案】不管来的是什么类型，全部转成字符串
        
        转换规则：
            - None → {"type": "null", "value": None}
            - 其他所有值 → {"type": "text", "value": str(值)}
        
        例如：
            - 整数 20 → {"type": "text", "value": "20"}
            - 浮点数 69761.2 → {"type": "text", "value": "69761.2"}
            - 字符串 "okx" → {"type": "text", "value": "okx"}
        ==================================================
        
        :param sql: SQL语句（字段名用中文）
        :param params: 参数列表
        :return: Turso API返回的原始结果
        """
        if params is None:
            params = []
        
        # ========== 【调试代码】如需详细调试，取消下面的注释 ==========
        # logger.info("=" * 60)
        # logger.info("🔍【调试】开始执行SQL，打印所有原始参数")
        # logger.info(f"📝 SQL: {sql}")
        # logger.info(f"📊 参数总数: {len(params)}")
        # 
        # for i, p in enumerate(params):
        #     logger.info(f"  🔹 参数[{i}]: 值={p}, 类型={type(p).__name__}")
        # 
        # logger.info("🔄【调试】强制转换所有值为字符串...")
        # ========== 【调试代码结束】 ==========
        
        # ========== 强制全部转字符串 ==========
        args = []
        
        for idx, p in enumerate(params):
            # 【调试代码】如需查看每个参数的转换过程，取消下面的注释
            # logger.info(f"  🔸 处理参数[{idx}]: 原始值={p}, 原始类型={type(p).__name__}")
            
            if p is None:
                # None值保持为null
                args.append({"type": "null", "value": None})
                # 【调试代码】logger.info(f"    ✅ 【数据库】转换结果: null")
            else:
                # 所有非空值强制转字符串
                str_value = str(p)
                args.append({"type": "text", "value": str_value})
                # 【调试代码】logger.info(f"    ✅ 【数据库】强制转字符串: '{str_value}'")
        
        # ========== 【调试代码】打印转换统计 ==========
        # logger.info(f"📊 【数据库】参数转换完成: 共 {len(args)} 个参数")
        # ========== 【调试代码结束】 ==========
        
        # ========== 构建请求体 ==========
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
        
        # ========== 【调试代码】打印请求体预览 ==========
        # logger.info("📤【调试】发送到Turso的请求体预览:")
        # try:
        #     args_summary = []
        #     for i, arg in enumerate(args):
        #         args_summary.append(f"参数[{i}]: {arg}")
        #     logger.info(f"参数列表: {args_summary}")
        # except Exception as e:
        #     logger.error(f"无法打印参数预览: {e}")
        # ========== 【调试代码结束】 ==========
        
        # ========== 发送HTTP请求 ==========
        try:
            # 【调试代码】如需查看请求发送过程，取消下面的注释
            # logger.info("📡【调试】【数据库】发送HTTP请求到Turso...")
            
            response = requests.post(
                f"{self.url}/v2/pipeline",
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=10
            )
            
            # 【调试代码】打印响应状态
            # logger.info(f"📥【调试】【数据库】收到响应: 状态码={response.status_code}")
            
            if response.status_code != 200:
                try:
                    error_detail = response.json()
                    logger.error(f"❌ 错误详情: {error_detail}")
                except:
                    logger.error(f"❌ 响应内容: {response.text[:500]}")
            
            response.raise_for_status()
            
            # 【调试代码】logger.info("✅【调试】【数据库】请求成功")
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.error("❌ 数据库请求超时")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ 数据库请求失败: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ 【数据库】未知错误: {e}")
            raise
        # finally:
        #     【调试代码】logger.info("=" * 60)
    
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
                logger.info("✅ 【数据库】创建持仓区表 active_positions")
            
            if 'closed_positions' not in tables:
                self._create_closed_positions_table()
                logger.info("✅ 【数据库】创建历史区表 closed_positions")
            
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
        创建持仓区表
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
        创建历史区表
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
                logger.debug(f"✅ 【数据库】索引创建/已存在: {sql[:40]}...")
            except Exception as e:
                logger.warning(f"⚠️【数据库】 索引创建失败（可能已存在）: {e}")