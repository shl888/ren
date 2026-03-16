#!/usr/bin/env python3
"""
真·蚂蚁基因检测器 v2.0
======================
检测范围：
🔴 致命基因（立即卡死系统）
🟠 高危基因（可能卡死）
🟡 中危基因（性能下降）
🟢 低危基因（建议优化）

不检测误报项（如普通for循环）
"""

import os
import re
import ast
import sys
import json
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Set
import aiofiles


class RealAntDetector:
    """真·蚂蚁基因检测器"""
    
    def __init__(self, root_path: str):
        self.root = Path(root_path)
        self.results = {}
        
        # 🔴 致命基因模式（立即卡死）
        self.fatal_patterns = {
            'threading_lock_in_async': {
                'pattern': r'async\s+def\s+\w+.*?:.*?(?=async\s+def|\Z)',
                'check': 'threading.Lock',
                'name': '异步函数中使用threading.Lock',
                'fix': '改为 asyncio.Lock()',
                'severity': '🔴 致命'
            },
            'sync_sleep_in_async': {
                'pattern': r'async\s+def\s+\w+.*?:.*?(?=async\s+def|\Z)',
                'check': r'time\.sleep\(([^)]+)\)',
                'name': '异步函数中使用time.sleep',
                'fix': '改为 await asyncio.sleep()',
                'severity': '🔴 致命'
            },
            'blocking_io_in_async': {
                'pattern': r'async\s+def\s+\w+.*?:.*?(?=async\s+def|\Z)',
                'check': r'(requests\.|urllib\.|http\.client|socket\.)',
                'name': '异步函数中使用同步网络IO',
                'fix': '改为 aiohttp 或 httpx',
                'severity': '🔴 致命'
            },
            'sync_file_in_async': {
                'pattern': r'async\s+def\s+\w+.*?:.*?(?=async\s+def|\Z)',
                'check': r'open\(|with\s+open\(|\.read\(|\.write\(',
                'name': '异步函数中使用同步文件IO',
                'fix': '改为 aiofiles',
                'severity': '🔴 致命'
            },
            'infinite_while_no_sleep': {
                'pattern': r'while\s+(True|1)\s*:.*?(?=^\s*(def|class|while|for|if|@)\s|\Z)',
                'check': r'await\s+asyncio\.sleep',
                'name': '无限循环无asyncio.sleep',
                'fix': '循环内添加 await asyncio.sleep(0)',
                'severity': '🔴 致命',
                'negative': True  # 检查是否不存在
            },
            'create_task_in_init': {
                'pattern': r'def\s+__init__\s*\(.*?\).*?:.*?(?=^\s*(def|class)\s|\Z)',
                'check': r'asyncio\.create_task|asyncio\.ensure_future',
                'name': '__init__中启动后台任务',
                'fix': '改为外部显式启动',
                'severity': '🔴 致命'
            }
        }
        
        # 🟠 高危基因（可能卡死）
        self.high_patterns = {
            'run_in_executor_no_timeout': {
                'pattern': r'run_in_executor\(',
                'check': r'timeout',
                'name': 'run_in_executor无超时',
                'fix': '添加 asyncio.wait_for(timeout=...)',
                'severity': '🟠 高危',
                'negative': True
            },
            'thread_pool_no_max_workers': {
                'pattern': r'ThreadPoolExecutor\(',
                'check': r'max_workers',
                'name': 'ThreadPoolExecutor无max_workers',
                'fix': '设置 max_workers=5-10',
                'severity': '🟠 高危',
                'negative': True
            },
            'gather_no_timeout': {
                'pattern': r'asyncio\.gather\(',
                'check': r'wait_for|timeout',
                'name': 'asyncio.gather无超时',
                'fix': '使用 asyncio.wait_for(asyncio.gather(...), timeout=...)',
                'severity': '🟠 高危',
                'negative': True
            },
            'queue_no_maxsize': {
                'pattern': r'asyncio\.Queue\(',
                'check': r'maxsize',
                'name': 'Queue无maxsize限制',
                'fix': '设置 maxsize=1000-10000',
                'severity': '🟠 高危',
                'negative': True
            },
            'lock_no_timeout': {
                'pattern': r'async\s+with\s+\w*lock',
                'check': r'timeout',
                'name': '锁无超时',
                'fix': '使用 async with lock.acquire(timeout=...)',
                'severity': '🟠 高危',
                'negative': True
            }
        }
        
        # 🟡 中危基因（性能下降）
        self.medium_patterns = {
            'sleep_too_short': {
                'pattern': r'await\s+asyncio\.sleep\(([^)]+)\)',
                'check': r'0\.0[0-9]+|0\.00[0-9]+',
                'name': 'sleep时间过短(<0.01s)',
                'fix': '改为 await asyncio.sleep(0.01) 或更大',
                'severity': '🟡 中危'
            },
            'tight_loop_no_yield': {
                'pattern': r'while\s+.*?:.*?(?=^\s*(def|class|while|for|if|@)\s|\Z)',
                'check': r'await\s+asyncio\.sleep\(0\)|await\s+asyncio\.sleep\(0\.',
                'name': '循环无主动让出',
                'fix': '添加 await asyncio.sleep(0)',
                'severity': '🟡 中危',
                'negative': True
            },
            'blocking_db_in_async': {
                'pattern': r'async\s+def\s+\w+.*?:.*?(?=async\s+def|\Z)',
                'check': r'(sqlite3|pymysql|psycopg2|redis)\.',
                'name': '异步函数中使用同步数据库',
                'fix': '改为 aiomysql/aiopg/aioredis',
                'severity': '🟡 中危'
            }
        }
        
        # 🟢 低危基因（建议优化）
        self.low_patterns = {
            'sync_logging_in_async': {
                'pattern': r'async\s+def\s+\w+.*?:.*?(?=async\s+def|\Z)',
                'check': r'logging\.\w+\(|logger\.\w+\(',
                'name': '异步函数中使用同步日志',
                'fix': '使用 aiologger 或结构化的异步日志',
                'severity': '🟢 低危'
            },
            'heavy_computation_in_async': {
                'pattern': r'async\s+def\s+\w+.*?:.*?(?=async\s+def|\Z)',
                'check': r'(for\s+\w+\s+in\s+range\([^)]{10,}\)|while\s+.*?:)',
                'name': '异步函数中大量计算',
                'fix': '使用 loop.run_in_executor()',
                'severity': '🟢 低危'
            }
        }
        
        # 合并所有模式
        self.all_patterns = {}
        self.all_patterns.update(self.fatal_patterns)
        self.all_patterns.update(self.high_patterns)
        self.all_patterns.update(self.medium_patterns)
        self.all_patterns.update(self.low_patterns)
    
    async def scan_file(self, file_path: Path) -> Dict:
        """扫描单个文件"""
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = await f.read()
            
            genes = []
            lines = content.split('\n')
            
            # 按函数/方法分割代码块
            blocks = self._split_into_blocks(content)
            
            for pattern_name, pattern_info in self.all_patterns.items():
                matches = self._check_pattern(content, blocks, pattern_info)
                for match in matches:
                    line_no = content[:match['start']].count('\n') + 1
                    line_content = lines[line_no - 1].strip() if line_no <= len(lines) else ''
                    
                    genes.append({
                        'type': pattern_name,
                        'name': pattern_info['name'],
                        'severity': pattern_info['severity'],
                        'line': line_no,
                        'code': line_content[:80],
                        'fix': pattern_info['fix'],
                        'context': self._get_context(lines, line_no)
                    })
            
            return {
                'file': str(file_path.relative_to(self.root)),
                'total_lines': len(lines),
                'genes': genes,
                'fatal_count': len([g for g in genes if '🔴' in g['severity']]),
                'high_count': len([g for g in genes if '🟠' in g['severity']]),
                'medium_count': len([g for g in genes if '🟡' in g['severity']]),
                'low_count': len([g for g in genes if '🟢' in g['severity']])
            }
            
        except Exception as e:
            return {
                'file': str(file_path.relative_to(self.root)),
                'error': str(e),
                'genes': []
            }
    
    def _split_into_blocks(self, content: str) -> List[Tuple[str, int]]:
        """将代码分割成函数/方法块"""
        blocks = []
        lines = content.split('\n')
        current_block = []
        current_start = 0
        indent_level = 0
        
        for i, line in enumerate(lines):
            stripped = line.lstrip()
            
            # 检测函数/类定义
            if re.match(r'^\s*(async\s+def|def|class)\s+', line):
                if current_block:
                    blocks.append(('\n'.join(current_block), current_start))
                current_block = [line]
                current_start = i
                indent_level = len(line) - len(line.lstrip())
            elif current_block:
                # 检测块结束（缩进回到定义级别）
                line_indent = len(line) - len(line.lstrip())
                if stripped and line_indent <= indent_level and i > current_start + 1:
                    blocks.append(('\n'.join(current_block), current_start))
                    current_block = []
                else:
                    current_block.append(line)
        
        if current_block:
            blocks.append(('\n'.join(current_block), current_start))
        
        return blocks
    
    def _check_pattern(self, content: str, blocks: List[Tuple[str, int]], pattern_info: Dict) -> List[Dict]:
        """检查特定模式"""
        matches = []
        is_negative = pattern_info.get('negative', False)
        
        # 如果是块级模式（如async def内部检查）
        if 'pattern' in pattern_info and pattern_info['pattern']:
            for block, start_line in blocks:
                # 检查块是否匹配主模式
                if re.search(pattern_info['pattern'], block, re.DOTALL):
                    # 检查是否包含/不包含检查项
                    found = re.search(pattern_info['check'], block, re.DOTALL)
                    
                    if is_negative and not found:  # 应该存在但不存在
                        matches.append({
                            'start': sum(len(l) + 1 for l in content.split('\n')[:start_line]),
                            'block': block[:200]
                        })
                    elif not is_negative and found:  # 存在且找到
                        # 找到具体位置
                        for m in re.finditer(pattern_info['check'], block):
                            abs_pos = sum(len(l) + 1 for l in content.split('\n')[:start_line])
                            abs_pos += m.start()
                            matches.append({
                                'start': abs_pos,
                                'match': m.group()
                            })
        else:
            # 全局模式
            for m in re.finditer(pattern_info['check'], content):
                matches.append({
                    'start': m.start(),
                    'match': m.group()
                })
        
        return matches
    
    def _get_context(self, lines: List[str], line_no: int, context: int = 3) -> str:
        """获取代码上下文"""
        start = max(0, line_no - context - 1)
        end = min(len(lines), line_no + context)
        return '\n'.join(f"{i+1:4d}: {lines[i]}" for i in range(start, end))
    
    async def scan_directory(self):
        """扫描整个目录"""
        py_files = list(self.root.rglob("*.py"))
        total = len(py_files)
        
        print(f"🔍 开始扫描: {self.root}")
        print(f"📊 找到 {total} 个Python文件")
        print("=" * 70)
        
        # 并发扫描
        semaphore = asyncio.Semaphore(10)  # 限制并发
        
        async def scan_with_limit(file_path):
            async with semaphore:
                return await self.scan_file(file_path)
        
        tasks = [scan_with_limit(f) for f in py_files]
        results = await asyncio.gather(*tasks)
        
        # 过滤有问题的文件
        self.results = {
            r['file']: r for r in results 
            if r.get('genes') or r.get('error')
        }
        
        return self.results
    
    def generate_report(self) -> str:
        """生成检测报告"""
        lines = [
            "=" * 80,
            "🐜 真·蚂蚁基因检测报告",
            "=" * 80,
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"扫描目录: {self.root}",
            "",
            "📊 威胁等级说明:",
            "  🔴 致命 - 立即卡死事件循环，必须立即修复",
            "  🟠 高危 - 可能卡死系统，建议24小时内修复", 
            "  🟡 中危 - 性能显著下降，建议本周修复",
            "  🟢 低危 - 轻微影响，建议优化",
            "",
            "=" * 80,
        ]
        
        # 统计
        all_genes = []
        for r in self.results.values():
            all_genes.extend(r.get('genes', []))
        
        fatal = len([g for g in all_genes if '🔴' in g['severity']])
        high = len([g for g in all_genes if '🟠' in g['severity']])
        medium = len([g for g in all_genes if '🟡' in g['severity']])
        low = len([g for g in all_genes if '🟢' in g['severity']])
        
        lines.extend([
            "📈 总体统计:",
            f"  扫描文件: {len(self.results)} 个有问题",
            f"  🔴 致命基因: {fatal} 处",
            f"  🟠 高危基因: {high} 处", 
            f"  🟡 中危基因: {medium} 处",
            f"  🟢 低危基因: {low} 处",
            "",
            "=" * 80,
            "🔥 致命基因文件（立即处理）:",
            "-" * 80,
        ])
        
        # 按致命程度排序
        fatal_files = [
            (f, r) for f, r in self.results.items()
            if any('🔴' in g['severity'] for g in r.get('genes', []))
        ]
        fatal_files.sort(key=lambda x: sum(
            1 for g in x[1].get('genes', []) if '🔴' in g['severity']
        ), reverse=True)
        
        for file_path, result in fatal_files:
            fatal_count = sum(1 for g in result['genes'] if '🔴' in g['severity'])
            lines.append(f"\n🔴 {file_path} ({fatal_count}处致命)")
            for gene in result['genes']:
                if '🔴' in gene['severity']:
                    lines.extend([
                        f"  📍 第{gene['line']}行: {gene['name']}",
                        f"     代码: {gene['code'][:60]}",
                        f"     修复: {gene['fix']}",
                        ""
                    ])
        
        # 其他级别简要列出
        if high > 0:
            lines.extend([
                "=" * 80,
                "🟠 高危基因文件:",
                "-" * 80,
            ])
            for file_path, result in self.results.items():
                high_genes = [g for g in result.get('genes', []) if '🟠' in g['severity']]
                if high_genes:
                    lines.append(f"\n🟠 {file_path} ({len(high_genes)}处)")
                    for gene in high_genes[:3]:  # 只显示前3个
                        lines.append(f"  第{gene['line']}行: {gene['name']}")
        
        lines.extend([
            "",
            "=" * 80,
            "✅ 修复优先级:",
            "1. 立即修复所有 🔴 致命基因",
            "2. 24小时内修复 🟠 高危基因", 
            "3. 本周修复 🟡 中危基因",
            "4. 有空优化 🟢 低危基因",
            "",
            "💡 快速修复命令:",
            "# 安装异步替代库",
            "pip install aiohttp aiofiles aiomysql",
            "",
            "# 常见替换:",
            "# threading.Lock() → asyncio.Lock()",
            "# time.sleep() → await asyncio.sleep()",
            "# requests.get() → aiohttp.ClientSession().get()",
            "# open() → aiofiles.open()",
            "=" * 80,
        ])
        
        return '\n'.join(lines)


async def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("用法: python real_ant_detector.py <扫描目录>")
        print("示例: python real_ant_detector.py ./my_project")
        sys.exit(1)
    
    root_path = sys.argv[1]
    detector = RealAntDetector(root_path)
    
    await detector.scan_directory()
    report = detector.generate_report()
    
    # 输出到控制台
    print(report)
    
    # 保存到文件
    output_file = f"真蚂蚁检测报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
        await f.write(report)
    
    print(f"\n📄 报告已保存: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
