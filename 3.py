#!/usr/bin/env python3
"""
真·蚂蚁基因检测器 v3.0 - 全面版
==================================
改进：
1. 精确检测类中的异步方法
2. 检测 threading.Lock 的定义和使用位置
3. 区分 time.sleep 在异步函数 vs 独立线程
4. 检测 run_in_executor 的阻塞风险
5. 检测后台循环和锁竞争
"""

import os
import re
import ast
import sys
import json
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass


@dataclass
class Gene:
    file: str
    line: int
    code: str
    name: str
    severity: str
    fix: str
    context: str = ""


class ASTAnalyzer(ast.NodeVisitor):
    """AST分析器 - 精确检测异步函数中的同步操作"""
    
    def __init__(self, source: str, filename: str):
        self.source = source
        self.filename = filename
        self.lines = source.split('\n')
        self.genes: List[Gene] = []
        self.current_function = None
        self.is_async = False
        self.lock_vars: Set[str] = set()  # 记录的锁变量名
        self.thread_vars: Set[str] = set()  # 记录的线程变量名
        
    def visit_ClassDef(self, node):
        """分析类定义"""
        for item in node.body:
            if isinstance(item, ast.FunctionDef) or isinstance(item, ast.AsyncFunctionDef):
                self.visit(item)
                
    def visit_AsyncFunctionDef(self, node):
        """分析异步函数"""
        old_function = self.current_function
        old_async = self.is_async
        
        self.current_function = node.name
        self.is_async = True
        
        # 检查函数体内的所有语句
        for stmt in ast.walk(node):
            self._check_async_body(stmt, node.lineno)
        
        self.generic_visit(node)
        
        self.current_function = old_function
        self.is_async = old_async
        
    def visit_FunctionDef(self, node):
        """分析普通函数（检查是否在独立线程中）"""
        old_function = self.current_function
        old_async = self.is_async
        
        self.current_function = node.name
        self.is_async = False
        
        # 检查是否是线程启动函数
        if any(dec.id == 'staticmethod' for dec in node.decorator_list if isinstance(dec, ast.Name)):
            pass
            
        self.generic_visit(node)
        
        self.current_function = old_function
        self.is_async = old_async
        
    def visit_Assign(self, node):
        """检测锁的定义"""
        for target in node.targets:
            if isinstance(target, ast.Attribute):
                if target.attr == '_lock' or target.attr == 'lock':
                    if isinstance(node.value, ast.Call):
                        if isinstance(node.value.func, ast.Attribute):
                            if node.value.func.attr == 'Lock':
                                self.lock_vars.add(target.attr)
                        elif isinstance(node.value.func, ast.Name):
                            if node.value.func.id == 'Lock':
                                self.lock_vars.add('lock')
                                
        self.generic_visit(node)
        
    def visit_With(self, node):
        """检测 with self._lock: 在异步函数中"""
        if self.is_async:
            for item in node.items:
                ctx = item.context_expr
                if isinstance(ctx, ast.Attribute):
                    if ctx.attr in self.lock_vars or 'lock' in ctx.attr.lower():
                        self._add_gene(
                            node.lineno,
                            f"异步函数中使用 {self._get_source(node)}",
                            "🔴 致命",
                            "threading.Lock 在 async def 中会阻塞事件循环",
                            "改为 asyncio.Lock()"
                        )
                elif isinstance(ctx, ast.Name):
                    if ctx.id in self.lock_vars or 'lock' in ctx.id.lower():
                        self._add_gene(
                            node.lineno,
                            f"异步函数中使用 {self._get_source(node)}",
                            "🔴 致命",
                            "threading.Lock 在 async def 中会阻塞事件循环",
                            "改为 asyncio.Lock()"
                        )
        self.generic_visit(node)
        
    def visit_Call(self, node):
        """检测函数调用"""
        if self.is_async:
            # 检测 time.sleep
            if isinstance(node.func, ast.Attribute):
                if node.func.attr == 'sleep':
                    if isinstance(node.func.value, ast.Name):
                        if node.func.value.id == 'time':
                            self._add_gene(
                                node.lineno,
                                f"time.sleep({self._get_args(node)})",
                                "🔴 致命",
                                "异步函数中使用 time.sleep 会阻塞事件循环",
                                "改为 await asyncio.sleep()"
                            )
            elif isinstance(node.func, ast.Name):
                if node.func.id == 'sleep':
                    # 检查是否是 time.sleep
                    self._add_gene(
                        node.lineno,
                        f"sleep({self._get_args(node)})",
                        "🔴 致命",
                        "可能的 time.sleep，会阻塞事件循环",
                        "改为 await asyncio.sleep()"
                    )
                    
            # 检测 requests
            if isinstance(node.func, ast.Attribute):
                if node.func.attr in ['get', 'post', 'put', 'delete', 'request']:
                    if isinstance(node.func.value, ast.Name):
                        if node.func.value.id == 'requests':
                            self._add_gene(
                                node.lineno,
                                f"requests.{node.func.attr}()",
                                "🔴 致命",
                                "异步函数中使用同步 requests 会阻塞事件循环",
                                "改为 aiohttp"
                            )
                            
            # 检测 open
            if isinstance(node.func, ast.Name):
                if node.func.id == 'open':
                    self._add_gene(
                        node.lineno,
                        f"open({self._get_args(node)})",
                        "🔴 致命",
                        "异步函数中使用同步 open 会阻塞事件循环",
                        "改为 aiofiles.open()"
                    )
                    
            # 检测 create_task 在特定位置
            if isinstance(node.func, ast.Attribute):
                if node.func.attr == 'create_task':
                    # 检查是否在 __init__ 中
                    if self.current_function == '__init__':
                        self._add_gene(
                            node.lineno,
                            "asyncio.create_task()",
                            "🔴 致命",
                            "__init__ 中启动后台任务，不可控",
                            "改为外部显式启动"
                        )
                        
        # 检测 run_in_executor（无论在不在异步函数中）
        if isinstance(node.func, ast.Attribute):
            if node.func.attr == 'run_in_executor':
                self._add_gene(
                    node.lineno,
                    "loop.run_in_executor()",
                    "🟠 高危",
                    "run_in_executor 可能永久等待，阻塞调用者",
                    "添加 asyncio.wait_for(timeout=...)"
                )
                
        # 检测 ThreadPoolExecutor
        if isinstance(node.func, ast.Name):
            if node.func.id == 'ThreadPoolExecutor':
                # 检查是否有 max_workers
                has_max_workers = any(
                    isinstance(kw, ast.keyword) and kw.arg == 'max_workers'
                    for kw in node.keywords
                )
                if not has_max_workers:
                    self._add_gene(
                        node.lineno,
                        "ThreadPoolExecutor()",
                        "🟠 高危",
                        "无 max_workers 限制，线程可能无限增长",
                        "设置 max_workers=5-10"
                    )
                    
        # 检测 gather 无 timeout
        if isinstance(node.func, ast.Attribute):
            if node.func.attr == 'gather':
                # 检查是否被 wait_for 包裹（简化检测）
                self._add_gene(
                    node.lineno,
                    "asyncio.gather()",
                    "🟠 高危",
                    "gather 无超时，一个任务卡住全卡住",
                    "使用 asyncio.wait_for(gather(...), timeout=...)"
                )
                
        self.generic_visit(node)
        
    def visit_While(self, node):
        """检测 while 循环"""
        if self.is_async:
            # 检查是否是无限循环
            if isinstance(node.test, ast.Constant) and node.test.value == True:
                # 检查循环体内是否有 await asyncio.sleep
                has_sleep = self._has_await_sleep(node.body)
                if not has_sleep:
                    self._add_gene(
                        node.lineno,
                        "while True: (无 await asyncio.sleep)",
                        "🔴 致命",
                        "无限循环无 sleep，会饿死事件循环",
                        "添加 await asyncio.sleep(0) 或 await asyncio.sleep(1)"
                    )
                    
        self.generic_visit(node)
        
    def _has_await_sleep(self, body: List[ast.stmt]) -> bool:
        """检查代码块中是否有 await asyncio.sleep"""
        for stmt in body:
            for node in ast.walk(stmt):
                if isinstance(node, ast.Await):
                    if isinstance(node.value, ast.Call):
                        call = node.value
                        if isinstance(call.func, ast.Attribute):
                            if call.func.attr == 'sleep':
                                return True
        return False
        
    def _check_async_body(self, stmt, func_lineno: int):
        """检查异步函数体内的危险操作"""
        pass  # 在 visit 方法中处理
        
    def _add_gene(self, lineno: int, code: str, severity: str, reason: str, fix: str):
        """添加基因记录"""
        context = self._get_context(lineno)
        self.genes.append(Gene(
            file=self.filename,
            line=lineno,
            code=code[:80],
            name=reason,
            severity=severity,
            fix=fix,
            context=context
        ))
        
    def _get_source(self, node) -> str:
        """获取节点的源代码"""
        try:
            start_line = node.lineno - 1
            end_line = getattr(node, 'end_lineno', start_line + 1)
            return '\n'.join(self.lines[start_line:end_line])[:80]
        except:
            return "<无法获取代码>"
            
    def _get_args(self, node) -> str:
        """获取函数调用的参数"""
        try:
            args = []
            for arg in node.args:
                if isinstance(arg, ast.Constant):
                    args.append(str(arg.value))
                else:
                    args.append("...")
            return ", ".join(args)
        except:
            return "..."
            
    def _get_context(self, lineno: int, context: int = 3) -> str:
        """获取代码上下文"""
        start = max(0, lineno - context - 1)
        end = min(len(self.lines), lineno + context - 1)
        return '\n'.join(f"{i+1:4d}: {self.lines[i]}" for i in range(start, end))


class RealAntDetectorV3:
    """真·蚂蚁基因检测器 v3.0"""
    
    def __init__(self, root_path: str):
        self.root = Path(root_path)
        self.all_genes: List[Gene] = []
        
    async def scan_file(self, file_path: Path) -> List[Gene]:
        """扫描单个文件"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                source = f.read()
                
            # 使用 AST 分析
            try:
                tree = ast.parse(source)
                analyzer = ASTAnalyzer(source, str(file_path.relative_to(self.root)))
                analyzer.visit(tree)
                return analyzer.genes
            except SyntaxError as e:
                # 语法错误，使用正则回退
                return self._regex_fallback(source, file_path)
                
        except Exception as e:
            print(f"❌ 扫描失败 {file_path}: {e}")
            return []
            
    def _regex_fallback(self, source: str, file_path: Path) -> List[Gene]:
        """正则回退检测（AST解析失败时用）"""
        genes = []
        lines = source.split('\n')
        filename = str(file_path.relative_to(self.root))
        
        # 检测 threading.Lock 在 async def 中
        async_funcs = re.finditer(r'async\s+def\s+(\w+)', source)
        for match in async_funcs:
            func_start = match.start()
            func_name = match.group(1)
            
            # 找到函数体（简化：找到下一个 async def 或 class 或文件结束）
            func_end = len(source)
            next_def = re.search(r'\n(?:async\s+def|def|class)\s+', source[func_start+1:])
            if next_def:
                func_end = func_start + 1 + next_def.start()
                
            func_body = source[func_start:func_end]
            
            # 检测 with self._lock:
            for lock_match in re.finditer(r'with\s+(self\._lock|self\.lock|_lock|lock)\s*:', func_body):
                abs_pos = func_start + lock_match.start()
                line_no = source[:abs_pos].count('\n') + 1
                genes.append(Gene(
                    file=filename,
                    line=line_no,
                    code=lines[line_no-1].strip()[:80],
                    name="异步函数中使用 threading.Lock",
                    severity="🔴 致命",
                    fix="改为 asyncio.Lock()",
                    context=self._get_context(lines, line_no)
                ))
                
            # 检测 time.sleep
            for sleep_match in re.finditer(r'time\.sleep\(([^)]+)\)', func_body):
                abs_pos = func_start + sleep_match.start()
                line_no = source[:abs_pos].count('\n') + 1
                genes.append(Gene(
                    file=filename,
                    line=line_no,
                    code=lines[line_no-1].strip()[:80],
                    name="异步函数中使用 time.sleep",
                    severity="🔴 致命",
                    fix="改为 await asyncio.sleep()",
                    context=self._get_context(lines, line_no)
                ))
                
        return genes
        
    def _get_context(self, lines: List[str], lineno: int, context: int = 3) -> str:
        """获取代码上下文"""
        start = max(0, lineno - context - 1)
        end = min(len(lines), lineno + context - 1)
        return '\n'.join(f"{i+1:4d}: {lines[i]}" for i in range(start, end))
        
    async def scan_directory(self):
        """扫描整个目录"""
        py_files = list(self.root.rglob("*.py"))
        # 排除检测脚本自己
        py_files = [f for f in py_files if not f.name.startswith('real_ant_detector')]
        
        total = len(py_files)
        print(f"🔍 开始扫描: {self.root}")
        print(f"📊 找到 {total} 个Python文件（排除检测脚本）")
        print("=" * 70)
        
        # 并发扫描
        semaphore = asyncio.Semaphore(20)
        
        async def scan_with_limit(file_path):
            async with semaphore:
                genes = await self.scan_file(file_path)
                self.all_genes.extend(genes)
                return len(genes)
        
        tasks = [scan_with_limit(f) for f in py_files]
        results = await asyncio.gather(*tasks)
        
        print(f"✅ 扫描完成，发现 {sum(results)} 处基因")
        return self.all_genes
        
    def generate_report(self) -> str:
        """生成检测报告"""
        # 按文件分组
        by_file: Dict[str, List[Gene]] = {}
        for gene in self.all_genes:
            if gene.file not in by_file:
                by_file[gene.file] = []
            by_file[gene.file].append(gene)
            
        # 按严重程度统计
        fatal = len([g for g in self.all_genes if '🔴' in g.severity])
        high = len([g for g in self.all_genes if '🟠' in g.severity])
        medium = len([g for g in self.all_genes if '🟡' in g.severity])
        low = len([g for g in self.all_genes if '🟢' in g.severity])
        
        lines = [
            "=" * 80,
            "🐜 真·蚂蚁基因检测报告 v3.0",
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
            "📈 总体统计:",
            f"  有问题文件: {len(by_file)} 个",
            f"  🔴 致命基因: {fatal} 处",
            f"  🟠 高危基因: {high} 处",
            f"  🟡 中危基因: {medium} 处",
            f"  🟢 低危基因: {low} 处",
            f"  总计: {len(self.all_genes)} 处",
            "",
            "=" * 80,
        ]
        
        # 按致命程度排序文件
        def file_severity(file_genes):
            fatal_count = sum(1 for g in file_genes if '🔴' in g.severity)
            high_count = sum(1 for g in file_genes if '🟠' in g.severity)
            return (fatal_count, high_count)
            
        sorted_files = sorted(by_file.items(), key=lambda x: file_severity(x[1]), reverse=True)
        
        # 详细列出致命基因
        if fatal > 0:
            lines.extend([
                "🔥 致命基因详情（立即修复）:",
                "-" * 80,
            ])
            for file_path, genes in sorted_files:
                fatal_genes = [g for g in genes if '🔴' in g.severity]
                if fatal_genes:
                    lines.append(f"\n🔴 {file_path} ({len(fatal_genes)}处致命)")
                    for gene in fatal_genes:
                        lines.extend([
                            f"  📍 第{gene.line}行: {gene.name}",
                            f"     代码: {gene.code}",
                            f"     修复: {gene.fix}",
                            "",
                        ])
                        
        # 列出高危基因
        if high > 0:
            lines.extend([
                "=" * 80,
                "🟠 高危基因详情:",
                "-" * 80,
            ])
            for file_path, genes in sorted_files:
                high_genes = [g for g in genes if '🟠' in g.severity]
                if high_genes:
                    lines.append(f"\n🟠 {file_path} ({len(high_genes)}处)")
                    for gene in high_genes[:5]:  # 只显示前5个
                        lines.append(f"  第{gene.line}行: {gene.name}")
                        lines.append(f"    代码: {gene.code[:60]}")
                        
        # 修复清单
        lines.extend([
            "",
            "=" * 80,
            "🛠️ 修复清单（按优先级）:",
            "-" * 80,
        ])
        
        # 提取所有需要修复的项
        fixes = {}
        for gene in self.all_genes:
            if gene.fix not in fixes:
                fixes[gene.fix] = {'count': 0, 'severity': gene.severity}
            fixes[gene.fix]['count'] += 1
            
        for fix, info in sorted(fixes.items(), key=lambda x: '🔴' not in x[1]['severity']):
            lines.append(f"  {info['severity']} {fix} ({info['count']}处)")
            
        lines.extend([
            "",
            "=" * 80,
        ])
        
        return '\n'.join(lines)


async def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("用法: python real_ant_detector_v3.py <扫描目录>")
        print("示例: python real_ant_detector_v3.py .")
        sys.exit(1)
        
    root_path = sys.argv[1]
    detector = RealAntDetectorV3(root_path)
    
    await detector.scan_directory()
    report = detector.generate_report()
    
    print(report)
    
    # 保存报告
    output_file = f"真蚂蚁检测报告_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
        
    print(f"\n📄 报告已保存: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
