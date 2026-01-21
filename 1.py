#!/usr/bin/env python3
"""
高级文件树生成器 - 带文件统计和过滤功能
"""

import os
import sys
import fnmatch
from pathlib import Path
from datetime import datetime
from collections import defaultdict

class FileTreeGenerator:
    def __init__(self, root_path='.'):
        self.root = Path(root_path).resolve()
        self.stats = {
            'directories': 0,
            'files': 0,
            'total_size': 0,
            'extensions': defaultdict(int)
        }
        # 存储生成时的参数
        self.exclude_patterns = None
        self.include_patterns = None
        self.max_depth = 10
        self.show_size = False
        self.show_date = False
        
    def generate_tree(self, 
                     exclude_patterns=None,
                     include_patterns=None,
                     max_depth=10,
                     show_size=False,
                     show_date=False):
        """
        生成文件树
        
        Args:
            exclude_patterns: 排除模式列表，如 ['*.pyc', '*.log']
            include_patterns: 包含模式列表
            max_depth: 最大递归深度
            show_size: 显示文件大小
            show_date: 显示修改日期
        """
        # 保存参数供后续使用
        self.exclude_patterns = exclude_patterns
        self.include_patterns = include_patterns
        self.max_depth = max_depth
        self.show_size = show_size
        self.show_date = show_date
        
        if exclude_patterns is None:
            exclude_patterns = [
                '*.pyc', '*.pyo', '*.so', '*.o', '__pycache__',
                '.git/*', '.idea/*', '.vscode/*', 'node_modules/*',
                '.DS_Store', 'Thumbs.db', '*.log'
            ]
        
        print(f"扫描目录: {self.root}")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        # 生成树结构
        self._walk_tree(
            self.root, 
            '', 
            True, 
            0, 
            max_depth,
            exclude_patterns,
            include_patterns,
            show_size,
            show_date
        )
        
        # 打印统计信息
        self._print_stats()
    
    def _should_include(self, path, exclude_patterns, include_patterns):
        """判断是否应该包含该路径"""
        rel_path = str(path.relative_to(self.root))
        
        # 检查排除模式
        for pattern in exclude_patterns:
            if fnmatch.fnmatch(rel_path, pattern) or fnmatch.fnmatch(path.name, pattern):
                return False
        
        # 检查包含模式
        if include_patterns:
            for pattern in include_patterns:
                if fnmatch.fnmatch(rel_path, pattern) or fnmatch.fnmatch(path.name, pattern):
                    return True
            return False
        
        return True
    
    def _format_size(self, size):
        """格式化文件大小"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f}{unit}"
            size /= 1024.0
        return f"{size:.1f}TB"
    
    def _walk_tree(self, current_path, prefix, is_last, depth, max_depth,
                  exclude_patterns, include_patterns, show_size, show_date):
        """递归遍历目录树"""
        if depth > max_depth:
            return
        
        try:
            items = sorted(os.listdir(current_path))
        except PermissionError:
            return
        
        # 过滤项目
        filtered_items = []
        for item in items:
            item_path = current_path / item
            if self._should_include(item_path, exclude_patterns, include_patterns):
                filtered_items.append(item)
        
        for i, item in enumerate(filtered_items):
            item_path = current_path / item
            is_last_item = (i == len(filtered_items) - 1)
            
            # 绘制连接线
            connector = '└── ' if is_last_item else '├── '
            
            # 获取文件信息
            suffix = ''
            try:
                if item_path.is_file():
                    self.stats['files'] += 1
                    size = item_path.stat().st_size
                    self.stats['total_size'] += size
                    
                    # 记录扩展名
                    ext = item_path.suffix.lower()
                    if ext:
                        self.stats['extensions'][ext] += 1
                    
                    if show_size:
                        suffix += f" [{self._format_size(size)}]"
                    
                    if show_date:
                        mtime = datetime.fromtimestamp(item_path.stat().st_mtime)
                        suffix += f" {mtime.strftime('%Y-%m-%d')}"
                else:
                    self.stats['directories'] += 1
                    suffix = "/"
                
                line = f"{prefix}{connector}{item}{suffix}"
                print(line)
                
                # 如果是目录，递归处理
                if item_path.is_dir():
                    new_prefix = prefix + ('    ' if is_last_item else '│   ')
                    self._walk_tree(
                        item_path, 
                        new_prefix, 
                        is_last_item, 
                        depth + 1, 
                        max_depth,
                        exclude_patterns,
                        include_patterns,
                        show_size,
                        show_date
                    )
                    
            except (OSError, PermissionError):
                print(f"{prefix}{connector}{item} [权限不足]")
    
    def _print_stats(self):
        """打印统计信息"""
        print("\n" + "=" * 70)
        print("统计信息:")
        print(f"  目录数量: {self.stats['directories']}")
        print(f"  文件数量: {self.stats['files']}")
        print(f"  总大小: {self._format_size(self.stats['total_size'])}")
        
        if self.stats['extensions']:
            print(f"  文件类型分布:")
            sorted_exts = sorted(self.stats['extensions'].items(), 
                               key=lambda x: x[1], reverse=True)
            for ext, count in sorted_exts[:10]:  # 显示前10种类型
                percentage = (count / self.stats['files']) * 100
                print(f"    {ext}: {count} ({percentage:.1f}%)")
        
        print(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def save_to_file(self, output_path=None, exclude_patterns=None,
                    include_patterns=None, max_depth=None, show_size=None, show_date=None):
        """保存文件树到文件"""
        # 使用保存的参数或传入的参数
        if exclude_patterns is None:
            exclude_patterns = self.exclude_patterns or [
                '*.pyc', '*.pyo', '*.so', '*.o', '__pycache__',
                '.git/*', '.idea/*', '.vscode/*', 'node_modules/*',
                '.DS_Store', 'Thumbs.db', '*.log'
            ]
        
        if max_depth is None:
            max_depth = self.max_depth or 10
        if show_size is None:
            show_size = self.show_size or False
        if show_date is None:
            show_date = self.show_date or False
        
        if output_path is None:
            # 默认保存在扫描目录下
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = self.root / f"file_tree_{timestamp}.txt"
        
        # 确保输出路径是Path对象
        output_path = Path(output_path)
        
        # 如果文件已存在，询问是否覆盖
        if output_path.exists():
            response = input(f"文件 {output_path} 已存在，是否覆盖？(y/n): ").lower()
            if response != 'y':
                print("取消保存。")
                return
        
        try:
            # 重置统计信息
            self.stats = {
                'directories': 0,
                'files': 0,
                'total_size': 0,
                'extensions': defaultdict(int)
            }
            
            with open(output_path, 'w', encoding='utf-8') as f:
                # 保存头部信息
                f.write(f"扫描目录: {self.root}\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                if exclude_patterns:
                    f.write(f"排除模式: {exclude_patterns}\n")
                if include_patterns:
                    f.write(f"包含模式: {include_patterns}\n")
                f.write("=" * 70 + "\n")
                
                # 保存树结构
                self._save_tree_to_file(
                    f, self.root, '', True, 0, max_depth,
                    exclude_patterns, include_patterns, show_size, show_date
                )
                
                # 保存统计信息
                f.write("\n" + "=" * 70 + "\n")
                f.write("统计信息:\n")
                f.write(f"  目录数量: {self.stats['directories']}\n")
                f.write(f"  文件数量: {self.stats['files']}\n")
                f.write(f"  总大小: {self._format_size(self.stats['total_size'])}\n")
                
                if self.stats['extensions']:
                    f.write("  文件类型分布:\n")
                    sorted_exts = sorted(self.stats['extensions'].items(), 
                                       key=lambda x: x[1], reverse=True)
                    for ext, count in sorted_exts[:10]:
                        if self.stats['files'] > 0:
                            percentage = (count / self.stats['files']) * 100
                            f.write(f"    {ext}: {count} ({percentage:.1f}%)\n")
                        else:
                            f.write(f"    {ext}: {count}\n")
                
                f.write(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            print(f"\n✓ 文件树已保存到: {output_path}")
            print(f"  文件大小: {self._format_size(output_path.stat().st_size)}")
            
        except Exception as e:
            print(f"保存失败: {e}")
            import traceback
            traceback.print_exc()
    
    def _save_tree_to_file(self, file_obj, current_path, prefix, is_last, depth, max_depth,
                          exclude_patterns, include_patterns, show_size, show_date):
        """递归遍历目录树并写入文件"""
        if depth > max_depth:
            return
        
        try:
            items = sorted(os.listdir(current_path))
        except PermissionError:
            return
        
        # 过滤项目
        filtered_items = []
        for item in items:
            item_path = current_path / item
            if self._should_include(item_path, exclude_patterns, include_patterns):
                filtered_items.append(item)
        
        for i, item in enumerate(filtered_items):
            item_path = current_path / item
            is_last_item = (i == len(filtered_items) - 1)
            
            # 绘制连接线
            connector = '└── ' if is_last_item else '├── '
            
            # 获取文件信息
            suffix = ''
            try:
                if item_path.is_file():
                    self.stats['files'] += 1
                    size = item_path.stat().st_size
                    self.stats['total_size'] += size
                    
                    # 记录扩展名
                    ext = item_path.suffix.lower()
                    if ext:
                        self.stats['extensions'][ext] += 1
                    
                    if show_size:
                        suffix += f" [{self._format_size(size)}]"
                    
                    if show_date:
                        mtime = datetime.fromtimestamp(item_path.stat().st_mtime)
                        suffix += f" {mtime.strftime('%Y-%m-%d')}"
                else:
                    self.stats['directories'] += 1
                    suffix = "/"
                
                line = f"{prefix}{connector}{item}{suffix}\n"
                file_obj.write(line)
                
                # 如果是目录，递归处理
                if item_path.is_dir():
                    new_prefix = prefix + ('    ' if is_last_item else '│   ')
                    self._save_tree_to_file(
                        file_obj, item_path, new_prefix, is_last_item, depth + 1, max_depth,
                        exclude_patterns, include_patterns, show_size, show_date
                    )
                    
            except (OSError, PermissionError):
                file_obj.write(f"{prefix}{connector}{item} [权限不足]\n")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='高级文件树生成器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  %(prog)s .                          # 生成当前目录文件树
  %(prog)s /path/to/repo --size       # 显示文件大小
  %(prog)s . -o tree.txt              # 输出到文件
  %(prog)s . -x "*.pyc" -x "*.log"    # 排除特定文件
  %(prog)s . -i "*.py" -i "*.js"      # 只包含特定文件
        '''
    )
    
    parser.add_argument('path', nargs='?', default='.', 
                       help='目录路径（默认当前目录）')
    parser.add_argument('--output', '-o', 
                       help='输出到文件（可选，交互模式下会询问）')
    parser.add_argument('--size', '-s', action='store_true',
                       help='显示文件大小')
    parser.add_argument('--date', '-d', action='store_true',
                       help='显示修改日期')
    parser.add_argument('--exclude', '-x', action='append',
                       help='排除模式（可多次使用）')
    parser.add_argument('--include', '-i', action='append',
                       help='包含模式（只显示匹配的文件）')
    parser.add_argument('--max-depth', type=int, default=10,
                       help='最大递归深度（默认10）')
    parser.add_argument('--no-interactive', action='store_true',
                       help='禁用交互模式（不询问是否保存）')
    parser.add_argument('--auto-save', action='store_true',
                       help='自动保存（不询问，直接保存）')
    
    args = parser.parse_args()
    
    # 创建生成器
    generator = FileTreeGenerator(args.path)
    
    # 保存原始标准输出
    original_stdout = sys.stdout
    
    # 如果指定了输出文件
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                sys.stdout = f
                generator.generate_tree(
                    exclude_patterns=args.exclude,
                    include_patterns=args.include,
                    max_depth=args.max_depth,
                    show_size=args.size,
                    show_date=args.date
                )
                sys.stdout = original_stdout
            print(f"文件树已保存到: {args.output}")
        except Exception as e:
            print(f"保存失败: {e}")
            sys.exit(1)
    else:
        # 显示到控制台
        generator.generate_tree(
            exclude_patterns=args.exclude,
            include_patterns=args.include,
            max_depth=args.max_depth,
            show_size=args.size,
            show_date=args.date
        )
        
        # 交互模式：询问是否保存
        if not args.no_interactive:
            if args.auto_save:
                # 自动保存
                generator.save_to_file(
                    exclude_patterns=args.exclude,
                    include_patterns=args.include,
                    max_depth=args.max_depth,
                    show_size=args.size,
                    show_date=args.date
                )
            else:
                # 询问用户
                response = input("\n是否保存文件树到当前目录？(y/n): ").lower()
                if response == 'y':
                    # 可选：让用户输入文件名
                    custom_name = input("输入文件名（直接回车使用默认名称）: ").strip()
                    if custom_name:
                        # 确保有文件扩展名
                        if not custom_name.endswith('.txt'):
                            custom_name += '.txt'
                        generator.save_to_file(
                            output_path=generator.root / custom_name,
                            exclude_patterns=args.exclude,
                            include_patterns=args.include,
                            max_depth=args.max_depth,
                            show_size=args.size,
                            show_date=args.date
                        )
                    else:
                        generator.save_to_file(
                            exclude_patterns=args.exclude,
                            include_patterns=args.include,
                            max_depth=args.max_depth,
                            show_size=args.size,
                            show_date=args.date
                        )
                else:
                    print("未保存文件。")


if __name__ == "__main__":
    main()