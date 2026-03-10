"""
欧意修复区 - 模块入口
==================================================
【文件职责】
这个文件是欧意修复区的入口，负责导出欧意修复类，
方便外部模块（如调度器或币安修复区入口）导入。

【导出内容】
- OkxMissingRepair - 欧意持仓缺失修复类

【使用示例】
    # 方式1：通过修复区根入口导入
    from data_completion_department.repair import OkxMissingRepair
    
    # 方式2：直接通过欧意修复区导入
    from data_completion_department.repair.okx import OkxMissingRepair
    
    # 方式3：导入整个欧意修复区
    from data_completion_department.repair import okx
==================================================
"""

from .missing_repair import OkxMissingRepair

__all__ = ['OkxMissingRepair']  # 欧意持仓缺失修复类