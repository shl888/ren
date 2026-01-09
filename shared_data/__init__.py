"""
shared_data 顶级模块
功能：数据存储 + 智能流水线 + 5步过滤
"""

# 核心实例
from .data_store import data_store  # 全局数据存储

# 管理员（主要接口）
from .pipeline_manager import PipelineManager  # 仅保留PipelineManager

# 5个步骤类（高级调试用）
from .step1_filter import Step1Filter, ExtractedData
from .step2_fusion import Step2Fusion, FusedData
from .step3_align import Step3Align, AlignedData
from .step4_calc import Step4Calc, PlatformData
from .step5_cross_calc import Step5CrossCalc, CrossPlatformData

# 数据模型
__all__ = [
    # 核心实例
    'data_store',
    
    # 管理员
    'PipelineManager',
    
    # 5个步骤类
    'Step1Filter',
    'Step2Fusion',
    'Step3Align',
    'Step4Calc',
    'Step5CrossCalc',
    
    # 数据模型
    'ExtractedData',
    'FusedData',
    'AlignedData',
    'PlatformData',
    'CrossPlatformData',
]

# 版本信息
__version__ = "3.0.0"
__description__ = "智能数据处理流水线模块（流式终极版）"

# 初始化日志
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

# 模块加载日志
logger = logging.getLogger(__name__)
# ✅ 修改：统一日志格式
logger.info(f"[数据处理][模块加载] shared_data v{__version__} 加载完成（流式终极版）")
