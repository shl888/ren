"""
shared_data 顶级模块 - 管理员-执行者架构
"""

# 核心实例
from .data_store import data_store  # 执行者
from .pipeline_manager import PipelineManager  # 管理员

# 5个步骤类
from .step1_filter import Step1Filter, ExtractedData
from .step2_fusion import Step2Fusion, FusedData
from .step3_align import Step3Align, AlignedData
from .step4_calc import Step4Calc, PlatformData
from .step5_cross_calc import Step5CrossCalc, CrossPlatformData

# 数据模型
__all__ = [
    # 核心实例
    'data_store',
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
__version__ = "4.0.0"
__description__ = "管理员-执行者架构数据处理系统"

# 初始化日志
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

# 模块加载日志
logger = logging.getLogger(__name__)
logger.info(f"✅ shared_data v{__version__} 加载完成（管理员-执行者架构）")
