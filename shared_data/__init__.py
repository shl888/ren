"""
shared_data 顶级模块 - 管理员-执行者架构（已集成Step0）
"""

# 核心实例
from .data_store import data_store
from .pipeline_manager import PipelineManager

# ✅ 新增：导出路由模块
from . import routes

# ✅ 现在有6个步骤类（新增Step0）
from .step0_rate_limiter import Step0RateLimiter
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
    
    # ✅ 新增：路由模块
    'routes',
    
    # ✅ 6个步骤类（新增Step0）
    'Step0RateLimiter',
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
__version__ = "4.2.0"  # ✅ 版本号更新（新增标记价格字段）
__description__ = "管理员-执行者架构数据处理系统（新增标记价格字段）"

# 初始化日志
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

# 模块加载日志
logger = logging.getLogger(__name__)
logger.info(f"✅ shared_data v{__version__} 加载完成（新增标记价格字段）")
logger.info(f"✅ shared_data.routes 模块已就绪")  # 新增提示