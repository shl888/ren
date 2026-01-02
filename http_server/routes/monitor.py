"""
系统监控接口模块
处理系统健康检查、指标收集等
"""
from aiohttp import web
import datetime
import logging

from ..auth import require_auth

logger = logging.getLogger(__name__)


async def system_monitor_placeholder(request: web.Request) -> web.Response:
    """系统监控占位函数"""
    return web.json_response({
        "error": "系统监控模块未安装",
        "hint": "请安装psutil包并确保system_monitor模块存在"
    }, status=501)


async def get_system_health(request: web.Request) -> web.Response:
    """获取系统健康状态（公开访问）"""
    try:
        # 导入可能缺失的模块
        from system_monitor.collector import SystemMonitor
        
        monitor = SystemMonitor()
        data = monitor.check_health()
        
        # 只返回基本信息，不暴露敏感数据
        safe_data = {
            "healthy": data.get("healthy", False),
            "timestamp": data.get("timestamp")
        }
        
        return web.json_response({
            "success": True,
            "data": safe_data
        })
        
    except ImportError:
        return system_monitor_placeholder(request)
    except Exception as e:
        logger.error(f"获取系统健康状态失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500)


@require_auth
async def get_system_metrics(request: web.Request) -> web.Response:
    """获取系统核心指标（需要密码）"""
    try:
        from system_monitor.collector import SystemMonitor
        
        monitor = SystemMonitor()
        data = monitor.collect_light()
        
        return web.json_response({
            "success": True,
            "data": data
        })
        
    except ImportError:
        return system_monitor_placeholder(request)
    except Exception as e:
        logger.error(f"获取系统指标失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500)


@require_auth
async def get_system_status(request: web.Request) -> web.Response:
    """获取完整系统状态（需要密码）"""
    try:
        from system_monitor.collector import SystemMonitor
        
        monitor = SystemMonitor()
        data = monitor.collect_all()
        
        return web.json_response({
            "success": True,
            "data": data
        })
        
    except ImportError:
        return system_monitor_placeholder(request)
    except Exception as e:
        logger.error(f"获取系统状态失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500)


def setup_monitor_routes(app: web.Application):
    """设置系统监控路由"""
    app.router.add_get('/api/monitor/health', get_system_health)
    app.router.add_get('/api/monitor/metrics', get_system_metrics)
    app.router.add_get('/api/monitor/status', get_system_status)
    
    logger.info("✅ 监控路由已加载: /api/monitor/health, /api/monitor/metrics, /api/monitor/status")
