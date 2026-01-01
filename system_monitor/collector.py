"""
系统数据采集器
按需采集，不保存历史数据
"""
import os
import sys
import time
import psutil
import platform
from datetime import datetime
from typing import Dict, Any, Optional

class SystemMonitor:
    """系统监控器 - 按需采集"""
    
    def __init__(self):
        self.start_time = time.time()
        self.pid = os.getpid()
        
    def collect_all(self) -> Dict[str, Any]:
        """采集所有系统数据"""
        return {
            "timestamp": datetime.now().isoformat(),
            "system": self._get_system_info(),
            "cpu": self._get_cpu_info(),
            "memory": self._get_memory_info(),
            "disk": self._get_disk_info(),
            "network": self._get_network_info(),
            "process": self._get_process_info(),
            "python": self._get_python_info(),
            "render": self._get_render_info()
        }
    
    def collect_light(self) -> Dict[str, Any]:
        """采集轻量数据（核心指标）"""
        return {
            "timestamp": datetime.now().isoformat(),
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_used_mb": self._get_memory_used_mb(),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage('/').percent,
            "uptime_seconds": time.time() - self.start_time,
            "process_memory_mb": self._get_process_memory_mb(),
            "process_cpu_percent": psutil.Process(self.pid).cpu_percent(interval=0.1)
        }
    
    def _get_system_info(self) -> Dict[str, Any]:
        """获取系统信息"""
        try:
            uname = platform.uname()
            return {
                "system": uname.system,
                "node_name": uname.node,
                "release": uname.release,
                "version": uname.version,
                "machine": uname.machine,
                "processor": uname.processor,
                "boot_time": datetime.fromtimestamp(psutil.boot_time()).isoformat()
            }
        except:
            return {"error": "无法获取系统信息"}
    
    def _get_cpu_info(self) -> Dict[str, Any]:
        """获取CPU信息"""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.5, percpu=True)
            cpu_freq = psutil.cpu_freq()
            cpu_count = {
                "physical": psutil.cpu_count(logical=False),
                "logical": psutil.cpu_count(logical=True)
            }
            
            return {
                "percent_per_core": cpu_percent,
                "percent_total": sum(cpu_percent) / len(cpu_percent) if cpu_percent else 0,
                "frequency_mhz": cpu_freq.current if cpu_freq else None,
                "count": cpu_count,
                "load_average": self._get_load_average()
            }
        except:
            return {"error": "无法获取CPU信息"}
    
    def _get_load_average(self) -> Optional[list]:
        """获取系统负载（Unix-like系统）"""
        try:
            if hasattr(os, 'getloadavg'):
                return list(os.getloadavg())
        except:
            pass
        return None
    
    def _get_memory_info(self) -> Dict[str, Any]:
        """获取内存信息"""
        try:
            mem = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            return {
                "total_mb": mem.total / 1024 / 1024,
                "available_mb": mem.available / 1024 / 1024,
                "used_mb": mem.used / 1024 / 1024,
                "percent": mem.percent,
                "swap_total_mb": swap.total / 1024 / 1024,
                "swap_used_mb": swap.used / 1024 / 1024,
                "swap_percent": swap.percent
            }
        except:
            return {"error": "无法获取内存信息"}
    
    def _get_memory_used_mb(self) -> float:
        """获取已用内存（MB）"""
        try:
            return psutil.virtual_memory().used / 1024 / 1024
        except:
            return 0.0
    
    def _get_process_memory_mb(self) -> float:
        """获取当前进程内存使用（MB）"""
        try:
            process = psutil.Process(self.pid)
            return process.memory_info().rss / 1024 / 1024
        except:
            return 0.0
    
    def _get_disk_info(self) -> Dict[str, Any]:
        """获取磁盘信息"""
        try:
            disk = psutil.disk_usage('/')
            io_counters = psutil.disk_io_counters()
            
            return {
                "total_gb": disk.total / 1024 / 1024 / 1024,
                "used_gb": disk.used / 1024 / 1024 / 1024,
                "free_gb": disk.free / 1024 / 1024 / 1024,
                "percent": disk.percent,
                "read_bytes": io_counters.read_bytes if io_counters else 0,
                "write_bytes": io_counters.write_bytes if io_counters else 0
            }
        except:
            return {"error": "无法获取磁盘信息"}
    
    def _get_network_info(self) -> Dict[str, Any]:
        """获取网络信息"""
        try:
            net_io = psutil.net_io_counters()
            net_connections = len(psutil.net_connections())
            
            return {
                "bytes_sent": net_io.bytes_sent,
                "bytes_recv": net_io.bytes_recv,
                "packets_sent": net_io.packets_sent,
                "packets_recv": net_io.packets_recv,
                "active_connections": net_connections
            }
        except:
            return {"error": "无法获取网络信息"}
    
    def _get_process_info(self) -> Dict[str, Any]:
        """获取当前进程信息"""
        try:
            process = psutil.Process(self.pid)
            
            with process.oneshot():
                return {
                    "pid": self.pid,
                    "name": process.name(),
                    "status": process.status(),
                    "create_time": datetime.fromtimestamp(process.create_time()).isoformat(),
                    "cpu_percent": process.cpu_percent(interval=0.1),
                    "memory_percent": process.memory_percent(),
                    "num_threads": process.num_threads(),
                    "open_files": len(process.open_files()),
                    "connections": len(process.connections())
                }
        except:
            return {"error": "无法获取进程信息"}
    
    def _get_python_info(self) -> Dict[str, Any]:
        """获取Python环境信息"""
        return {
            "version": sys.version,
            "implementation": platform.python_implementation(),
            "compiler": platform.python_compiler(),
            "build": platform.python_build(),
            "path": sys.path
        }
    
    def _get_render_info(self) -> Dict[str, Any]:
        """获取Render平台信息"""
        render_info = {}
        
        # Render环境变量
        render_vars = [
            'RENDER_SERVICE_NAME',
            'RENDER_SERVICE_TYPE',
            'RENDER_INSTANCE_ID',
            'RENDER_EXTERNAL_URL',
            'RENDER_INSTANCE_COUNT',
            'RENDER_GIT_BRANCH',
            'RENDER_GIT_COMMIT'
        ]
        
        for var in render_vars:
            value = os.getenv(var)
            if value:
                render_info[var.lower()] = value
        
        return render_info
    
    def check_health(self) -> Dict[str, Any]:
        """健康检查（快速版）"""
        try:
            # 快速检查核心指标
            cpu_ok = psutil.cpu_percent(interval=0.1) < 90
            mem_ok = psutil.virtual_memory().percent < 90
            disk_ok = psutil.disk_usage('/').percent < 90
            
            return {
                "healthy": cpu_ok and mem_ok and disk_ok,
                "cpu_ok": cpu_ok,
                "memory_ok": mem_ok,
                "disk_ok": disk_ok,
                "timestamp": datetime.now().isoformat()
            }
        except:
            return {
                "healthy": False,
                "error": "健康检查失败",
                "timestamp": datetime.now().isoformat()
            }