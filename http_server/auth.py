"""
HTTP API认证
从环境变量获取API密钥
"""
import os
import hmac
import hashlib
import base64
import time
from typing import Dict, Any, Optional
from functools import wraps
from aiohttp import web

# ============ 【完全删除：API_KEYS字典和所有读取环境变量的代码】============

# ============ 【保留：签名工具函数】============
def generate_binance_signature(secret: str, data: str) -> str:
    """生成币安签名"""
    return hmac.new(
        secret.encode('utf-8'),
        data.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

def generate_okx_signature(secret: str, timestamp: str, method: str, request_path: str, body: str = "") -> str:
    """生成欧意签名"""
    message = timestamp + method.upper() + request_path + body
    mac = hmac.new(
        bytes(secret, encoding='utf-8'),
        bytes(message, encoding='utf-8'),
        digestmod='sha256'
    )
    return base64.b64encode(mac.digest()).decode()

# ============ 【保留：HTTP密码认证】============
# 服务器访问密码
ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "default_password_change_me")

def require_auth(func):
    """认证装饰器 - 基于HTTP Header的密码认证"""
    @wraps(func)
    async def wrapper(request):
        # ============ 公开路径定义 ============
        public_paths = [
            '/',                     # 首页
            '/public/ping',          # 保活ping
            '/health',               # 健康检查
            '/api/monitor/health'    # 系统健康状态（公开）
        ]
        
        # 检查是否为公开路径
        if request.path in public_paths:
            return await func(request)
        
        # ============ 检查路径是否为公开监控端点 ============
        if request.path.startswith('/api/monitor/health'):
            return await func(request)
        
        # 检查访问密码
        provided_password = request.headers.get('X-Access-Password')
        if not provided_password:
            return web.json_response(
                {"error": "缺少访问密码。请在请求头中使用: X-Access-Password"},
                status=401
            )
        
        if provided_password != ACCESS_PASSWORD:
            return web.json_response(
                {"error": "访问密码无效"},
                status=401
            )
        
        return await func(request)
    
    return wrapper

# ============ 【保留：系统监控专用装饰器】============
def require_monitor_auth(func):
    """系统监控认证装饰器 - 仅检查密码，不检查交易所API"""
    @wraps(func)
    async def wrapper(request):
        # 公开的监控健康检查不需要认证
        if request.path.endswith('/api/monitor/health'):
            return await func(request)
        
        # 检查访问密码
        provided_password = request.headers.get('X-Access-Password')
        if not provided_password:
            return web.json_response(
                {"error": "缺少访问密码。请在请求头中使用: X-Access-Password"},
                status=401
            )
        
        if provided_password != ACCESS_PASSWORD:
            return web.json_response(
                {"error": "访问密码无效"},
                status=401
            )
        
        return await func(request)
    
    return wrapper