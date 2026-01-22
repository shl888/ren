"""
HTTP API认证和签名工具
注意：不再从环境变量读取API！API由大脑统一管理
"""
import hmac
import hashlib
import base64
import os
from typing import Dict, Any, Optional
from functools import wraps
from aiohttp import web

# ❌ 删除整个API_KEYS部分！
# API_KEYS = {
#     "binance": {
#         "api_key": os.getenv("BINANCE_API_KEY", ""),
#         "api_secret": os.getenv("BINANCE_API_SECRET", "")
#     },
#     ...
# }

# ✅ 保留：服务器访问密码（与API无关）
ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "default_password_change_me")

# ❌ 删除：has_api_keys函数（大脑负责检查）
# ❌ 删除：get_api_config函数（大脑负责提供）

# ✅ 保留：签名生成函数（工具函数，不依赖环境变量）
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

# ✅ 保留：认证装饰器（只检查访问密码，不检查API）
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
        
        # ❌ 删除：交易所API检查（大脑负责提供API）
        # if '/api/trade/' in request.path or '/api/account/' in request.path:
        #     exchange = request.match_info.get('exchange', '')
        #     if exchange and not has_api_keys(exchange):
        #         return web.json_response(
        #             {"error": f"{exchange} API密钥未配置"},
        #             status=400
        #         )
        
        return await func(request)
    
    return wrapper

# ✅ 保留：系统监控专用装饰器
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