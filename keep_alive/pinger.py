import aiohttp  # ✅ [蚂蚁基因修复] 导入aiohttp
import asyncio  # ✅ [蚂蚁基因修复] 导入asyncio
import time
import random
from .config import Config

class Pinger:
    """Ping执行器 - 异步优化版"""
    
    @staticmethod
    async def ping_single_async(url, timeout=None, session=None):
        """异步执行单次ping"""
        if timeout is None:
            timeout = Config.REQUEST_TIMEOUT
        
        # ✅ [蚂蚁基因修复] 创建或使用传入的session
        close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True
        
        try:
            headers = {'User-Agent': Config.get_random_user_agent()}
            
            async with session.get(url, headers=headers, timeout=timeout) as response:
                status = response.status
                
                # 读取少量数据确认响应
                if status == 200 or status == 204:
                    try:
                        await response.content.read(100)  # ✅ [蚂蚁基因修复] 异步读取
                    except:
                        pass  # 读取失败也视为成功（有响应）
                    return True, url
                
                return False, url
                
        except (aiohttp.ClientError, asyncio.TimeoutError):
            # 静默处理常见网络错误
            return False, url
        except Exception:
            return False, url
        finally:
            if close_session:
                await session.close()
    
    @classmethod
    async def ping_with_retry_async(cls, url, max_retries=None, session=None):
        """异步带重试的ping（快速版）"""
        if max_retries is None:
            max_retries = Config.MAX_RETRIES
        
        # ✅ [蚂蚁基因修复] 创建或使用传入的session
        close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True
        
        try:
            for attempt in range(max_retries + 1):  # 包括首次尝试
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                success, used_url = await cls.ping_single_async(url, session=session)
                if success:
                    return True, used_url
                
                # 快速重试等待（如果还有重试次数）
                if attempt < max_retries:
                    await asyncio.sleep(1)  # ✅ [蚂蚁基因修复] 异步sleep
        finally:
            if close_session:
                await session.close()
        
        return False, url
    
    @classmethod
    async def self_ping_async(cls):
        """异步自ping - 带端点回退策略"""
        # ✅ [蚂蚁基因修复] 创建一个共享session，避免重复创建
        async with aiohttp.ClientSession() as session:
            # 按优先级尝试所有端点
            for endpoint in Config.SELF_ENDPOINTS:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                success, used_url = await cls.ping_with_retry_async(
                    endpoint, max_retries=1, session=session
                )
                if success:
                    return True, used_url
                # 立即尝试下一个端点，不等待
        
        return False, "all_failed"
    
    @classmethod
    async def external_ping_async(cls):
        """异步外ping - 保持不变"""
        target = Config.get_random_external_target()
        
        # ✅ [蚂蚁基因修复] 创建一个新的session
        async with aiohttp.ClientSession() as session:
            success, used_url = await cls.ping_with_retry_async(
                target, max_retries=2, session=session
            )
            return success, used_url
    
    # ✅ [蚂蚁基因修复] 保留同步版本供非异步代码调用（内部使用run_in_executor）
    @classmethod
    def self_ping(cls):
        """同步自ping（兼容旧代码）"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(cls.self_ping_async())
        finally:
            loop.close()
    
    @classmethod
    def external_ping(cls):
        """同步外ping（兼容旧代码）"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(cls.external_ping_async())
        finally:
            loop.close()
    
    @staticmethod
    def detect_uptimerobot(request_headers):
        """检测是否为UptimeRobot访问（仅检测，不修改行为）"""
        user_agent = request_headers.get('User-Agent', '')
        return Config.UPTIMEROBOT_USER_AGENT in user_agent