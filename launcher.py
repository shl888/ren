# launcher.py
"""
极简启动器 - 启动智能大脑
"""

import asyncio
import logging
import sys
import traceback

from smart_brain.core import SmartBrain


def start_keep_alive_background():
    """启动保活服务（后台线程）"""
    try:
        from keep_alive import start_with_http_check
        import threading
        
        def run_keeper():
            try:
                start_with_http_check()
            except Exception as e:
                logging.error(f"保活服务异常: {e}")
        
        thread = threading.Thread(target=run_keeper, daemon=True)
        thread.start()
        logging.info("✅ 保活服务已启动")
    except:
        logging.warning("⚠️  保活服务未启动，但继续运行")


def main():
    """主函数"""
    # 启动保活服务（与原brain_core.py保持一致）
    start_keep_alive_background()
    
    # 配置日志（修复日期格式笔误）
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'  # 修复：%Y-%m-%d 不是 %Y-%m-d
    )
    
    brain = SmartBrain()
    
    try:
        asyncio.run(brain.run())
    except KeyboardInterrupt:
        logging.info("程序已停止")
    except Exception as e:
        logging.error(f"程序错误: {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
    