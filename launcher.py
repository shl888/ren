"""
极简启动器
"""

import asyncio
import logging
import sys
import traceback

from smart_brain.core import SmartBrain, start_keep_alive_background

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-d %H:%M:%S'
    )
    
    start_keep_alive_background()
    
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