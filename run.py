import os
import sys

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "obsave.api.app:app",  # 使用导入字符串
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=10,  # 开发模式使用单个worker
        log_level="info"
    )