import os
import sys
from obsave.api.app import app
from obsave.core.settings import MAX_WORKERS

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=1,  # 开发模式使用单个worker
        log_level="info"
    )