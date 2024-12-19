from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
import time
import asyncio
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# 数据库配置
DATABASE_URL = "sqlite:///./example.db"

# 创建数据库引擎
engine = create_engine(
    DATABASE_URL,
    connect_args={
        "check_same_thread": False,
        "timeout": 30
    },
    pool_pre_ping=True,  # 自动检查连接
    pool_recycle=3600    # 每小时回收连接
)

# 创建会话工厂
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    expire_on_commit=False
)

# 创建基类
Base = declarative_base()

# 初始化数据库
def init_db():
    """初始化数据库"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

# 监控查询执行时间
@event.listens_for(engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.info.setdefault('query_start_time', []).append(time.time())
    logger.debug("Start query: %s", statement)

@event.listens_for(engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    total = time.time() - conn.info['query_start_time'].pop(-1)
    logger.debug("Query Complete!")
    logger.debug("Total Time: %f", total)
    if total > 1.0:  # 记录执行时间超过1秒的查询
        logger.warning("Long running query: %s", statement)
        logger.warning("Execution time: %f", total)

async def get_db():
    """异步数据库会话依赖"""
    session = SessionLocal()
    try:
        yield session
    except Exception as e:
        logger.error(f"Database session error: {str(e)}")
        await asyncio.sleep(0)  # 让出控制权
        raise
    finally:
        session.close()
