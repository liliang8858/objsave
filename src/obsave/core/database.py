from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import logging
import time
import asyncio
from contextlib import contextmanager, asynccontextmanager
import multiprocessing

logger = logging.getLogger(__name__)

# 系统配置
MAX_WORKERS = min(32, multiprocessing.cpu_count() * 4)
DB_POOL_SIZE = MAX_WORKERS * 2
DB_MAX_OVERFLOW = MAX_WORKERS
DB_POOL_TIMEOUT = 30
DB_POOL_RECYCLE = 3600

# 数据库配置
DATABASE_URL = "sqlite:///./example.db"

# 创建数据库引擎
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT,
    pool_recycle=DB_POOL_RECYCLE,
    connect_args={
        "check_same_thread": False,
        "timeout": 30
    }
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
    if total > 1.0:
        logger.warning("Long running query: %s", statement)
        logger.warning("Execution time: %f", total)

@asynccontextmanager
async def get_async_session():
    """异步数据库会话上下文管理器"""
    session = SessionLocal()
    try:
        yield session
    except Exception as e:
        logger.error(f"Database session error: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

async def get_db():
    """异步数据库会话依赖"""
    async with get_async_session() as session:
        try:
            yield session
        except Exception as e:
            logger.error(f"Database session error: {str(e)}")
            raise
