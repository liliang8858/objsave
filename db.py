import sqlite3
import json
import os
import uuid
from sqlalchemy import Column, String, Integer, LargeBinary, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

DATABASE = 'object_storage.db'

# 确保数据库目录存在
os.makedirs('data', exist_ok=True)

# SQLite数据库路径
DATABASE_URL = 'sqlite:///./data/objects.db'

# 创建基础模型类
Base = declarative_base()

class ObjectStorage(Base):
    """对象存储模型"""
    __tablename__ = 'objects'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, index=True)
    content = Column(LargeBinary)
    content_type = Column(String)
    size = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)

# 创建数据库引擎
engine = create_engine(DATABASE_URL, connect_args={'check_same_thread': False})

# 创建会话工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建数据库表
def init_db():
    """初始化数据库"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS objects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT NOT NULL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()
    Base.metadata.create_all(bind=engine)

def add_object(data):
    """存储 JSON 对象到数据库"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO objects (data) VALUES (?)
    ''', (json.dumps(data),))
    conn.commit()
    conn.close()

def get_object(object_id):
    """根据 ID 查询 JSON 对象"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM objects WHERE id = ?', (object_id,))
    obj = cursor.fetchone()
    conn.close()
    if obj:
        return json.loads(obj[1])
    return None

def get_all_objects():
    """获取所有存储的 JSON 对象"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM objects')
    rows = cursor.fetchall()
    conn.close()
    return [{"id": row[0], "data": json.loads(row[1]), "timestamp": row[2]} for row in rows]

def get_objects_by_field(field, value):
    """根据 JSON 字段进行过滤查询"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    query = f'''
    SELECT * FROM objects WHERE json_extract(data, '$.{field}') = ?
    '''
    cursor.execute(query, (value,))
    rows = cursor.fetchall()
    conn.close()
    return [{"id": row[0], "data": json.loads(row[1]), "timestamp": row[2]} for row in rows]

def update_object(object_id, new_data):
    """更新指定 ID 的 JSON 对象"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''
    UPDATE objects SET data = ? WHERE id = ?
    ''', (json.dumps(new_data), object_id))
    conn.commit()
    conn.close()

def delete_object(object_id):
    """删除指定 ID 的 JSON 对象"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM objects WHERE id = ?', (object_id,))
    conn.commit()
    conn.close()

# 获取数据库会话
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
