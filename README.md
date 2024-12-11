# 轻量级对象存储服务

## 功能特性
- 使用SQLite作为本地数据库
- 支持文件上传、下载、列表和删除
- 基于FastAPI构建的HTTP服务
- 自动生成唯一对象标识符
- 支持任意文件类型和大小

## 安装依赖
```bash
pip install -r requirements.txt
```

## 启动服务
```bash
python app.py
```

服务将在 `http://localhost:8000` 运行

## API 接口

### 上传对象
`POST /upload`
- 支持任意文件上传
- 返回对象元数据和唯一ID

### 下载对象
`GET /download/{object_id}`
- 根据对象ID下载文件

### 列出对象
`GET /list`
- 支持分页
- 返回对象元数据列表

### 删除对象
`DELETE /delete/{object_id}`
- 根据对象ID删除文件

## 运行测试
```bash
pytest test_app.py
```

### 测试覆盖的场景
- 文件上传
- 文件下载
- 对象列表
- 对象删除
- 大文件处理
- 错误场景处理

## 注意事项
- 默认数据存储在 `data/objects.db`
- 文件内容直接存储在SQLite数据库中
- 生产环境建议添加认证和大文件处理机制

## 技术栈
- FastAPI
- SQLAlchemy
- SQLite
- Uvicorn
