# 对象存储服务 API 文档

## 概述
本服务提供轻量级本地对象存储的 HTTP 接口，支持文件的上传、下载、列出和删除操作。

## 基本信息
- **服务标题**: 对象存储服务
- **描述**: 轻量级本地对象存储HTTP服务
- **默认运行地址**: `http://localhost:8000`

## 接口详情

### 1. 文件上传 `/upload`
- **方法**: POST
- **功能**: 上传文件到对象存储服务

#### 请求参数
- `file`: 必填，待上传的文件
  - 支持任意类型和大小的文件

#### 响应
- **成功响应** (200 OK):
  ```json
  {
    "id": "唯一标识符",
    "name": "文件名",
    "content_type": "文件MIME类型",
    "size": "文件大小（字节）",
    "created_at": "创建时间"
  }
  ```

#### 示例
```bash
curl -X POST -F "file=@/path/to/your/file" http://localhost:8000/upload
```

### 2. 文件下载 `/download/{object_id}`
- **方法**: GET
- **功能**: 根据对象ID下载文件

#### 路径参数
- `object_id`: 必填，文件的唯一标识符

#### 响应
- **成功响应** (200 OK):
  ```json
  {
    "file_name": "文件名",
    "content_type": "文件MIME类型",
    "content": "文件内容（Base64编码）"
  }
  ```
- **错误响应** (404 Not Found): 对象不存在

#### 示例
```bash
curl http://localhost:8000/download/{object_id}
```

### 3. 列出对象 `/list`
- **方法**: GET
- **功能**: 列出存储的对象，支持分页

#### 查询参数
- `limit`: 可选，返回对象数量限制（默认100）
- `offset`: 可选，分页起始偏移量（默认0）

#### 响应
- **成功响应** (200 OK):
  ```json
  [
    {
      "id": "对象ID",
      "name": "文件名",
      "content_type": "文件MIME类型",
      "size": "文件大小（字节）",
      "created_at": "创建时间"
    },
    ...
  ]
  ```

#### 示例
```bash
curl "http://localhost:8000/list?limit=10&offset=0"
```

### 4. 删除对象 `/delete/{object_id}`
- **方法**: DELETE
- **功能**: 根据对象ID删除文件

#### 路径参数
- `object_id`: 必填，文件的唯一标识符

#### 响应
- **成功响应** (200 OK):
  ```json
  {
    "message": "对象删除成功"
  }
  ```
- **错误响应** (404 Not Found): 对象不存在

#### 示例
```bash
curl -X DELETE http://localhost:8000/delete/{object_id}
```

## 错误处理
- 404 Not Found: 请求的对象不存在
- 422 Unprocessable Entity: 请求参数无效

## 运行服务
```bash
python app.py
```
默认监听 `0.0.0.0:8000`

## 注意事项
1. 所有文件使用UUID作为唯一标识符
2. 支持任意类型和大小的文件上传
3. 文件存储在本地SQLite数据库中

## 技术栈
- FastAPI
- SQLAlchemy
- Pydantic
- SQLite
