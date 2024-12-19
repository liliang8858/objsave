# 对象存储服务 API 文档

## 概述
本服务提供轻量级本地对象存储的 HTTP 接口，支持文件的上传、下载、列出和删除操作。

## 基本信息
- **服务标题**: 对象存储服务
- **描述**: 轻量级本地对象存储HTTP服务
- **默认运行地址**: `http://localhost:8000`
- **API 前缀**: `/objsave`

## 接口详情

### 1. 文件上传 `/objsave/upload`
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
curl -X POST -F "file=@/path/to/your/file" http://localhost:8000/objsave/upload
```

### 2. 文件下载 `/objsave/download/{object_id}`
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
    "type": "对象类型（JSON对象）",
    "content": "文件内容（Base64编码）"
  }
  ```
- **错误响应** (404 Not Found): 对象不存在

#### 示例
```bash
curl http://localhost:8000/objsave/download/{object_id}
```

### 3. 列出对象 `/objsave/list`
- **方法**: GET
- **功能**: 列出存储的对象，支持分页

#### 查询参数
- `limit`: 可选，返回对象数量限制（默认100）
- `offset`: 可选，分页起始偏移量（默认0）
- `last_id`: 可选，游标分页的最后一个ID

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
curl "http://localhost:8000/objsave/list?limit=10&offset=0"
```

### 4. JSON对象上传 `/objsave/json`
- **方法**: POST
- **功能**: 上传JSON对象

#### 请求体
```json
{
  "type": "对象类型",
  "content": "JSON内容",
  "name": "对象名称（可选）"
}
```

#### 响应
- **成功响应** (200 OK):
  ```json
  {
    "id": "对象唯一标识符",
    "type": "对象类型",
    "content": "JSON内容",
    "created_at": "创建时间"
  }
  ```

### 5. JSON对象批量上传 `/objsave/json/batch`
- **方法**: POST
- **功能**: 批量上传JSON对象

#### 请求体
```json
[
  {
    "type": "对象类型",
    "content": "JSON内容",
    "name": "对象名称（可选）"
  },
  ...
]
```

### 6. JSON对象查询 `/objsave/json/query`
- **方法**: POST
- **功能**: 使用JSONPath查询对象

#### 请求体
```json
{
  "jsonpath": "JSONPath表达式",
  "value": "匹配值（可选）",
  "operator": "比较操作符（默认eq）",
  "type": "对象类型（可选）"
}
```

### 7. 备份管理

#### 7.1 创建备份 `/objsave/backup/create`
- **方法**: POST
- **功能**: 创建系统备份
- **请求参数**: 
  - `backup_name`: 可选，备份名称

#### 7.2 列出备份 `/objsave/backup/list`
- **方法**: GET
- **功能**: 获取所有可用备份

#### 7.3 恢复备份 `/objsave/backup/restore/{backup_name}`
- **方法**: POST
- **功能**: 从指定备份恢复系统

### 8. 监控指标

#### 8.1 系统指标 `/objsave/metrics`
- **方法**: GET
- **功能**: 获取系统性能指标
- **响应**: CPU、内存、磁盘使用等指标

#### 8.2 写入统计 `/objsave/metrics/write`
- **方法**: GET
- **功能**: 获取写入操作统计

## 错误处理
所有 API 使用标准 HTTP 状态码：
- 200: 成功
- 400: 请求参数错误
- 404: 资源不存在
- 408: 请求超时
- 500: 服务器内部错误

错误响应格式：
```json
{
  "detail": "错误描述信息"
}
```

## 性能优化
- 异步 I/O 处理
- 写入队列和批处理
- 缓存优化
- 文件分块上传
- 并发请求限制

## 限制说明
- 最大文件大小: 50MB
- 默认请求超时: 30秒
- 最大并发请求: 根据CPU核心数自动调整
- 写入批次大小: 100条记录
- 缓存容量: 20000条记录
