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

## JSON 对象管理接口

### 5. 上传 JSON 对象 `/upload/json`
- **方法**: POST
- **功能**: 上传单个 JSON 对象到存储服务

#### 请求参数
```json
{
  "data": {}, // 必填，任意 JSON 对象
  "name": "可选的对象名称", // 可选
  "id": "可选的对象ID" // 可选
}
```

#### 响应
- **成功响应** (200 OK):
  ```json
  {
    "id": "唯一标识符",
    "name": "对象名称",
    "content_type": "application/json",
    "size": "对象大小（字节）",
    "created_at": "创建时间"
  }
  ```

#### 示例
```bash
curl -X POST http://localhost:8000/upload/json \
     -H "Content-Type: application/json" \
     -d '{"data": {"key": "value"}, "name": "example_object"}'
```

### 6. 批量上传 JSON 对象 `/upload/json/batch`
- **方法**: POST
- **功能**: 一次性上传多个 JSON 对象

#### 请求参数
```json
[
  {
    "data": {}, // 必填，第一个 JSON 对象
    "name": "可选的对象名称" // 可选
  },
  {
    "data": {}, // 必填，第二个 JSON 对象
    "name": "可选的对象名称" // 可选
  }
]
```

#### 响应
- **成功响应** (200 OK):
  ```json
  [
    {
      "id": "第一个对象唯一标识符",
      "name": "对象名称",
      "content_type": "application/json",
      "size": "对象大小（字节）",
      "created_at": "创建时间"
    },
    {
      "id": "第二个对象唯一标识符",
      "name": "对象名称",
      "content_type": "application/json",
      "size": "对象大小（字节）",
      "created_at": "创建时间"
    }
  ]
  ```

#### 示例
```bash
curl -X POST http://localhost:8000/upload/json/batch \
     -H "Content-Type: application/json" \
     -d '[
           {"data": {"key1": "value1"}, "name": "object1"},
           {"data": {"key2": "value2"}, "name": "object2"}
         ]'
```

### 7. 更新 JSON 对象 `/update/json/{object_id}`
- **方法**: PUT
- **功能**: 更新指定 ID 的 JSON 对象

#### 路径参数
- `object_id`: 必填，待更新对象的唯一标识符

#### 请求参数
```json
{
  "data": {}, // 必填，新的 JSON 对象内容
  "name": "可选的新对象名称" // 可选
}
```

#### 响应
- **成功响应** (200 OK):
  ```json
  {
    "id": "对象唯一标识符",
    "name": "更新后的对象名称",
    "content_type": "application/json",
    "size": "新对象大小（字节）",
    "created_at": "创建时间"
  }
  ```
- **错误响应** (404 Not Found): 对象不存在

#### 示例
```bash
curl -X PUT http://localhost:8000/update/json/{object_id} \
     -H "Content-Type: application/json" \
     -d '{"data": {"new_key": "new_value"}, "name": "updated_object"}'
```

### 8. JSON 对象查询 `/query/json`
- **方法**: POST
- **功能**: 根据 JSONPath 查询和过滤 JSON 对象

#### 请求参数
```json
{
  "jsonpath": "必填，JSONPath查询表达式", 
  "value": "可选，精确匹配的值",
  "operator": "可选，比较运算符（默认为 'eq'）"
}
```

#### 支持的运算符
- `eq`：等于（默认）
- `gt`：大于
- `lt`：小于
- `ge`：大于等于
- `le`：小于等于
- `contains`：包含

#### 查询参数
- `limit`：可选，返回结果数量限制（默认 100）
- `offset`：可选，分页偏移量（默认 0）

#### 响应
- **成功响应** (200 OK):
  ```json
  [
    {
      "id": "对象唯一标识符",
      "name": "对象名称",
      "content_type": "application/json",
      "size": "对象大小（字节）",
      "created_at": "创建时间"
    }
  ]
  ```

#### JSONPath 查询示例

1. 查询特定字段值
```bash
curl -X POST http://localhost:8000/query/json \
     -H "Content-Type: application/json" \
     -d '{
           "jsonpath": "$.user.name", 
           "value": "John Doe"
         }'
```

2. 查询数组中的元素
```bash
curl -X POST http://localhost:8000/query/json \
     -H "Content-Type: application/json" \
     -d '{
           "jsonpath": "$.tags[*]", 
           "value": "important",
           "operator": "contains"
         }'
```

3. 数值比较
```bash
curl -X POST http://localhost:8000/query/json \
     -H "Content-Type: application/json" \
     -d '{
           "jsonpath": "$.age", 
           "value": 30,
           "operator": "gt"
         }'
```

#### JSONPath 语法快速参考
- `$`: 根对象
- `.`: 子对象
- `[*]`: 所有数组元素
- `[index]`: 数组特定索引
- `..`: 递归搜索

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
4. JSON 对象支持任意复杂度的 JSON 结构
5. JSON 对象默认使用 `application/json` 内容类型
6. 批量上传支持同时提交多个 JSON 对象
7. JSON 对象查询支持复杂的 JSONPath 表达式
8. 支持多种比较运算符，适用于不同类型的数据比较
9. 查询结果返回匹配对象的元数据，而非完整 JSON 内容

## 技术栈
- FastAPI
- SQLAlchemy
- Pydantic
- SQLite
