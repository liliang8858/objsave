# ObjSave - 高性能对象存储服务

ObjSave 是一个基于 FastAPI 的高性能对象存储服务，提供对象的存储、检索、更新和查询功能。

## 功能特性

- 对象存储：支持任意类型对象的存储和检索
- JSON 查询：支持基于 JSONPath 的复杂查询
- 高性能：异步处理，内存缓存
- 监控指标：全面的系统监控和性能指标
- 健康检查：实时系统状态监控和告警
- 安全性：支持访问控制和数据加密

## 版本信息

- 版本号：1.0.0
- 发布日期：2024-12-19
- Python 版本要求：>=3.8

## 快速开始

1. 安装依赖：
```bash
pip install -r requirements.txt
```

2. 启动服务：
```bash
python app.py
```

3. 访问服务：
- API 文档：http://localhost:8000/docs
- 健康检查：http://localhost:8000/objsave/health
- 指标监控：http://localhost:8000/objsave/metrics

## API 接口

### 对象操作
- POST /objsave/objects - 存储对象
- GET /objsave/objects/{id} - 获取对象
- PUT /objsave/objects/{id} - 更新对象
- DELETE /objsave/objects/{id} - 删除对象

### 查询接口
- POST /objsave/query/json - JSON对象查询
- GET /objsave/query/metadata - 元数据查询

### 监控接口
- GET /objsave/health - 系统健康状态
- GET /objsave/metrics - 性能指标

## 监控指标

### 系统指标
- CPU使用率
- 内存使用情况
- 磁盘使用情况
- 进程状态

### 存储指标
- 操作计数和延迟
- 缓存命中率
- 错误率统计
- 数据大小统计

### HTTP指标
- 请求率和错误率
- 延迟分布
- 状态码统计
- 响应大小统计

### 队列和工作流
- 队列使用情况
- 工作流成功率
- 处理延迟统计

## 配置说明

主要配置项：
- PORT: 服务端口号（默认：8000）
- HOST: 服务地址（默认：0.0.0.0）
- STORAGE_PATH: 存储路径
- MAX_OBJECT_SIZE: 最大对象大小
- CACHE_SIZE: 缓存大小

## 性能优化

1. 异步处理：
   - 使用异步IO
   - 后台任务处理
   - 批量操作优化

2. 缓存策略：
   - 内存缓存
   - 热点数据优化
   - 缓存预热

3. 存储优化：
   - 数据压缩
   - 批量写入
   - 延迟写入

## 监控告警

支持多级告警：
- Critical: 严重问题，需要立即处理
- Warning: 潜在问题，需要关注
- Info: 提示信息

告警指标：
- 系统资源使用率
- 错误率
- 响应时间
- 队列积压
- 存储容量

## 开发计划

1. 短期计划：
   - 分布式存储支持
   - 数据压缩优化
   - 更多查询功能

2. 长期计划：
   - 集群支持
   - 数据备份恢复
   - 更多存储后端

## 贡献指南

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License
