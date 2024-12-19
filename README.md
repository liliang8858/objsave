# ObjSave - High-Performance Object Storage Service

ObjSave is a high-performance object storage service built on FastAPI, providing robust capabilities for object storage, retrieval, updates, and querying. It is designed for high performance, reliability, and ease of use.

English | [简体中文](README_zh.md)

## Features

- **High Performance** - Asynchronous processing, memory caching, support for massive concurrency
- **Object Storage** - Store and retrieve objects of any type
- **Smart Querying** - Complex queries supported via JSONPath
- **Monitoring** - Comprehensive system monitoring and performance metrics
- **Security** - Access control and data encryption
- **Real-time Monitoring** - Real-time system status monitoring and alerts

## Version Information

- Version: 1.0.0
- Release Date: 2024-12-19
- Python Version Required: >=3.8

## Quick Start

### Environment Setup

```bash
# Create virtual environment
python -m venv objectstorage_env

# Activate virtual environment
# Windows
objectstorage_env\Scripts\activate
# Linux/Mac
source objectstorage_env/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Start Service

```bash
python app.py
```

### Access Service

- API Documentation: http://localhost:8000/docs
- Health Check: http://localhost:8000/objsave/health
- Metrics: http://localhost:8000/objsave/metrics

## API Endpoints

### Object Operations
- `POST /objsave/objects` - Store object
- `GET /objsave/objects/{id}` - Retrieve object
- `PUT /objsave/objects/{id}` - Update object
- `DELETE /objsave/objects/{id}` - Delete object

### Query Interfaces
- `POST /objsave/query/json` - JSON object query
- `GET /objsave/query/metadata` - Metadata query

### Monitoring Interfaces
- `GET /objsave/health` - System health status
- `GET /objsave/metrics` - Performance metrics

## Monitoring Metrics

### System Metrics
- CPU Usage
- Memory Usage
- Disk Usage
- Process Status

### Storage Metrics
- Operation Count and Latency
- Cache Hit Rate
- Error Rate Statistics
- Data Size Statistics

### HTTP Metrics
- Request Rate and Error Rate
- Latency Distribution
- Status Code Statistics
- Response Size Statistics

## Configuration

Key Configuration Items:
- `PORT`: Service port (default: 8000)
- `HOST`: Service address (default: 0.0.0.0)
- `STORAGE_PATH`: Storage path
- `MAX_OBJECT_SIZE`: Maximum object size
- `CACHE_SIZE`: Cache size

## Performance Optimization

1. Asynchronous Processing:
   - Async IO
   - Background Task Processing
   - Batch Operation Optimization

2. Caching Strategy:
   - Memory Cache
   - Hot Data Optimization
   - Cache Preheating

3. Storage Optimization:
   - Data Compression
   - Batch Writing
   - Delayed Writing

## Monitoring Alerts

Supports multi-level alerts:
- Critical: Severe issues requiring immediate attention
- Warning: Potential issues requiring attention
- Info: Informational notifications

## Development Roadmap

1. Short-term Plans:
   - Distributed Storage Support
   - Data Compression Optimization
   - Additional Query Features

2. Long-term Plans:
   - Cluster Support
   - Data Backup and Recovery
   - More Storage Backends

## Contributing

We welcome all forms of contributions, whether it's new features, documentation improvements, or bug reports!

## License

This project is licensed under the MIT License. For commercial use, please contact: sblig3@gmail.com

See the [LICENSE](LICENSE) file for details.
