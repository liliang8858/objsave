# ObSave API Documentation

## Overview

ObSave provides a RESTful API for object storage operations. This document describes the available endpoints and their usage.

## Authentication

All API requests require authentication using an API key. Include the API key in the request header:

```
Authorization: Bearer your-api-key
```

## API Endpoints

### Objects

#### Upload an Object

```http
POST /api/v1/objects
```

Request body (multipart/form-data):
- `file`: The file to upload
- `metadata` (optional): JSON object with custom metadata

Response:
```json
{
    "object_id": "string",
    "size": integer,
    "content_type": "string",
    "metadata": {},
    "created_at": "string"
}
```

#### Get an Object

```http
GET /api/v1/objects/{object_id}
```

Response: Object content with appropriate content-type header

#### Delete an Object

```http
DELETE /api/v1/objects/{object_id}
```

Response:
```json
{
    "status": "success",
    "message": "Object deleted successfully"
}
```

### Query

#### Search Objects

```http
POST /api/v1/query
```

Request body:
```json
{
    "query": "$.metadata[?(@.type=='document')]",
    "limit": 10,
    "offset": 0
}
```

Response:
```json
{
    "objects": [...],
    "total": integer,
    "limit": integer,
    "offset": integer
}
```

## Error Handling

The API uses standard HTTP status codes and returns error details in JSON format:

```json
{
    "error": {
        "code": "string",
        "message": "string",
        "details": {}
    }
}
```

Common status codes:
- 200: Success
- 400: Bad Request
- 401: Unauthorized
- 404: Not Found
- 500: Internal Server Error

## Rate Limiting

API requests are limited to 1000 requests per hour per API key. Rate limit information is included in response headers:

```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1640995200
```
