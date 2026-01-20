# Langfuse API 文档

**Base URL**: `http://localhost:9380/v1/langfuse`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 设置/更新 API Key (Set API Key)

设置或更新 Langfuse 的 API 配置信息。

- **URL**: `/api_key`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `secret_key` | string | 是 | Langfuse Secret Key |
| `public_key` | string | 是 | Langfuse Public Key |
| `host` | string | 是 | Langfuse Host URL |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/langfuse/api_key" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "secret_key": "sk-lf-...",
           "public_key": "pk-lf-...",
           "host": "https://cloud.langfuse.com"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "tenant_id": "tenant_123",
    "secret_key": "sk-lf-...",
    "public_key": "pk-lf-...",
    "host": "https://cloud.langfuse.com"
  },
  "message": "success"
}
```

---

## 2. 获取 API Key (Get API Key)

获取当前配置的 Langfuse API 信息。

- **URL**: `/api_key`
- **Method**: `GET`

### 请求参数 (Query)

无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/langfuse/api_key" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "tenant_id": "tenant_123",
    "secret_key": "sk-lf-...",
    "public_key": "pk-lf-...",
    "host": "https://cloud.langfuse.com",
    "project_id": "project_abc",
    "project_name": "My Project"
  },
  "message": "success"
}
```

---

## 3. 删除 API Key (Delete API Key)

删除当前配置的 Langfuse API 信息。

- **URL**: `/api_key`
- **Method**: `DELETE`

### 请求参数 (Body)

无

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/langfuse/api_key" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

---

