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

**成功 (200)**
```json
{
  "code": 0,
  "data": {
    "host": "https://cloud.langfuse.com",
    "public_key": "pk-lf-...",
    "secret_key": "sk-lf-...",
    "tenant_id": "69736047aca811efb21c0242ac120006"
  },
  "message": "success"
}
```

**失败 - 参数缺失 (200)**
```json
{
  "code": 102,
  "message": "Missing required fields"
}
```

**失败 - 无效的 Langfuse Keys (200)**
```json
{
  "code": 102,
  "message": "Invalid Langfuse keys"
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

**成功 (200)**
```json
{
  "code": 0,
  "data": {
    "host": "https://cloud.langfuse.com",
    "project_id": "clxxxxxxxxxxxxxxxxxx",
    "project_name": "My Project",
    "public_key": "pk-lf-...",
    "secret_key": "sk-lf-...",
    "tenant_id": "69736047aca811efb21c0242ac120006"
  },
  "message": "success"
}
```

**未找到记录 (200)**
```json
{
  "code": 0,
  "data": null,
  "message": "Have not record any Langfuse keys."
}
```

**失败 - 无效的 Langfuse Keys (200)**
```json
{
  "code": 102,
  "message": "Invalid Langfuse keys loaded"
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

**成功 (200)**
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

**未找到记录 (200)**
```json
{
  "code": 0,
  "data": null,
  "message": "Have not record any Langfuse keys."
}
```

---

