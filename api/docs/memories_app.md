# Memory API 文档

**Base URL**: `http://localhost:9380/v1/memories`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建 Memory (Create Memory)

创建一个新的 Memory。

- **URL**: `/`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | Memory 名称 (非空, 长度限制) |
| `memory_type` | list[string] | 是 | Memory 类型列表 (支持: raw, semantic, episodic, procedural) |
| `embd_id` | string | 是 | Embedding 模型 ID |
| `llm_id` | string | 是 | LLM 模型 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/memories" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Memory",
           "memory_type": ["raw", "semantic"],
           "embd_id": "embd_123",
           "llm_id": "llm_123"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "mem_xxx",
    "name": "My Memory",
    "memory_type": ["raw", "semantic"],
    "embd_id": "embd_123",
    "llm_id": "llm_123",
    "create_time": 1700000000,
    "update_time": 1700000000
  },
  "message": "success"
}
```

---

## 2. 更新 Memory (Update Memory)

更新指定 Memory 的配置。

- **URL**: `/<memory_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | Memory 名称 |
| `permissions` | string | 否 | 权限设置 (支持 TenantPermission 枚举值) |
| `llm_id` | string | 否 | LLM 模型 ID |
| `embd_id` | string | 否 | Embedding 模型 ID (非空 Memory 不可更新) |
| `memory_type` | list[string] | 否 | Memory 类型列表 (非空 Memory 不可更新) |
| `memory_size` | int | 否 | Memory 大小限制 (Bytes) |
| `forgetting_policy` | string | 否 | 遗忘策略 (如: FIFO) |
| `temperature` | float | 否 | 温度系数 [0, 1] |
| `avatar` | string | 否 | 头像 |
| `description` | string | 否 | 描述 |
| `system_prompt` | string | 否 | 系统提示词 |
| `user_prompt` | string | 否 | 用户提示词 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/memories/mem_xxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "Updated Memory Name",
           "temperature": 0.7
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "mem_xxx",
    "name": "Updated Memory Name",
    "temperature": 0.7,
    "memory_type": ["raw", "semantic"]
  },
  "message": "success"
}
```

---

## 3. 删除 Memory (Delete Memory)

删除指定的 Memory。

- **URL**: `/<memory_id>`
- **Method**: `DELETE`

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/memories/mem_xxx" \
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

## 4. 获取 Memory 列表 (List Memory)

获取 Memory 列表，支持过滤和分页。

- **URL**: `/`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tenant_id` | list[string] | 否 | Tenant ID 列表 |
| `memory_type` | list[string] | 否 | Memory 类型列表 |
| `storage_type` | string | 否 | 存储类型 |
| `keywords` | string | 否 | 搜索关键词 |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 50) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/memories?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "memory_list": [
      {
        "id": "mem_xxx",
        "name": "My Memory",
        "memory_type": ["raw"]
      }
    ],
    "total_count": 1
  },
  "message": "success"
}
```

---

## 5. 获取 Memory 配置 (Get Memory Config)

获取指定 Memory 的详细配置信息。

- **URL**: `/<memory_id>/config`
- **Method**: `GET`

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/memories/mem_xxx/config" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "mem_xxx",
    "name": "My Memory",
    "owner_name": "User Name",
    "memory_type": ["raw"]
  },
  "message": "success"
}
```

---

## 6. 获取 Memory 详情 (Get Memory Detail)

获取 Memory 详情，包括其中的消息记录等。

- **URL**: `/<memory_id>`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | list[string] | 否 | 关联的 Agent ID 列表 |
| `keywords` | string | 否 | 消息搜索关键词 |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 50) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/memories/mem_xxx?page=1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "messages": {
        "message_list": [],
        "total": 0
    },
    "storage_type": "graph"
  },
  "message": "success"
}
```

