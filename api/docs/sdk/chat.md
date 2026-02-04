# Chat API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建对话 (Create Chat)

创建一个新的对话对话 (Chat/Chat)。

- **URL**: `/chats`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 对话名称 |
| `avatar` | string | 否 | 头像 (Base64 或 URL) |
| `dataset_ids` | list[string] | 否 | 关联的知识库 ID 列表 |
| `llm` | object | 否 | LLM 配置 (包含 model_name 等) |
| `prompt` | object | 否 | 提示词与检索配置 (包含 prompt, variables, top_n 等) |
| `description` | string | 否 | 描述信息 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chats" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Chat",
           "avatar": "",
           "dataset_ids": ["kb_123"],
           "llm": {
               "model_name": "gpt-3.5-turbo"
           },
           "prompt": {
               "prompt": "You are a helpful Chat...",
               "variables": [{"key": "knowledge", "optional": false}],
               "opener": "Hi!",
               "show_quote": true,
               "top_n": 6
           }
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "chat_xxx",
    "name": "My Chat",
    "avatar": "",
    "tenant_id": "tenant_1",
    "dataset_ids": ["kb_123"],
    "llm": {
        "model_name": "gpt-3.5-turbo"
    },
    "prompt": {
        "prompt": "You are a helpful Chat...",
        "variables": [{"key": "knowledge", "optional": false}],
        "opener": "Hi!",
        "show_quote": true,
        "top_n": 6,
        "similarity_threshold": 0.2,
        "keywords_similarity_weight": 0.7,
        "rerank_model": ""
    },
    "create_time": 1700000000,
    "update_time": 1700000000
  },
  "message": "success"
}
```

---

## 2. 更新对话 (Update Chat)

更新现有的对话对话配置。

- **URL**: `/chats/<chat_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 对话名称 |
| `avatar` | string | 否 | 头像 |
| `dataset_ids` | list[string] | 否 | 知识库 ID 列表 |
| `llm` | object | 否 | LLM 配置 |
| `prompt` | object | 否 | 提示词与检索配置 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/chats/chat_xxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "Updated Name",
           "prompt": {
               "opener": "Hello!"
           }
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": null,
  "message": "success"
}
```

---

## 3. 删除对话 (Delete Chats)

删除一个或多个对话。

- **URL**: `/chats`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ids` | list[string] | 否 | 要删除的对话 ID 列表 (若为空则删除所有) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/chats" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["chat_xxx"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": null,
  "message": "success"
}
```

---

## 4. 获取对话列表 (List Chats)

列出所有对话。

- **URL**: `/chats`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 30) |
| `orderby` | string | 否 | 排序字段 (默认 create_time) |
| `desc` | boolean | 否 | 是否降序 (默认 true) |
| `id` | string | 否 | 按 ID 筛选 |
| `name` | string | 否 | 按名称筛选 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/chats?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "chat_xxx",
      "name": "My Chat",
      "dataset_ids": [],
      "llm": {
          "model_name": "gpt-3.5-turbo"
      },
      "prompt": {
          "prompt": "You are a helpful Chat...",
          "opener": "Hi!",
          "variables": [{"key": "knowledge", "optional": false}]
      },
      "create_time": 1700000000
    }
  ],
  "message": "success"
}
```

