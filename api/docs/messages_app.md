# Message API 文档

**Base URL**: `http://localhost:9380/v1/messages`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 添加消息 (Add Message)

向指定的 Memory 添加用户输入和 Agent 回复的消息记录。

- **URL**: `/`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `memory_id` | list[string] | 是 | 记忆 ID 列表 |
| `agent_id` | string | 是 | Agent ID |
| `session_id` | string | 是 | 会话 ID |
| `user_input` | string | 是 | 用户输入内容 |
| `agent_response` | string | 是 | Agent 回复内容 |
| `user_id` | string | 否 | 用户 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/messages" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "memory_id": ["mem_123", "mem_456"],
           "agent_id": "agent_1",
           "session_id": "session_abc",
           "user_input": "Hello",
           "agent_response": "Hi there!"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "Successfully added to memories."
}
```

---

## 2. 删除/遗忘消息 (Forget Message)

将指定消息标记为“遗忘”状态。

- **URL**: `/<memory_id>:<message_id>`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `memory_id` | string | 是 | 记忆 ID |
| `message_id` | int | 是 | 消息 ID (整数) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/messages/mem_123:1001" \
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

## 3. 更新消息状态 (Update Message)

更新消息的状态（例如标记为有效或无效）。

- **URL**: `/<memory_id>:<message_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `memory_id` | string | 是 | 记忆 ID |
| `message_id` | int | 是 | 消息 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `status` | boolean | 是 | 状态值 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/messages/mem_123:1001" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "status": false
         }'
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

## 4. 获取消息列表 (Get Messages)

获取指定 Memory 的最近消息记录。

- **URL**: `/`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `memory_id` | list[string] | 是 | 记忆 ID 列表 (可传多个) |
| `agent_id` | string | 否 | Agent ID |
| `session_id` | string | 否 | 会话 ID |
| `limit` | int | 否 | 返回条数限制 (默认 10) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/messages?memory_id=mem_123&limit=5" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "msg_1",
      "role": "user",
      "content": "Hello",
      "create_time": 1700000000
    },
    {
      "id": "msg_2",
      "role": "assistant",
      "content": "Hi there!",
      "create_time": 1700000001
    }
  ],
  "message": "success"
}
```

---

## 5. 搜索消息 (Search Message)

根据 Query 语义搜索相关的历史消息。

- **URL**: `/search`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `memory_id` | list[string] | 是 | 记忆 ID 列表 (可传多个) |
| `query` | string | 是 | 搜索关键词或语句 |
| `similarity_threshold` | float | 否 | 相似度阈值 (默认 0.2) |
| `keywords_similarity_weight` | float | 否 | 关键词相似度权重 (默认 0.7) |
| `top_n` | int | 否 | 返回结果数量 (默认 5) |
| `agent_id` | string | 否 | Agent ID |
| `session_id` | string | 否 | 会话 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/messages/search?memory_id=mem_123&query=Hello" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "msg_1",
      "content": "Hello world",
      "similarity": 0.9
    }
  ],
  "message": "success"
}
```

---

## 6. 获取单条消息内容 (Get Message Content)

获取指定消息的详细内容。

- **URL**: `/<memory_id>:<message_id>/content`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `memory_id` | string | 是 | 记忆 ID |
| `message_id` | int | 是 | 消息 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/messages/mem_123:1001/content" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": 1001,
    "memory_id": "mem_123",
    "content": "Message content here...",
    "role": "user",
    "create_time": "2024-01-01 12:00:00"
  },
  "message": "success"
}
```

