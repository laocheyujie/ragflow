# Agent Management API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取 Agent 列表 (List Agents)

获取当前用户的 Agent 列表。

- **URL**: `/agents`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 30) |
| `orderby` | string | 否 | 排序字段 (默认 "update_time") |
| `desc` | boolean | 否 | 是否降序 (默认 true) |
| `id` | string | 否 | 按 Agent ID 筛选 |
| `title` | string | 否 | 按 Agent 标题筛选 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/agents?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "agent_id_1",
      "title": "Agent Title",
      "dsl": { "..." },
      "create_time": 1700000000,
      "update_time": 1700000000
    }
  ],
  "message": "success"
}
```

---

## 2. 创建 Agent (Create Agent)

创建一个新的 Agent。

- **URL**: `/agents`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `title` | string | 是 | Agent 标题 |
| `dsl` | object | 是 | Agent 的 DSL 定义 (JSON 对象或 JSON 字符串) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agents" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "title": "My New Agent",
           "dsl": {
             "components": { "..." },
             "connections": [ ... ]
           }
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

## 3. 更新 Agent (Update Agent)

更新指定 Agent 的信息。

- **URL**: `/agents/<agent_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `title` | string | 否 | 新的 Agent 标题 |
| `dsl` | object | 否 | 新的 DSL 定义 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/agents/agent_id_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "title": "Updated Agent Title"
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

## 4. 删除 Agent (Delete Agent)

删除指定的 Agent。

- **URL**: `/agents/<agent_id>`
- **Method**: `DELETE`

### 请求参数

无 (Agent ID 在 URL 中)

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/agents/agent_id_1" \
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

## 5. Webhook 触发 (Webhook Trigger)

通过 Webhook 触发 Agent 运行。支持的 HTTP 方法取决于 Agent DSL 中的 Webhook 配置。

- **URL**: `/webhook/<agent_id>`
- **Method**: `POST`, `GET`, `PUT`, `PATCH`, `DELETE`, `HEAD`

### 请求参数

请求参数 (Query, Headers, Body) 将根据 Agent DSL 中 Webhook 组件的配置进行解析和传递。

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/webhook/agent_id_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "input": "some input"
         }'
```

### 响应示例
响应内容取决于 Agent DSL 中的配置。如果是流式响应 (SSE)，则返回数据流；如果是立即返回，则返回配置的 JSON 响应。

```json
{
  "message": "Agent execution result...",
  "success": true,
  "code": 200
}
```

---

## 6. Webhook 追踪 (Webhook Trace)

获取 Agent Webhook 运行的追踪日志。

- **URL**: `/webhook_trace/<agent_id>`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `since_ts` | float | 否 | 起始时间戳，用于增量获取日志 |
| `webhook_id` | string | 否 | 特定的 Webhook 执行 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/webhook_trace/agent_id_1?since_ts=1700000000" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "webhook_id": "encoded_id_xxx",
    "events": [
      {
        "ts": 1700000001.5,
        "event": "node_start",
        "data": { "..." }
      },
      {
        "ts": 1700000002.0,
        "event": "finished",
        "success": true
      }
    ],
    "next_since_ts": 1700000002.0,
    "finished": true
  },
  "message": "success"
}
```

