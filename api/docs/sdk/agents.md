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
      "avatar": null,
      "user_id": "user_id_xxx",
      "title": "Agent Title",
      "permission": "me",
      "description": "Agent description",
      "canvas_type": null,
      "canvas_category": "agent_canvas",
      "dsl": {
        "components": {},
        "connections": []
      },
      "create_time": 1700000000000,
      "create_date": "2023-11-14 22:13:20",
      "update_time": 1700000000000,
      "update_date": "2023-11-14 22:13:20"
    }
  ]
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

### 成功响应
```json
{
  "code": 0,
  "message": "success",
  "data": true
}
```

### 失败响应 - 标题已存在
```json
{
  "code": 102,
  "message": "Agent with title My New Agent already exists."
}
```

### 失败响应 - 缺少必填参数
```json
{
  "code": 101,
  "message": "No DSL data in request.",
  "data": false
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

### 成功响应
```json
{
  "code": 0,
  "message": "success",
  "data": true
}
```

### 失败响应 - 无权限操作
```json
{
  "code": 103,
  "message": "Only owner of canvas authorized for this operation.",
  "data": false
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

### 成功响应
```json
{
  "code": 0,
  "message": "success",
  "data": true
}
```

### 失败响应 - 无权限操作
```json
{
  "code": 103,
  "message": "Only owner of canvas authorized for this operation.",
  "data": false
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

响应内容取决于 Agent DSL 中 Webhook 组件的 `execution_mode` 配置：

#### 立即返回模式 (Immediately)
当 `execution_mode` 为 `Immediately` 时，Webhook 会立即返回配置的响应，Agent 在后台异步执行：

```json
{
  "result": "ok"
}
```
> 注意: 响应内容由 DSL 中的 `response.body_template` 配置决定

#### 等待结果模式 (Wait for Result)
当 `execution_mode` 不为 `Immediately` 时，Webhook 会等待 Agent 执行完成后返回结果：

**成功响应**
```json
{
  "message": "Agent execution completed. Here is the result...",
  "success": true,
  "code": 200
}
```

**失败响应**
```json
{
  "code": 400,
  "message": "Error message describing what went wrong",
  "success": false
}
```

### 错误响应示例

**Canvas 不存在**
```json
{
  "code": 100,
  "message": "Canvas not found."
}
```

**Webhook 未配置**
```json
{
  "code": 100,
  "message": "Webhook not configured for this agent."
}
```

**HTTP 方法不允许**
```json
{
  "code": 100,
  "message": "HTTP method 'DELETE' not allowed for this webhook."
}
```

**请求体过大**
```json
{
  "code": 100,
  "message": "Request body too large: 15728640 > 10485760"
}
```

**认证失败**
```json
{
  "code": 100,
  "message": "Invalid token authentication"
}
```

**速率限制**
```json
{
  "code": 100,
  "message": "Too many requests (rate limit exceeded)"
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

#### 初次请求 (未提供 since_ts)
```json
{
  "code": 0,
  "data": {
    "webhook_id": null,
    "events": [],
    "next_since_ts": 1700000000.0,
    "finished": false
  }
}
```

#### 发现新 Webhook 执行
```json
{
  "code": 0,
  "data": {
    "webhook_id": "dGltZXN0YW1wX2hhc2g",
    "events": [],
    "next_since_ts": 1700000001.5,
    "finished": false
  }
}
```

#### 获取执行过程中的事件
```json
{
  "code": 0,
  "data": {
    "webhook_id": "dGltZXN0YW1wX2hhc2g",
    "events": [
      {
        "ts": 1700000001.5,
        "event": "message",
        "data": {
          "content": "Processing your request..."
        }
      },
      {
        "ts": 1700000002.0,
        "event": "message",
        "data": {
          "content": "Analysis complete."
        }
      }
    ],
    "next_since_ts": 1700000002.0,
    "finished": false
  }
}
```

#### 执行完成
```json
{
  "code": 0,
  "data": {
    "webhook_id": "dGltZXN0YW1wX2hhc2g",
    "events": [
      {
        "ts": 1700000003.0,
        "event": "finished",
        "elapsed_time": 2.5,
        "success": true
      }
    ],
    "next_since_ts": 1700000003.0,
    "finished": true
  }
}
```

#### 执行出错
```json
{
  "code": 0,
  "data": {
    "webhook_id": "dGltZXN0YW1wX2hhc2g",
    "events": [
      {
        "ts": 1700000002.5,
        "event": "error",
        "message": "Connection timeout",
        "error_type": "TimeoutError"
      },
      {
        "ts": 1700000002.6,
        "event": "finished",
        "elapsed_time": 1.1,
        "success": false
      }
    ],
    "next_since_ts": 1700000002.6,
    "finished": true
  }
}
```

#### 无追踪数据
```json
{
  "code": 0,
  "data": {
    "webhook_id": null,
    "events": [],
    "next_since_ts": 1700000000.0,
    "finished": false
  }
}
```

