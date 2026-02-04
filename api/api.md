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


---


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
    "description": "A helpful Assistant",
    "avatar": "",
    "tenant_id": "tenant_1",
    "language": "English",
    "dataset_ids": ["kb_123"],
    "llm": {
      "model_name": "gpt-3.5-turbo",
      "temperature": 0.1,
      "top_p": 0.3,
      "frequency_penalty": 0.7,
      "presence_penalty": 0.4,
      "max_tokens": 512
    },
    "prompt": {
      "prompt": "You are a helpful Chat...",
      "variables": [{"key": "knowledge", "optional": false}],
      "opener": "Hi!",
      "show_quote": true,
      "empty_response": "Sorry! No relevant content was found in the knowledge base!",
      "tts": false,
      "refine_multiturn": true,
      "similarity_threshold": 0.2,
      "keywords_similarity_weight": 0.7,
      "top_n": 6,
      "rerank_model": ""
    },
    "prompt_type": "simple",
    "do_refer": "1",
    "status": "1",
    "create_time": 1700000000,
    "update_time": 1700000000,
    "create_date": "2024-01-01 00:00:00",
    "update_date": "2024-01-01 00:00:00"
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

**部分删除成功时的响应示例**:
```json
{
  "code": 0,
  "data": {
    "success_count": 2,
    "errors": ["Assistant(chat_xxx) not found."]
  },
  "message": "Partially deleted 2 chats with 1 errors"
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
      "description": "A helpful Assistant",
      "avatar": "",
      "tenant_id": "tenant_1",
      "language": "English",
      "datasets": [
        {
          "id": "kb_123",
          "name": "My Dataset",
          "description": "Dataset description",
          "tenant_id": "tenant_1",
          "embd_id": "BAAI/bge-large-zh-v1.5",
          "chunk_num": 100,
          "doc_num": 10,
          "token_num": 50000,
          "parser_id": "naive",
          "permission": "me",
          "similarity_threshold": 0.2,
          "vector_similarity_weight": 0.3,
          "status": "1",
          "create_time": 1700000000,
          "update_time": 1700000000
        }
      ],
      "llm": {
        "model_name": "gpt-3.5-turbo",
        "temperature": 0.1,
        "top_p": 0.3,
        "frequency_penalty": 0.7,
        "presence_penalty": 0.4,
        "max_tokens": 512
      },
      "prompt": {
        "prompt": "You are a helpful Chat...",
        "variables": [{"key": "knowledge", "optional": false}],
        "opener": "Hi!",
        "show_quote": true,
        "empty_response": "Sorry! No relevant content was found in the knowledge base!",
        "tts": false,
        "refine_multiturn": true,
        "similarity_threshold": 0.2,
        "keywords_similarity_weight": 0.7,
        "top_n": 6,
        "rerank_model": ""
      },
      "prompt_type": "simple",
      "do_refer": "1",
      "status": "1",
      "create_time": 1700000000,
      "update_time": 1700000000,
      "create_date": "2024-01-01 00:00:00",
      "update_date": "2024-01-01 00:00:00"
    }
  ],
  "message": "success"
}
```

**注意**: 
- 创建对话接口返回 `dataset_ids`（知识库 ID 列表）
- 获取对话列表接口返回 `datasets`（完整的知识库对象列表）


---


# Dataset Management API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建数据集 (Create Dataset)

创建一个新的数据集 (Knowledge Base)。

- **URL**: `/datasets`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 数据集名称 |
| `avatar` | string | 否 | 数据集头像 (Base64 编码) |
| `description` | string | 否 | 数据集描述 |
| `embedding_model` | string | 否 | 嵌入模型名称 (若省略则使用 Tenant 默认模型) |
| `permission` | string | 否 | 可见性 ('me' 或 'team') |
| `chunk_method` | string | 否 | 切片方法 (默认为 "naive")。可选值: "naive", "book", "email", "laws", "manual", "one", "paper", "picture", "presentation", "qa", "table", "tag" |
| `parser_config` | object | 否 | 解析器配置 (若省略则使用服务端默认配置) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Knowledge Base",
           "permission": "me",
           "chunk_method": "naive"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "name": "My Knowledge Base",
    "avatar": "",
    "tenant_id": "user123456789",
    "language": "English",
    "description": "",
    "embedding_model": "BAAI/bge-large-zh-v1.5",
    "permission": "me",
    "created_by": "user123456789",
    "document_count": 0,
    "token_num": 0,
    "chunk_count": 0,
    "similarity_threshold": 0.2,
    "vector_similarity_weight": 0.3,
    "chunk_method": "naive",
    "pipeline_id": null,
    "parser_config": {
      "pages": [[1, 1000000]],
      "table_context_size": 0,
      "image_context_size": 0,
      "llm_id": "deepseek-chat"
    },
    "pagerank": 0,
    "graphrag_task_id": null,
    "graphrag_task_finish_at": null,
    "raptor_task_id": null,
    "raptor_task_finish_at": null,
    "mindmap_task_id": null,
    "mindmap_task_finish_at": null,
    "status": "1",
    "create_time": 1700000000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-01 12:00:00"
  },
  "message": "success"
}
```

---

## 2. 删除数据集 (Delete Datasets)

删除一个或多个数据集。

- **URL**: `/datasets`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ids` | list[string] | 是 | 要删除的数据集 ID 列表。若为 `null` 则删除所有数据集；若为空数组则不删除任何数据集。 |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/datasets" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["kb_1", "kb_2"]
         }'
```

### 响应示例 (成功)
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

### 响应示例 (部分成功)
```json
{
  "code": 0,
  "data": {
    "success_count": 1,
    "errors": ["Remove document 'doc_123' error for dataset 'kb_2'"]
  },
  "message": "Successfully deleted 1 datasets, 1 failed. Details: Remove document 'doc_123' error for dataset 'kb_2'..."
}
```

---

## 3. 更新数据集 (Update Dataset)

更新指定数据集的信息。

- **URL**: `/datasets/<dataset_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 新的数据集名称 |
| `avatar` | string | 否 | 新的头像 (Base64 编码) |
| `description` | string | 否 | 新的描述 |
| `embedding_model` | string | 否 | 新的嵌入模型名称 |
| `permission` | string | 否 | 新的权限设置 ('me' 或 'team') |
| `chunk_method` | string | 否 | 新的切片方法 |
| `pagerank` | integer | 否 | PageRank 值 (仅当 doc_engine 为 elasticsearch 时有效) |
| `parser_config` | object | 否 | 新的解析器配置 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/datasets/kb_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "description": "Updated description"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "name": "My Knowledge Base",
    "avatar": "",
    "tenant_id": "user123456789",
    "language": "English",
    "description": "Updated description",
    "embedding_model": "BAAI/bge-large-zh-v1.5",
    "permission": "me",
    "created_by": "user123456789",
    "document_count": 5,
    "token_num": 12345,
    "chunk_count": 100,
    "similarity_threshold": 0.2,
    "vector_similarity_weight": 0.3,
    "chunk_method": "naive",
    "pipeline_id": null,
    "parser_config": {
      "pages": [[1, 1000000]],
      "table_context_size": 0,
      "image_context_size": 0,
      "llm_id": "deepseek-chat"
    },
    "pagerank": 0,
    "graphrag_task_id": null,
    "graphrag_task_finish_at": null,
    "raptor_task_id": null,
    "raptor_task_finish_at": null,
    "mindmap_task_id": null,
    "mindmap_task_finish_at": null,
    "status": "1",
    "create_time": 1700000000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1700001000000,
    "update_date": "2024-01-01 12:16:40"
  },
  "message": "success"
}
```

---

## 4. 获取数据集列表 (List Datasets)

获取当前用户或 Tenant 的数据集列表。

- **URL**: `/datasets`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 否 | 按数据集 ID 筛选 |
| `name` | string | 否 | 按数据集名称筛选 |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 30) |
| `orderby` | string | 否 | 排序字段 (默认 "create_time") |
| `desc` | boolean | 否 | 是否降序 (默认 true) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "a1b2c3d4e5f6789012345678",
      "name": "Dataset 1",
      "avatar": "",
      "tenant_id": "user123456789",
      "language": "English",
      "description": "My first dataset",
      "embedding_model": "BAAI/bge-large-zh-v1.5",
      "permission": "me",
      "created_by": "user123456789",
      "document_count": 10,
      "token_num": 50000,
      "chunk_count": 500,
      "similarity_threshold": 0.2,
      "vector_similarity_weight": 0.3,
      "chunk_method": "naive",
      "pipeline_id": null,
      "parser_config": {
        "pages": [[1, 1000000]],
        "table_context_size": 0,
        "image_context_size": 0,
        "llm_id": "deepseek-chat"
      },
      "pagerank": 0,
      "graphrag_task_id": null,
      "graphrag_task_finish_at": null,
      "raptor_task_id": null,
      "raptor_task_finish_at": null,
      "mindmap_task_id": null,
      "mindmap_task_finish_at": null,
      "status": "1",
      "create_time": 1700000000000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1700000000000,
      "update_date": "2024-01-01 12:00:00"
    }
  ],
  "total": 100,
  "message": "success"
}
```

---

## 5. 获取知识图谱 (Get Knowledge Graph)

获取数据集的知识图谱数据 (节点和边)。

- **URL**: `/datasets/<dataset_id>/knowledge_graph`
- **Method**: `GET`

### 请求参数

无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/kb_123/knowledge_graph" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "graph": {
      "nodes": [
        {
          "id": "node_1",
          "label": "Entity A",
          "pagerank": 0.85
        },
        {
          "id": "node_2",
          "label": "Entity B",
          "pagerank": 0.72
        }
      ],
      "edges": [
        {
          "source": "node_1",
          "target": "node_2",
          "weight": 0.9,
          "label": "related_to"
        }
      ]
    },
    "mind_map": {}
  },
  "message": "success"
}
```

---

## 6. 删除知识图谱 (Delete Knowledge Graph)

删除数据集的知识图谱数据。

- **URL**: `/datasets/<dataset_id>/knowledge_graph`
- **Method**: `DELETE`

### 请求参数

无

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/datasets/kb_123/knowledge_graph" \
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

## 7. 运行 GraphRAG (Run GraphRAG)

对数据集运行 GraphRAG 任务 (需确保文档已解析)。

- **URL**: `/datasets/<dataset_id>/run_graphrag`
- **Method**: `POST`

### 请求参数

无

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/kb_123/run_graphrag" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "graphrag_task_id": "a1b2c3d4e5f6789012345678"
  },
  "message": "success"
}
```

---

## 8. 追踪 GraphRAG 状态 (Trace GraphRAG)

获取 GraphRAG 任务的执行状态。

- **URL**: `/datasets/<dataset_id>/trace_graphrag`
- **Method**: `GET`

### 请求参数

无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/kb_123/trace_graphrag" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "doc_id": "graph_raptor_x",
    "from_page": 0,
    "to_page": 100000000,
    "task_type": "graphrag",
    "priority": 0,
    "begin_at": "2024-01-01 12:00:00",
    "process_duration": 120.5,
    "progress": 0.5,
    "progress_msg": "12:00:00 Task has been received.\n12:01:00 Processing entities...",
    "retry_count": 0,
    "digest": "",
    "chunk_ids": "",
    "create_time": 1700000000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1700000120000,
    "update_date": "2024-01-01 12:02:00"
  },
  "message": "success"
}
```

### 响应示例 (任务未找到)
```json
{
  "code": 0,
  "data": {},
  "message": "success"
}
```

---

## 9. 运行 RAPTOR (Run RAPTOR)

对数据集运行 RAPTOR 任务 (递归摘要)。

- **URL**: `/datasets/<dataset_id>/run_raptor`
- **Method**: `POST`

### 请求参数

无

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/kb_123/run_raptor" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "raptor_task_id": "a1b2c3d4e5f6789012345678"
  },
  "message": "success"
}
```

---

## 10. 追踪 RAPTOR 状态 (Trace RAPTOR)

获取 RAPTOR 任务的执行状态。

- **URL**: `/datasets/<dataset_id>/trace_raptor`
- **Method**: `GET`

### 请求参数

无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/kb_123/trace_raptor" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "doc_id": "graph_raptor_x",
    "from_page": 0,
    "to_page": 100000000,
    "task_type": "raptor",
    "priority": 0,
    "begin_at": "2024-01-01 12:00:00",
    "process_duration": 300.0,
    "progress": 1.0,
    "progress_msg": "12:00:00 Task has been received.\n12:05:00 RAPTOR completed successfully.",
    "retry_count": 0,
    "digest": "",
    "chunk_ids": "chunk_1 chunk_2 chunk_3",
    "create_time": 1700000000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1700000300000,
    "update_date": "2024-01-01 12:05:00"
  },
  "message": "success"
}
```

### 响应示例 (任务未找到)
```json
{
  "code": 0,
  "data": {},
  "message": "success"
}
```


---


# Dify Retrieval API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 检索 (Retrieval)

Dify 兼容的检索接口，支持从指定的知识库中检索相关内容。

- **URL**: `/dify/retrieval`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `knowledge_id` | string | 是 | Knowledge base ID (知识库 ID) |
| `query` | string | 是 | Query text (检索关键词) |
| `use_kg` | boolean | 否 | Whether to use knowledge graph (是否使用知识图谱，默认 false) |
| `retrieval_setting` | object | 否 | Retrieval configuration (检索设置) |
| `retrieval_setting.score_threshold` | number | 否 | Similarity threshold (相似度阈值，默认 0.0) |
| `retrieval_setting.top_k` | integer | 否 | Number of results to return (返回结果数量，默认 1024) |
| `metadata_condition` | object | 否 | Metadata filter condition (元数据过滤条件) |
| `metadata_condition.logic` | string | 否 | Logic connection (逻辑关系 'and' 或 'or') |
| `metadata_condition.conditions` | array | 否 | List of conditions (条件列表) |
| `metadata_condition.conditions[].name` | string | 否 | Field name (字段名) |
| `metadata_condition.conditions[].comparison_operator` | string | 否 | Operator (操作符，如 =, <, > 等) |
| `metadata_condition.conditions[].value` | string | 否 | Field value (字段值) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/dify/retrieval" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "knowledge_id": "kb_123456",
           "query": "什么是 RAGFlow？",
           "retrieval_setting": {
             "score_threshold": 0.5,
             "top_k": 5
           },
           "metadata_condition": {
             "logic": "and",
             "conditions": [
               {
                 "name": "author",
                 "comparison_operator": "=",
                 "value": "admin"
               }
             ]
           }
         }'
```

### 响应示例

**成功响应 (200)**
```json
{
  "records": [
    {
      "content": "RAGFlow is an open-source RAG engine based on deep document understanding...",
      "score": 0.89,
      "title": "RAGFlow_Introduction.pdf",
      "metadata": {
        "doc_id": "abc123def456",
        "author": "admin",
        "category": "技术文档"
      }
    },
    {
      "content": "RAGFlow 支持多种文档格式，包括 PDF、Word、Excel 等...",
      "score": 0.75,
      "title": "RAGFlow_用户手册.docx",
      "metadata": {
        "doc_id": "xyz789ghi012",
        "version": "1.0"
      }
    }
  ]
}
```

**知识库不存在 (404)**
```json
{
  "code": 102,
  "message": "Knowledgebase not found!"
}
```

**未找到相关 chunk (404)**
```json
{
  "code": 102,
  "message": "No chunk found! Check the chunk status please!"
}
```

**服务器错误 (500)**
```json
{
  "code": 100,
  "message": "Internal server error message"
}
```

### 响应字段说明

| 字段名 | 类型 | 描述 |
| :--- | :--- | :--- |
| `records` | array | 检索结果列表 |
| `records[].content` | string | Chunk 内容文本 |
| `records[].score` | number | 相似度分数 (0-1) |
| `records[].title` | string | 文档名称 |
| `records[].metadata` | object | 元数据信息 |
| `records[].metadata.doc_id` | string | 文档 ID |
| `records[].metadata.*` | any | 其他用户自定义的元数据字段 |


---


# Document Management API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 上传文件 (Upload Documents)

上传文档到指定数据集。

- **URL**: `/datasets/<dataset_id>/documents`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Body/Form)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file` | file | 是 | 要上传的文档文件 (支持多个文件) |
| `parent_path` | string | 否 | 父文件夹路径，使用 '/' 分隔 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/dataset_123/documents" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -F "file=@/path/to/document.pdf" \
     -F "parent_path=/"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "doc_1",
      "name": "document.pdf",
      "thumbnail": null,
      "dataset_id": "dataset_123",
      "chunk_method": "naive",
      "parser_config": {
        "pages": [[1, 1000000]],
        "table_context_size": 0,
        "image_context_size": 0
      },
      "source_type": "local",
      "type": "doc",
      "created_by": "user_123",
      "location": "dataset_123/doc_1",
      "size": 102400,
      "token_count": 0,
      "chunk_count": 0,
      "progress": 0.0,
      "progress_msg": "",
      "process_begin_at": null,
      "process_duration": 0.0,
      "meta_fields": {},
      "suffix": "pdf",
      "run": "UNSTART",
      "status": "1",
      "create_time": "2024-01-01 12:00:00",
      "create_date": "2024-01-01",
      "update_time": "2024-01-01 12:00:00",
      "update_date": "2024-01-01"
    }
  ],
  "message": "success"
}
```

---

## 2. 更新文档 (Update Document)

更新数据集中文档的元信息或配置。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 新的文档名称 (需包含扩展名) |
| `chunk_method` | string | 否 | 解析方法 (如: naive, manual, qa, table, etc.) |
| `parser_config` | object | 否 | 解析器配置 |
| `enabled` | boolean | 否 | 启用/禁用文档 |
| `meta_fields` | object | 否 | 元数据字段 (JSON Object) |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "new_name.pdf",
           "enabled": true
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "doc_1",
    "name": "new_name.pdf",
    "thumbnail": null,
    "dataset_id": "dataset_123",
    "chunk_method": "naive",
    "pipeline_id": null,
    "parser_config": {
      "pages": [[1, 1000000]],
      "table_context_size": 0,
      "image_context_size": 0
    },
    "source_type": "local",
    "type": "doc",
    "created_by": "user_123",
    "location": "dataset_123/doc_1",
    "size": 102400,
    "token_count": 5000,
    "chunk_count": 50,
    "progress": 1.0,
    "progress_msg": "Done",
    "process_begin_at": "2024-01-01 12:00:00",
    "process_duration": 10.5,
    "meta_fields": {},
    "suffix": "pdf",
    "run": "DONE",
    "status": "1",
    "create_time": "2024-01-01 12:00:00",
    "create_date": "2024-01-01",
    "update_time": "2024-01-01 12:05:00",
    "update_date": "2024-01-01"
  },
  "message": "success"
}
```

---

## 3. 下载文档 (Download Document)

下载数据集中的文档文件。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" --output document.pdf
```

### 响应示例
(文件流，Content-Type: application/octet-stream)

---

## 4. 获取文档列表 (List Documents)

列出数据集中的文档。

- **URL**: `/datasets/<dataset_id>/documents`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `id` | string | 否 | 按文档 ID 过滤 |
| `name` | string | 否 | 按文档名称过滤 |
| `keywords` | string | 否 | 搜索关键字 |
| `orderby` | string | 否 | 排序字段 (默认: create_time) |
| `desc` | boolean | 否 | 是否降序 (默认: true) |
| `suffix` | array[string] | 否 | 按文件后缀过滤 (e.g., pdf, docx) |
| `run` | array[string] | 否 | 按运行状态过滤 (UNSTART, RUNNING, CANCEL, DONE, FAIL) |
| `create_time_from` | integer | 否 | 创建时间起始 (Unix timestamp) |
| `create_time_to` | integer | 否 | 创建时间结束 (Unix timestamp) |
| `metadata_condition` | json string | 否 | 元数据过滤条件 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/dataset_123/documents?page=1&page_size=10&keywords=report" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 100,
    "docs": [
      {
        "id": "doc_1",
        "name": "report.pdf",
        "thumbnail": null,
        "dataset_id": "dataset_123",
        "chunk_method": "naive",
        "pipeline_id": null,
        "parser_config": {
          "pages": [[1, 1000000]],
          "table_context_size": 0,
          "image_context_size": 0
        },
        "source_type": "local",
        "type": "doc",
        "created_by": "user_123",
        "location": "dataset_123/doc_1",
        "size": 102400,
        "token_count": 5000,
        "chunk_count": 50,
        "progress": 1.0,
        "progress_msg": "Done",
        "process_begin_at": "2024-01-01 12:00:00",
        "process_duration": 10.5,
        "meta_fields": {
          "author": "Alice"
        },
        "suffix": "pdf",
        "run": "DONE",
        "status": "1",
        "create_time": "2024-01-01 12:00:00",
        "create_date": "2024-01-01",
        "update_time": "2024-01-01 12:05:00",
        "update_date": "2024-01-01",
        "title": null
      }
    ]
  },
  "message": "success"
}
```

---

## 5. 元数据摘要 (Metadata Summary)

获取数据集的元数据摘要信息。

- **URL**: `/datasets/<dataset_id>/metadata/summary`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/dataset_123/metadata/summary" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "summary": {
      "author": ["Alice", "Bob"],
      "department": ["Engineering", "Sales"],
      "year": ["2023", "2024"]
    }
  },
  "message": "success"
}
```

---

## 6. 元数据批量更新 (Metadata Batch Update)

批量更新或删除文档的元数据。

- **URL**: `/datasets/<dataset_id>/metadata/update`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `selector` | object | 否 | 选择器，包含 `metadata_condition` (filter) 或 `document_ids` |
| `updates` | list[object] | 否 | 更新操作列表，每项含 `key`, `value` |
| `deletes` | list[object] | 否 | 删除操作列表，每项含 `key` |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/dataset_123/metadata/update" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "selector": {
             "document_ids": ["doc_1", "doc_2"]
           },
           "updates": [
             {"key": "author", "value": "Alice"}
           ]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "updated": 2,
    "matched_docs": 2
  },
  "message": "success"
}
```

---

## 7. 删除文档 (Delete Documents)

删除数据集中的一个或多个文档。

- **URL**: `/datasets/<dataset_id>/documents`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ids` | list[string] | 否 | 要删除的文档 ID 列表 (若为空则删除该知识库下所有文档) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/datasets/dataset_123/documents" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["doc_1", "doc_2"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "success"
}
```

---

## 8. 解析文档 (Parse Documents)

开始解析文档（生成 Chunk）。

- **URL**: `/datasets/<dataset_id>/chunks`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `document_ids` | list[string] | 是 | 要解析的文档 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/dataset_123/chunks" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "document_ids": ["doc_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "success"
}
```

---

## 9. 停止解析 (Stop Parsing)

停止文档的解析任务。

- **URL**: `/datasets/<dataset_id>/chunks`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `document_ids` | list[string] | 是 | 要停止解析的文档 ID 列表 |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/datasets/dataset_123/chunks" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "document_ids": ["doc_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "success"
}
```

---

## 10. 获取 Chunk 列表 (List Chunks)

获取文档的 Chunk 列表或搜索 Chunk。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>/chunks`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `id` | string | 否 | 按 Chunk ID 精确查找 |
| `keywords` | string | 否 | 搜索关键字 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1/chunks?page=1&keywords=test" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 10,
    "chunks": [
      {
        "id": "chunk_1",
        "content": "This is a chunk content.",
        "document_id": "doc_1",
        "docnm_kwd": "report.pdf",
        "important_keywords": ["keyword1", "keyword2"],
        "questions": ["What is this?"],
        "dataset_id": "dataset_123",
        "image_id": "",
        "available": true,
        "positions": [[1, 100, 200, 300, 400]]
      }
    ],
    "doc": {
      "id": "doc_1",
      "name": "report.pdf",
      "thumbnail": null,
      "dataset_id": "dataset_123",
      "chunk_method": "naive",
      "pipeline_id": null,
      "parser_config": {
        "pages": [[1, 1000000]],
        "table_context_size": 0,
        "image_context_size": 0
      },
      "source_type": "local",
      "type": "doc",
      "created_by": "user_123",
      "location": "dataset_123/doc_1",
      "size": 102400,
      "token_count": 5000,
      "chunk_count": 50,
      "progress": 1.0,
      "progress_msg": "Done",
      "process_begin_at": "2024-01-01 12:00:00",
      "process_duration": 10.5,
      "meta_fields": {},
      "suffix": "pdf",
      "run": "DONE",
      "status": "1",
      "create_time": "2024-01-01 12:00:00",
      "create_date": "2024-01-01",
      "update_time": "2024-01-01 12:05:00",
      "update_date": "2024-01-01"
    }
  },
  "message": "success"
}
```

---

## 11. 添加 Chunk (Add Chunk)

手动添加 Chunk 到文档。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>/chunks`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `content` | string | 是 | Chunk 内容 |
| `important_keywords` | list[string] | 否 | 关键词列表 |
| `questions` | list[string] | 否 | 相关问题列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1/chunks" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "content": "New chunk content",
           "important_keywords": ["new", "chunk"],
           "questions": ["What is new?"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "chunk": {
      "id": "a1b2c3d4e5f6g7h8",
      "content": "New chunk content",
      "document_id": "doc_1",
      "important_keywords": ["new", "chunk"],
      "questions": ["What is new?"],
      "dataset_id": "dataset_123",
      "create_timestamp": 1704110400.0,
      "create_time": "2024-01-01 12:00:00"
    }
  },
  "message": "success"
}
```

---

## 12. 删除 Chunk (Remove Chunks)

删除文档中的一个或多个 Chunk。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>/chunks`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chunk_ids` | list[string] | 否 | 要删除的 Chunk ID 列表 (若空则删除文档下所有 Chunk) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1/chunks" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "chunk_ids": ["chunk_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "deleted 1 chunks"
}
```

---

## 13. 更新 Chunk (Update Chunk)

更新 Chunk 的内容或属性。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>/chunks/<chunk_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |
| `chunk_id` | string | 是 | Chunk ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `content` | string | 否 | 新的 Chunk 内容 |
| `important_keywords` | list[string] | 否 | 关键词列表 |
| `questions` | list[string] | 否 | 相关问题列表 |
| `available` | boolean | 否 | 是否启用 |
| `positions` | list[list[int]] | 否 | 位置信息，每个元素为长度为 5 的整数数组 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1/chunks/chunk_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "content": "Updated content",
           "important_keywords": ["updated"],
           "available": true
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "success"
}
```

---

## 14. 检索测试 (Retrieval Test)

执行检索测试。

- **URL**: `/retrieval`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_ids` | list[string] | 是 | 搜索的数据集 ID 列表 |
| `question` | string | 是 | 查询问题 |
| `document_ids` | list[string] | 否 | 限定文档 ID 列表 |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `similarity_threshold` | number | 否 | 相似度阈值 (默认: 0.2) |
| `vector_similarity_weight` | number | 否 | 向量相似度权重 (默认: 0.3) |
| `top_k` | integer | 否 | 返回数量 (默认: 1024) |
| `highlight` | boolean | 否 | 是否高亮匹配内容 (默认: true) |
| `rerank_id` | string | 否 | 重排模型 ID |
| `keyword` | boolean | 否 | 是否进行关键词增强 |
| `cross_languages` | list[string] | 否 | 跨语言搜索配置 |
| `use_kg` | boolean | 否 | 是否使用知识图谱 |
| `toc_enhance` | boolean | 否 | 是否启用目录增强 |
| `metadata_condition` | object | 否 | 元数据过滤条件 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/retrieval" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "dataset_ids": ["dataset_123"],
           "question": "what is ragflow?",
           "top_k": 5,
           "similarity_threshold": 0.2,
           "vector_similarity_weight": 0.3,
           "highlight": true
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 5,
    "chunks": [
      {
        "id": "chunk_1",
        "content": "RAGFlow is an open-source RAG engine based on deep document understanding.",
        "document_id": "doc_1",
        "document_keyword": "ragflow_intro.pdf",
        "dataset_id": "dataset_123",
        "important_keywords": ["RAGFlow", "RAG", "document understanding"],
        "questions": [],
        "similarity": 0.95,
        "vector_similarity": 0.92,
        "term_similarity": 0.98,
        "positions": [[1, 100, 200, 300, 400]]
      },
      {
        "id": "chunk_2",
        "content": "RAGFlow provides deep document parsing capabilities.",
        "document_id": "doc_1",
        "document_keyword": "ragflow_intro.pdf",
        "dataset_id": "dataset_123",
        "important_keywords": ["document parsing"],
        "questions": [],
        "similarity": 0.88,
        "vector_similarity": 0.85,
        "term_similarity": 0.91,
        "positions": [[2, 50, 100, 150, 200]]
      }
    ],
    "doc_aggs": {
      "doc_1": 2
    }
  },
  "message": "success"
}
```


---


# File Management API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 上传文件 (Upload File)

上传文件到系统。

- **URL**: `/file/upload`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (FormData)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file` | file | 是 | 要上传的文件 |
| `parent_id` | string | 否 | 父文件夹 ID (若不传则上传到根目录) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/upload" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -F "file=@/path/to/document.pdf" \
     -F "parent_id=folder_123"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "file_uuid",
      "parent_id": "folder_123",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "document.pdf",
      "location": "document.pdf",
      "size": 1024,
      "type": "pdf",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 12:00:00"
    }
  ],
  "message": "success"
}
```

---

## 2. 创建文件/文件夹 (Create File/Folder)

创建一个新的文件夹或虚拟文件。

- **URL**: `/file/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 文件/文件夹名称 |
| `type` | string | 否 | 类型: `FOLDER` 或 `VIRTUAL` (默认 `VIRTUAL`) |
| `parent_id` | string | 否 | 父文件夹 ID (默认根目录) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "New Folder",
           "type": "FOLDER",
           "parent_id": "root_id"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "folder_uuid",
    "parent_id": "root_id",
    "tenant_id": "tenant_id",
    "created_by": "tenant_id",
    "name": "New Folder",
    "location": "",
    "size": 0,
    "type": "folder",
    "source_type": "",
    "create_time": 1704067200000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1704067200000,
    "update_date": "2024-01-01 12:00:00"
  },
  "message": "success"
}
```

---

## 3. 获取文件列表 (List Files)

列出指定文件夹下的文件。

- **URL**: `/file/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `parent_id` | string | 否 | 文件夹 ID (默认根目录) |
| `keywords` | string | 否 | 搜索关键字 |
| `page` | integer | 否 | 页码 (默认 1) |
| `page_size` | integer | 否 | 每页数量 (默认 15) |
| `orderby` | string | 否 | 排序字段 (默认 `create_time`) |
| `desc` | boolean | 否 | 是否降序 (默认 `true`) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/list?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 50,
    "files": [
      {
        "id": "file_1",
        "parent_id": "folder_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "doc.pdf",
        "location": "doc.pdf",
        "size": 2048,
        "type": "pdf",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 10:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 10:00:00",
        "kbs_info": [
          {
            "kb_id": "kb_id_1",
            "kb_name": "My Dataset",
            "document_id": "doc_id_1"
          }
        ]
      },
      {
        "id": "folder_2",
        "parent_id": "folder_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "subfolder",
        "location": "",
        "size": 4096,
        "type": "folder",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 09:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 09:00:00",
        "kbs_info": [],
        "has_child_folder": true
      }
    ],
    "parent_folder": {
      "id": "folder_id",
      "parent_id": "root_id",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "root",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 08:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 08:00:00"
    }
  },
  "message": "success"
}
```

---

## 4. 获取根目录 (Get Root Folder)

获取用户的根文件夹信息。

- **URL**: `/file/root_folder`
- **Method**: `GET`

### 请求参数
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/root_folder" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "root_folder": {
      "id": "root_id",
      "parent_id": "root_id",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "/",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 00:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 00:00:00"
    }
  },
  "message": "success"
}
```

---

## 5. 获取父文件夹 (Get Parent Folder)

获取指定文件的父文件夹信息。

- **URL**: `/file/parent_folder`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 目标文件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/parent_folder?file_id=file_xxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "parent_folder": {
      "id": "parent_id",
      "parent_id": "root_id",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "Parent Folder",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 00:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 00:00:00"
    }
  },
  "message": "success"
}
```

---

## 6. 获取所有父文件夹 (Get All Parent Folders)

获取文件的所有上级目录（路径）。

- **URL**: `/file/all_parent_folder`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 目标文件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/all_parent_folder?file_id=file_xxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "parent_folders": [
      {
        "id": "file_xxx",
        "parent_id": "folder_level_1",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "current_file.pdf",
        "location": "current_file.pdf",
        "size": 1024,
        "type": "pdf",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 12:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 12:00:00"
      },
      {
        "id": "folder_level_1",
        "parent_id": "root_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "Project A",
        "location": "",
        "size": 0,
        "type": "folder",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 10:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 10:00:00"
      },
      {
        "id": "root_id",
        "parent_id": "root_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "/",
        "location": "",
        "size": 0,
        "type": "folder",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 00:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 00:00:00"
      }
    ]
  },
  "message": "success"
}
```

---

## 7. 删除文件 (Remove Files)

删除一个或多个文件/文件夹。如果删除文件夹，其中的文件也会被删除。

- **URL**: `/file/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_ids` | list[string] | 是 | 要删除的文件 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_ids": ["file_1", "file_2"]
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

## 8. 重命名文件 (Rename File)

重命名文件。

- **URL**: `/file/rename`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 目标文件 ID |
| `name` | string | 是 | 新名称 (扩展名需保持一致) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/rename" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_id": "file_xxx",
           "name": "new_name.pdf"
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

## 9. 下载文件 (Download File)

下载文件内容。

- **URL**: `/file/get/<file_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 文件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/get/file_uuid_xxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     --output my_file.pdf
```

### 响应示例
(返回二进制文件流，响应头包含 `Content-Type` 字段，如 `application/pdf` 或 `image/png`)

---

## 10. 下载附件 (Download Attachment)

下载系统生成的附件。

- **URL**: `/file/download/<attachment_id>`
- **Method**: `GET`

### 请求参数

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `attachment_id` | string | 是 | 附件 ID (URL Path) |
| `ext` | string | 否 | 扩展名/格式 (Query, 默认 `markdown`) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/download/att_uuid?ext=pdf" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
(返回二进制文件流，响应头包含 `Content-Type` 字段，如 `application/pdf` 或 `text/markdown`)

---

## 11. 移动文件 (Move Files)

移动一个或多个文件到另一个文件夹。

- **URL**: `/file/mv`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `src_file_ids` | list[string] | 是 | 源文件 ID 列表 |
| `dest_file_id` | string | 是 | 目标文件夹 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/mv" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "src_file_ids": ["file_1", "file_2"],
           "dest_file_id": "folder_target"
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

## 12. 转换文件 (Convert File)

将文件解析并添加到知识库。

- **URL**: `/file/convert`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_ids` | list[string] | 是 | 目标知识库 ID 列表 |
| `file_ids` | list[string] | 是 | 要转换的文件 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/convert" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_ids": ["kb_1"],
           "file_ids": ["file_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "file2doc_id",
      "file_id": "file_1",
      "document_id": "doc_1",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 12:00:00"
    }
  ],
  "message": "success"
}
```


---


# Session & Chat API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建会话 (Create Session)

为指定的助手 (Assistant/Chat) 创建一个新的会话。

- **URL**: `/chats/<chat_id>/sessions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID (Dialog ID) |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 会话名称 (默认: "New session") |
| `user_id` | string | 否 | 用户标识 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chats/chat_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Chat Session",
           "user_id": "user_abc"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "550e8400e29b41d4a716446655440000",
    "chat_id": "chat_123",
    "name": "My Chat Session",
    "user_id": "user_abc",
    "create_time": 1704067200000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1704067200000,
    "update_date": "2024-01-01 12:00:00",
    "messages": [
      {
        "role": "assistant",
        "content": "Hi! I'm your assistant. What can I do for you?"
      }
    ]
  }
}
```

---

## 2. 创建 Agent 会话 (Create Agent Session)

为指定的 Agent 创建一个新的会话。

- **URL**: `/agents/<agent_id>/sessions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `user_id` | string | 否 | 用户标识 (默认为 tenant_id) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agents/agent_123/sessions?user_id=user_abc" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "550e8400e29b41d4a716446655440001",
    "agent_id": "agent_123",
    "user_id": "user_abc",
    "message": [
      {
        "role": "assistant",
        "content": "Hello! How can I assist you today?"
      }
    ],
    "source": "agent",
    "dsl": {
      "components": {},
      "history": [],
      "path": [],
      "answer": []
    }
  }
}
```

---

## 3. 更新会话 (Update Session)

更新会话信息（如重命名）。

- **URL**: `/chats/<chat_id>/sessions/<session_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |
| `session_id` | string | 是 | 会话 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 新的会话名称 (不能为空) |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/chats/chat_123/sessions/session_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "Renamed Session"
         }'
```

### 响应示例
```json
{
  "code": 0
}
```

---

## 4. 对话补全 (Chat Completion)

与助手进行对话。

- **URL**: `/chats/<chat_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 否 | 用户提问内容 (若 session_id 未提供则为空字符串) |
| `session_id` | string | 否 | 会话 ID (若提供则基于历史上下文) |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `metadata_condition` | object | 否 | 元数据过滤条件 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chats/chat_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAG?",
           "session_id": "session_1",
           "stream": true
         }'
```

### 响应示例 (Stream)
```text
data:{"code": 0, "data": {"answer": "RAG stands for Retrieval-Augmented Generation...", "reference": {"total": 3, "chunks": [{"id": "chunk_1", "content": "...", "document_id": "doc_1", "document_name": "example.pdf", "dataset_id": "kb_1", "image_id": "", "positions": [[1, 100, 200, 300, 400]]}], "doc_aggs": [{"doc_id": "doc_1", "doc_name": "example.pdf", "count": 1}]}, "audio_binary": null, "id": "msg_123", "session_id": "session_1"}}

data:{"code": 0, "data": true}
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "answer": "RAG stands for Retrieval-Augmented Generation...",
    "reference": {
      "total": 3,
      "chunks": [
        {
          "id": "chunk_1",
          "content": "RAG is a technique that combines retrieval and generation...",
          "document_id": "doc_1",
          "document_name": "example.pdf",
          "dataset_id": "kb_1",
          "image_id": "",
          "positions": [[1, 100, 200, 300, 400]]
        }
      ],
      "doc_aggs": [
        {
          "doc_id": "doc_1",
          "doc_name": "example.pdf",
          "count": 1
        }
      ]
    },
    "audio_binary": null,
    "id": "msg_123",
    "session_id": "session_1",
    "prompt": "...",
    "created_at": 1704067200.123
  }
}
```

---

## 5. OpenAI 兼容对话 (Chat Completion OpenAI Compatible)

OpenAI 兼容的对话接口。

- **URL**: `/chats_openai/<chat_id>/chat/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `messages` | list[object] | 是 | 消息列表 (包含 role 和 content) |
| `model` | string | 是 | 模型名称 (占位符，实际由后端配置决定) |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `extra_body` | object | 否 | 额外参数 (如 `reference`: boolean, `metadata_condition`: object) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chats_openai/chat_123/chat/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "model": "gpt-3.5-turbo",
           "messages": [
             {"role": "user", "content": "Hello"}
           ],
           "stream": true
         }'
```

### 响应示例 (Stream)
```text
data:{"id": "chatcmpl-chat_123", "choices": [{"delta": {"content": "Hello", "role": "assistant", "function_call": null, "tool_calls": null, "reasoning_content": null}, "finish_reason": null, "index": 0, "logprobs": null}], "created": 1704067200, "model": "model", "object": "chat.completion.chunk", "system_fingerprint": "", "usage": null}

data:{"id": "chatcmpl-chat_123", "choices": [{"delta": {"content": null, "reasoning_content": null}, "finish_reason": "stop", "index": 0, "logprobs": null}], "created": 1704067200, "model": "model", "object": "chat.completion.chunk", "system_fingerprint": "", "usage": {"prompt_tokens": 5, "completion_tokens": 50, "total_tokens": 55}}

data:[DONE]
```

### 响应示例 (Non-Stream)
```json
{
  "id": "chatcmpl-chat_123",
  "object": "chat.completion",
  "created": 1704067200,
  "model": "gpt-3.5-turbo",
  "usage": {
    "prompt_tokens": 5,
    "completion_tokens": 50,
    "total_tokens": 55,
    "completion_tokens_details": {
      "reasoning_tokens": 100,
      "accepted_prediction_tokens": 50,
      "rejected_prediction_tokens": 0
    }
  },
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "Hello! How can I help you today?"
      },
      "logprobs": null,
      "finish_reason": "stop",
      "index": 0
    }
  ]
}
```

### 响应示例 (Non-Stream with Reference)
```json
{
  "id": "chatcmpl-chat_123",
  "object": "chat.completion",
  "created": 1704067200,
  "model": "gpt-3.5-turbo",
  "usage": {
    "prompt_tokens": 5,
    "completion_tokens": 50,
    "total_tokens": 55,
    "completion_tokens_details": {
      "reasoning_tokens": 100,
      "accepted_prediction_tokens": 50,
      "rejected_prediction_tokens": 0
    }
  },
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "Based on the documents...",
        "reference": [
          {
            "id": "chunk_1",
            "content": "...",
            "document_id": "doc_1",
            "document_name": "example.pdf",
            "dataset_id": "kb_1"
          }
        ]
      },
      "logprobs": null,
      "finish_reason": "stop",
      "index": 0
    }
  ]
}
```

---

## 6. OpenAI 兼容 Agent 对话 (Agent Completion OpenAI Compatible)

OpenAI 兼容的 Agent 对话接口。

- **URL**: `/agents_openai/<agent_id>/chat/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `messages` | list[object] | 是 | 消息列表 |
| `model` | string | 是 | 模型名称 |
| `stream` | boolean | 否 | 是否流式返回 (默认: false, 注意此接口默认值与其他不同) |
| `session_id` | string | 否 | 会话 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agents_openai/agent_123/chat/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "messages": [{"role": "user", "content": "Run analysis"}],
           "model": "agent-model"
         }'
```

### 响应示例 (Non-Stream)
```json
{
  "id": "agent_123",
  "object": "chat.completion",
  "created": 1704067200,
  "model": "agent-model",
  "usage": {
    "prompt_tokens": 10,
    "completion_tokens": 100,
    "total_tokens": 110
  },
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "The analysis results show..."
      },
      "logprobs": null,
      "finish_reason": "stop",
      "index": 0
    }
  ]
}
```

---

## 7. Agent 补全 (Agent Completion)

执行 Agent 对话/任务。

- **URL**: `/agents/<agent_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `return_trace` | boolean | 否 | 是否返回执行轨迹 (默认: false) |
| `...` | any | 否 | 其他传递给 Agent 的参数 (如 inputs, question 等) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agents/agent_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Analyze this data",
           "stream": true,
           "return_trace": true
         }'
```

### 响应示例 (Stream)
```text
data:{"event": "message", "data": {"content": "Analyzing...", "session_id": "session_1"}}

data:{"event": "node_finished", "data": {"component_id": "begin_0", "trace": [{"component_id": "begin_0", "...": "..."}]}}

data:{"event": "message_end", "data": {"content": "Analysis complete.", "reference": {}, "session_id": "session_1"}}

data:[DONE]
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "event": "message_end",
    "data": {
      "content": "The analysis shows that...",
      "reference": {
        "chunks": [
          {
            "id": "chunk_1",
            "content": "...",
            "document_id": "doc_1",
            "document_name": "data.csv",
            "dataset_id": "kb_1"
          }
        ],
        "doc_aggs": [
          {
            "doc_id": "doc_1",
            "doc_name": "data.csv",
            "count": 1
          }
        ]
      },
      "trace": [
        {
          "component_id": "begin_0",
          "trace": [{"component_id": "begin_0"}]
        },
        {
          "component_id": "generate_1",
          "trace": [{"component_id": "generate_1"}]
        }
      ]
    }
  }
}
```

---

## 8. 获取会话列表 (List Sessions)

获取助手的会话列表。

- **URL**: `/chats/<chat_id>/sessions`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `orderby` | string | 否 | 排序字段 (默认: create_time) |
| `desc` | boolean | 否 | 是否降序 (默认: true) |
| `id` | string | 否 | 按会话 ID 过滤 |
| `name` | string | 否 | 按会话名称过滤 |
| `user_id` | string | 否 | 按用户 ID 过滤 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/chats/chat_123/sessions?page=1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "session_1",
      "chat_id": "chat_123",
      "name": "New session",
      "user_id": "user_abc",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 12:00:00",
      "messages": [
        {
          "role": "assistant",
          "content": "Hi! How can I help you?",
          "created_at": 1704067200.0
        },
        {
          "role": "user",
          "content": "What is RAG?",
          "id": "msg_user_1"
        },
        {
          "role": "assistant",
          "content": "RAG stands for...",
          "id": "msg_assistant_1",
          "created_at": 1704067210.0,
          "reference": [
            {
              "id": "chunk_1",
              "content": "...",
              "document_id": "doc_1",
              "document_name": "example.pdf",
              "dataset_id": "kb_1",
              "image_id": "",
              "positions": [[1, 100, 200, 300, 400]]
            }
          ]
        }
      ]
    }
  ]
}
```

---

## 9. 获取 Agent 会话列表 (List Agent Sessions)

获取 Agent 的会话列表。

- **URL**: `/agents/<agent_id>/sessions`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `orderby` | string | 否 | 排序字段 (默认: update_time) |
| `desc` | boolean | 否 | 是否降序 (默认: true) |
| `dsl` | boolean | 否 | 是否包含 DSL (默认: true) |
| `id` | string | 否 | 按 ID 过滤 |
| `user_id` | string | 否 | 按用户 ID 过滤 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/agents/agent_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "session_agent_1",
      "agent_id": "agent_123",
      "user_id": "user_abc",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704153600000,
      "update_date": "2024-01-02 12:00:00",
      "tokens": 1500,
      "source": "agent",
      "duration": 2.5,
      "round": 3,
      "thumb_up": 1,
      "messages": [
        {
          "role": "assistant",
          "content": "Hello! How can I assist you?",
          "created_at": 1704067200.0
        },
        {
          "role": "user",
          "content": "Analyze this data",
          "id": "msg_user_1"
        },
        {
          "role": "assistant",
          "content": "The analysis shows...",
          "id": "msg_assistant_1",
          "created_at": 1704067210.0,
          "reference": [
            {
              "id": "chunk_1",
              "content": "...",
              "document_id": "doc_1",
              "document_name": "data.csv",
              "dataset_id": "kb_1",
              "image_id": "",
              "positions": []
            }
          ]
        }
      ],
      "dsl": {
        "components": {},
        "history": [],
        "path": [],
        "answer": []
      }
    }
  ]
}
```

---

## 10. 删除会话 (Delete Sessions)

删除一个或多个会话。

- **URL**: `/chats/<chat_id>/sessions`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ids` | list[string] | 否 | 要删除的会话 ID 列表 (若为空则删除该 chat 下的全部会话) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/chats/chat_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["session_1"]
         }'
```

### 响应示例 (全部成功)
```json
{
  "code": 0
}
```

### 响应示例 (部分成功)
```json
{
  "code": 0,
  "message": "Partially deleted 2 sessions with 1 errors",
  "data": {
    "success_count": 2,
    "errors": [
      "The chat doesn't own the session session_not_exist"
    ]
  }
}
```

---

## 11. 删除 Agent 会话 (Delete Agent Sessions)

删除一个或多个 Agent 会话。

- **URL**: `/agents/<agent_id>/sessions`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ids` | list[string] | 否 | 要删除的会话 ID 列表 (若为空则删除该 agent 下的全部会话) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/agents/agent_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["session_agent_1"]
         }'
```

### 响应示例 (全部成功)
```json
{
  "code": 0
}
```

### 响应示例 (部分成功)
```json
{
  "code": 0,
  "message": "Partially deleted 2 sessions with 1 errors",
  "data": {
    "success_count": 2,
    "errors": [
      "The agent doesn't own the session session_not_exist"
    ]
  }
}
```

---

## 12. 知识库问答 (Ask KB)

直接针对知识库提问。

- **URL**: `/sessions/ask`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题内容 |
| `dataset_ids` | list[string] | 是 | 知识库 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/sessions/ask" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is in the doc?",
           "dataset_ids": ["kb_1"]
         }'
```

### 响应示例 (Stream)
```text
data:{"code": 0, "message": "", "data": {"answer": "Based on the documents...", "reference": {}}}

data:{"code": 0, "message": "", "data": {"answer": "Based on the documents, the content includes...", "reference": {"total": 2, "chunks": [{"id": "chunk_1", "content": "...", "document_id": "doc_1", "document_name": "example.pdf", "dataset_id": "kb_1"}], "doc_aggs": [{"doc_id": "doc_1", "doc_name": "example.pdf", "count": 1}]}}}

data:{"code": 0, "message": "", "data": true}
```

---

## 13. 相关问题生成 (Related Questions)

根据问题生成相关搜索建议。

- **URL**: `/sessions/related_questions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 原始问题 |
| `industry` | string | 否 | 行业背景 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/sessions/related_questions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Deep learning"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    "What is deep learning?",
    "Deep learning vs machine learning",
    "Deep learning applications",
    "Neural network architectures",
    "How to get started with deep learning"
  ]
}
```

---

## 14. 聊天机器人补全 (Chatbot Completion)

用于嵌入式聊天机器人 (Iframe/External) 的对话接口。

- **URL**: `/chatbots/<dialog_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | 对话 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 用户提问 |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `session_id` | string | 否 | 会话 ID |
| `quote` | boolean | 否 | 是否返回引用 (默认: false) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chatbots/dialog_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Hello"
         }'
```

### 响应示例 (Stream - 新会话)
```text
data:{"code": 0, "message": "", "data": {"answer": "Hi! I'm your assistant. What can I do for you?", "reference": {}, "audio_binary": null, "id": null, "session_id": "550e8400e29b41d4a716446655440000"}}

data:{"code": 0, "message": "", "data": true}
```

### 响应示例 (Stream - 已有会话)
```text
data:{"code": 0, "message": "", "data": {"answer": "Hello! How can I help you today?", "reference": {"chunks": [...], "doc_aggs": [...]}, "audio_binary": null, "id": "msg_123", "session_id": "session_1"}}

data:{"code": 0, "message": "", "data": true}
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "answer": "Hello! How can I help you today?",
    "reference": {
      "chunks": [],
      "doc_aggs": []
    },
    "audio_binary": null,
    "id": "msg_123",
    "session_id": "session_1",
    "prompt": "...",
    "created_at": 1704067200.123
  }
}
```

---

## 15. 获取聊天机器人信息 (Chatbot Info)

获取嵌入式聊天机器人的基本信息。

- **URL**: `/chatbots/<dialog_id>/info`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | 对话 ID |

### 响应示例
```json
{
  "code": 0,
  "data": {
    "title": "Customer Service Bot",
    "avatar": "data:image/png;base64,iVBORw0KGgo...",
    "prologue": "Hi! I'm your assistant. What can I do for you?"
  }
}
```

---

## 16. Agent 机器人补全 (Agentbot Completion)

用于嵌入式 Agent 机器人 (Iframe/External) 的执行接口。

- **URL**: `/agentbots/<agent_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `question` | string | 否 | 用户问题 |
| `session_id` | string | 否 | 会话 ID |
| `...` | any | 否 | Agent 输入参数 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agentbots/agent_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Process this request",
           "stream": true
         }'
```

### 响应示例 (Stream)
```text
data:{"event": "message", "data": {"content": "Processing your request...", "session_id": "session_1"}}

data:{"event": "message", "data": {"content": "Processing your request... Done!", "session_id": "session_1"}}

data:{"event": "message_end", "data": {"content": "Processing your request... Done!", "reference": {}, "session_id": "session_1"}}

data:[DONE]
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "event": "message_end",
    "data": {
      "content": "Request processed successfully.",
      "reference": {},
      "session_id": "session_1"
    }
  }
}
```

---

## 17. 获取 Agent 机器人输入项 (Agentbot Inputs)

获取 Agent 机器人的初始输入表单配置。

- **URL**: `/agentbots/<agent_id>/inputs`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 响应示例
```json
{
  "code": 0,
  "data": {
    "title": "Data Analysis Agent",
    "avatar": "data:image/png;base64,iVBORw0KGgo...",
    "inputs": [
      {
        "key": "file",
        "type": "file",
        "name": "Upload File",
        "required": true
      },
      {
        "key": "query",
        "type": "text",
        "name": "Analysis Query",
        "required": false
      }
    ],
    "prologue": "Welcome! Please upload your data file to begin analysis.",
    "mode": "chat"
  }
}
```

---

## 18. 搜索机器人问答 (Searchbot Ask)

用于搜索机器人 (Searchbot) 的问答接口。

- **URL**: `/searchbots/ask`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `kb_ids` | list[string] | 是 | 知识库 ID 列表 |
| `search_id` | string | 否 | 搜索应用 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/ask" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is machine learning?",
           "kb_ids": ["kb_1", "kb_2"]
         }'
```

### 响应示例 (Stream)
```text
data:{"code": 0, "message": "", "data": {"answer": "Machine learning is...", "reference": {}}}

data:{"code": 0, "message": "", "data": {"answer": "Machine learning is a subset of artificial intelligence...", "reference": {"total": 5, "chunks": [{"id": "chunk_1", "content": "...", "document_id": "doc_1", "document_name": "ml_guide.pdf", "dataset_id": "kb_1"}], "doc_aggs": [{"doc_id": "doc_1", "doc_name": "ml_guide.pdf", "count": 2}]}}}

data:{"code": 0, "message": "", "data": true}
```

---

## 19. 搜索机器人检索测试 (Searchbot Retrieval Test)

搜索机器人的检索测试接口。

- **URL**: `/searchbots/retrieval_test`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `kb_id` | string 或 list[string] | 是 | 知识库 ID (列表) |
| `top_k` | integer | 否 | 返回数量 (默认: 1024) |
| `similarity_threshold` | number | 否 | 相似度阈值 (默认: 0.0) |
| `vector_similarity_weight` | number | 否 | 向量相似度权重 (默认: 0.3) |
| `doc_ids` | list[string] | 否 | 文档 ID 过滤列表 |
| `page` | integer | 否 | 页码 (默认: 1) |
| `size` | integer | 否 | 每页数量 (默认: 30) |
| `rerank_id` | string | 否 | Rerank 模型 ID |
| `use_kg` | boolean | 否 | 是否使用知识图谱 (默认: false) |
| `highlight` | boolean | 否 | 是否高亮显示 |
| `keyword` | boolean | 否 | 是否启用关键词提取 (默认: false) |
| `cross_languages` | list[string] | 否 | 跨语言搜索列表 |
| `search_id` | string | 否 | 搜索应用 ID |
| `meta_data_filter` | object | 否 | 元数据过滤配置 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/retrieval_test" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAG?",
           "kb_id": ["kb_1"],
           "top_k": 10,
           "similarity_threshold": 0.2
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 25,
    "chunks": [
      {
        "chunk_id": "chunk_001",
        "content_with_weight": "RAG (Retrieval-Augmented Generation) is a technique...",
        "content_ltks": "rag retrieval augmented generation technique",
        "doc_id": "doc_1",
        "docnm_kwd": "rag_guide.pdf",
        "kb_id": "kb_1",
        "similarity": 0.89,
        "vector_similarity": 0.85,
        "term_similarity": 0.92,
        "positions": [[1, 50, 100, 200, 150]],
        "image_id": ""
      },
      {
        "chunk_id": "chunk_002",
        "content_with_weight": "RAG combines the power of retrieval...",
        "content_ltks": "rag combines power retrieval",
        "doc_id": "doc_1",
        "docnm_kwd": "rag_guide.pdf",
        "kb_id": "kb_1",
        "similarity": 0.82,
        "vector_similarity": 0.80,
        "term_similarity": 0.84,
        "positions": [[2, 60, 110, 210, 160]],
        "image_id": ""
      }
    ],
    "doc_aggs": [
      {
        "doc_id": "doc_1",
        "doc_name": "rag_guide.pdf",
        "count": 5
      }
    ],
    "labels": ["technology", "ai"]
  }
}
```

---

## 20. 搜索机器人相关问题 (Searchbot Related Questions)

生成搜索机器人的相关推荐问题。

- **URL**: `/searchbots/related_questions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `search_id` | string | 否 | 搜索应用 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/related_questions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAG?"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    "How does RAG work?",
    "RAG vs fine-tuning comparison",
    "Best practices for RAG implementation",
    "RAG architecture overview",
    "Common RAG use cases"
  ]
}
```

---

## 21. 获取搜索机器人详情 (Searchbot Detail)

获取搜索机器人的详细配置。

- **URL**: `/searchbots/detail`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `search_id` | string | 是 | 搜索应用 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/searchbots/detail?search_id=search_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "search_123",
    "name": "Knowledge Search",
    "avatar": "data:image/png;base64,iVBORw0KGgo...",
    "description": "A search application for internal knowledge base",
    "tenant_id": "tenant_1",
    "created_by": "user_1",
    "create_time": 1704067200000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1704153600000,
    "update_date": "2024-01-02 12:00:00",
    "status": "1",
    "search_config": {
      "kb_ids": ["kb_1", "kb_2"],
      "doc_ids": [],
      "similarity_threshold": 0.2,
      "vector_similarity_weight": 0.3,
      "use_kg": false,
      "rerank_id": "",
      "top_k": 1024,
      "summary": true,
      "chat_id": "llm_model_1",
      "llm_setting": {
        "temperature": 0.1,
        "top_p": 0.3
      },
      "cross_languages": [],
      "highlight": true,
      "keyword": false,
      "web_search": false,
      "related_search": true,
      "query_mindmap": false
    }
  }
}
```

---

## 22. 搜索机器人思维导图 (Searchbot Mindmap)

生成搜索结果的思维导图。

- **URL**: `/searchbots/mindmap`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `kb_ids` | list[string] | 是 | 知识库 ID 列表 |
| `search_id` | string | 否 | 搜索应用 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/mindmap" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Explain machine learning concepts",
           "kb_ids": ["kb_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "name": "Machine Learning Concepts",
    "children": [
      {
        "name": "Supervised Learning",
        "children": [
          {"name": "Classification"},
          {"name": "Regression"}
        ]
      },
      {
        "name": "Unsupervised Learning",
        "children": [
          {"name": "Clustering"},
          {"name": "Dimensionality Reduction"}
        ]
      },
      {
        "name": "Reinforcement Learning",
        "children": [
          {"name": "Q-Learning"},
          {"name": "Policy Gradient"}
        ]
      }
    ]
  }
}
```


---


# API Token API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建 API Token (New Token)

为对话 (Dialog) 或 Agent Canvas 创建一个新的 API Token。

- **URL**: `/new_token`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 否 | Dialog ID (若未提供 canvas_id 则必填) |
| `canvas_id` | string | 否 | Canvas ID (若提供，则是 Agent 模式) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/new_token" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "dialog_id": "dialog_123"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "tenant_id": "tenant_1",
    "token": "ragflow-xxxxx",
    "dialog_id": "dialog_123",
    "source": null,
    "beta": null,
    "create_time": 1700000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": null,
    "update_date": null
  }
}
```

若传入 `canvas_id`，则 `source` 为 `"agent"`：

```json
{
  "code": 0,
  "data": {
    "tenant_id": "tenant_1",
    "token": "ragflow-xxxxx",
    "dialog_id": "canvas_123",
    "source": "agent",
    "beta": null,
    "create_time": 1700000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": null,
    "update_date": null
  }
}
```

---


## 2. 获取 Token 列表 (Token List)

获取指定 Dialog 或 Canvas 的 Token 列表。

- **URL**: `/token_list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 否 | Dialog ID (或 Canvas ID) |
| `canvas_id` | string | 否 | Canvas ID (同 dialog_id) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/token_list?dialog_id=dialog_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "tenant_id": "tenant_1",
      "token": "ragflow-xxxxx",
      "dialog_id": "dialog_123",
      "source": null,
      "beta": null,
      "create_time": 1700000000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1700001000,
      "update_date": "2024-01-01 12:16:40"
    }
  ]
}
```

---


## 3. 删除 Token (Remove Token)

删除一个或多个 API Token。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tokens` | list[string] | 是 | 要删除的 Token 列表 |
| `tenant_id` | string | 是 | Tenant ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "tokens": ["token_1", "token_2"],
           "tenant_id": "tenant_1"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 4. 获取统计信息 (Stats)

获取对话或 Agent 的统计信息 (PV, UV, Speed, Tokens 等)。

- **URL**: `/stats`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `from_date` | string | 否 | 开始日期 (yyyy-MM-dd HH:mm:ss, 默认7天前) |
| `to_date` | string | 否 | 结束日期 (yyyy-MM-dd HH:mm:ss, 默认现在) |
| `canvas_id` | string | 否 | Canvas ID (若存在则查询 Agent 统计) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/stats?canvas_id=canvas_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "pv": [["2024-01-01", 10], ["2024-01-02", 15]],
    "uv": [["2024-01-01", 5], ["2024-01-02", 8]],
    "speed": [["2024-01-01", 15.5], ["2024-01-02", 18.2]],
    "tokens": [["2024-01-01", 1.2], ["2024-01-02", 2.5]],
    "round": [["2024-01-01", 3.5], ["2024-01-02", 4.2]],
    "thumb_up": [["2024-01-01", 2], ["2024-01-02", 5]]
  }
}
```

### 响应字段说明

| 字段名 | 类型 | 描述 |
| :--- | :--- | :--- |
| `pv` | list | 页面访问量，格式为 `[[日期, 数量], ...]` |
| `uv` | list | 独立访客数，格式为 `[[日期, 数量], ...]` |
| `speed` | list | 平均响应速度 (tokens/秒)，格式为 `[[日期, 速度], ...]` |
| `tokens` | list | Token 消耗量 (千)，格式为 `[[日期, 数量], ...]` |
| `round` | list | 平均对话轮数，格式为 `[[日期, 轮数], ...]` |
| `thumb_up` | list | 点赞数，格式为 `[[日期, 数量], ...]` |


---


# Canvas API 文档

**Base URL**: `http://localhost:9380/v1/canvas`

**Authentication**:
绝大多数接口需要认证。请在 Header 中携带 API Key 或登录后的 Authorization Token：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取画布模板 (Templates)

获取系统提供的画布模板列表。

- **URL**: `/templates`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/templates" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "template_1",
      "avatar": null,
      "title": {"en": "Translation Agent", "zh": "翻译代理"},
      "description": {"en": "A template for translation tasks.", "zh": "用于翻译任务的模板。"},
      "canvas_type": "chatbot",
      "canvas_category": "agent_canvas",
      "dsl": {},
      "create_time": 1704067200000,
      "create_date": "2024-01-01 00:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 00:00:00"
    }
  ],
  "message": "success"
}
```

---


## 2. 删除画布 (Remove Canvas)

删除一个或多个画布。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_ids` | list[string] | 是 | 要删除的 Canvas ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/rm" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "canvas_ids": ["canvas_1", "canvas_2"]
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


## 3. 保存/创建画布 (Save/Set Canvas)

创建新的画布或更新现有画布。

- **URL**: `/set`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dsl` | string/object | 是 | 画布的 DSL (JSON 结构) |
| `title` | string | 是 | 画布标题 |
| `id` | string | 否 | Canvas ID (更新时必填，创建时不填) |
| `canvas_category` | string | 否 | 画布类别 (默认 "Agent") |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/set" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "title": "My New Agent",
           "dsl": {}
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "title": "My New Agent",
    "dsl": {},
    "user_id": "user_123456"
  },
  "message": "success"
}
```

---


## 4. 获取画布详情 (Get Canvas)

获取指定 ID 的画布详情。

- **URL**: `/get/<canvas_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/get/canvas_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "canvas_123",
    "avatar": null,
    "title": "My Agent",
    "dsl": {},
    "description": "A sample agent",
    "permission": "me",
    "update_time": 1704067200000,
    "user_id": "user_123456",
    "create_time": 1704067200000,
    "create_date": "2024-01-01 00:00:00",
    "update_date": "2024-01-01 00:00:00",
    "canvas_category": "agent_canvas",
    "nickname": "John Doe",
    "tenant_avatar": null
  },
  "message": "success"
}
```

---


## 5. 获取画布详情 (Get Canvas SSE)

通过 API Key 获取画布详情 (主要用于 SSE 场景下的鉴权)。

- **URL**: `/getsse/<canvas_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/getsse/canvas_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "canvas_123",
    "avatar": null,
    "user_id": "user_123456",
    "title": "My Agent",
    "permission": "me",
    "description": "A sample agent",
    "canvas_type": "chatbot",
    "canvas_category": "agent_canvas",
    "dsl": {},
    "create_time": 1704067200000,
    "create_date": "2024-01-01 00:00:00",
    "update_time": 1704067200000,
    "update_date": "2024-01-01 00:00:00"
  },
  "message": "success"
}
```

---


## 6. 运行画布 (Completion)

运行画布 (Agent 或 DataFlow)。返回 SSE 流。

- **URL**: `/completion`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Canvas ID |
| `query` | string | 否 | 用户输入的问题 |
| `files` | list | 否 | 上传的文件列表 |
| `inputs` | object | 否 | 其他输入参数 |
| `user_id` | string | 否 | 用户 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/completion" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "query": "Hello world"
         }'
```

### 响应示例 (SSE Stream)

**Agent 模式**:
```
data: {"event": "message", "data": {"content": "Thinking...", "node_id": "step_1"}}

data: {"event": "message", "data": {"content": "Hello! How can I help you?", "node_id": "step_2"}}

data: {"event": "message_end", "data": {"reference": {}}}
```

**DataFlow 模式**:
```json
{
  "code": 0,
  "data": {
    "message_id": "task_uuid_12345678"
  },
  "message": "success"
}
```

---


## 7. 重跑任务 (Rerun)

重跑某个任务或组件。

- **URL**: `/rerun`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Pipeline Operation Log ID |
| `dsl` | object | 是 | 画布 DSL |
| `component_id` | string | 是 | 需要重跑的组件 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/rerun" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "log_123",
           "component_id": "component_abc",
           "dsl": {}
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


## 8. 取消任务 (Cancel Task)

取消正在运行的任务。

- **URL**: `/cancel/<task_id>`
- **Method**: `PUT`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `task_id` | string | 是 | 任务 ID |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/canvas/cancel/task_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
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


## 9. 重置画布 (Reset Canvas)

重置画布状态。

- **URL**: `/reset`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Canvas ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/reset" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "components": {},
    "history": [],
    "messages": [],
    "path": [],
    "answer": []
  },
  "message": "success"
}
```

---


## 10. 上传文件 (Upload File)

上传文件到指定画布。

- **URL**: `/upload/<canvas_id>`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Path/Query/Form)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID (Path) |
| `file` | file | 否 | 文件内容 (Form Data) |
| `url` | string | 否 | 文件 URL (Query, 若无 file 则使用 url) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/upload/canvas_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -F "file=@/path/to/file.pdf"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4-uuid-location",
    "name": "file.pdf",
    "size": 102400,
    "extension": "pdf",
    "mime_type": "application/pdf",
    "created_by": "user_123456",
    "created_at": 1704067200.123,
    "preview_url": null
  },
  "message": "success"
}
```

---


## 11. 获取组件输入表单 (Input Form)

获取画布中组件的输入表单结构。

- **URL**: `/input_form`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Canvas ID |
| `component_id` | string | 是 | 组件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/input_form?id=canvas_123&component_id=comp_1" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "key": "query",
      "name": "User Query",
      "type": "string",
      "optional": false
    },
    {
      "key": "temperature",
      "name": "Temperature",
      "type": "number",
      "optional": true
    }
  ],
  "message": "success"
}
```

---


## 12. 调试组件 (Debug Component)

调试运行单个组件。

- **URL**: `/debug`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Canvas ID |
| `component_id` | string | 是 | 组件 ID |
| `params` | object | 是 | 调试参数 (key: {value: ...}) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/debug" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "component_id": "llm_component",
           "params": {
             "prompt": {"value": "Hello"}
           }
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "content": "This is the result from the LLM component.",
    "usage": {
      "prompt_tokens": 10,
      "completion_tokens": 50,
      "total_tokens": 60
    }
  },
  "message": "success"
}
```

---


## 13. 测试数据库连接 (Test DB Connect)

测试各种数据库连接 (MySQL, Postgres, MSSQL, Trino, IBM DB2 等)。

- **URL**: `/test_db_connect`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `db_type` | string | 是 | 数据库类型 (mysql, mariadb, postgres, mssql, trino, IBM DB2) |
| `database` | string | 是 | 数据库名 |
| `username` | string | 是 | 用户名 |
| `host` | string | 是 | 主机地址 |
| `port` | int | 是 | 端口 |
| `password` | string | 是 | 密码 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/test_db_connect" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "db_type": "mysql",
           "host": "localhost",
           "port": 3306,
           "username": "root",
           "password": "password",
           "database": "test_db"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": "Database Connection Successful!",
  "message": "success"
}
```

---


## 14. 获取版本列表 (Get Version List)

获取画布的历史版本列表。

- **URL**: `/getlistversion/<canvas_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/getlistversion/canvas_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "version_1",
      "title": "My Agent_2024_01_15_10_30_00",
      "user_canvas_id": "canvas_123",
      "create_time": 1705312200000,
      "create_date": "2024-01-15 10:30:00",
      "update_time": 1705312200000,
      "update_date": "2024-01-15 10:30:00"
    },
    {
      "id": "version_2",
      "title": "My Agent_2024_01_14_09_00_00",
      "user_canvas_id": "canvas_123",
      "create_time": 1705220400000,
      "create_date": "2024-01-14 09:00:00",
      "update_time": 1705220400000,
      "update_date": "2024-01-14 09:00:00"
    }
  ],
  "message": "success"
}
```

---


## 15. 获取版本详情 (Get Version)

获取指定版本的画布 DSL。

- **URL**: `/getversion/<version_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `version_id` | string | 是 | 版本 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/getversion/ver_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "ver_123",
    "user_canvas_id": "canvas_123",
    "title": "My Agent_2024_01_15_10_30_00",
    "description": null,
    "dsl": {
      "components": {},
      "history": [],
      "messages": [],
      "path": [],
      "answer": []
    },
    "create_time": 1705312200000,
    "create_date": "2024-01-15 10:30:00",
    "update_time": 1705312200000,
    "update_date": "2024-01-15 10:30:00"
  },
  "message": "success"
}
```

---


## 16. 画布列表 (List Canvas)

获取用户的画布列表，支持分页和搜索。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `keywords` | string | 否 | 搜索关键词 |
| `page` | int | 否 | 页码 (默认 0) |
| `page_size` | int | 否 | 每页数量 (默认 0 表示全部) |
| `orderby` | string | 否 | 排序字段 (默认 create_time) |
| `desc` | boolean | 否 | 是否倒序 (默认 true) |
| `canvas_category` | string | 否 | 类别筛选 (agent_canvas / dataflow_canvas) |
| `owner_ids` | string | 否 | 逗号分隔的 User ID 列表 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/list?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "canvas": [
      {
        "id": "canvas_123",
        "avatar": null,
        "title": "My Agent",
        "dsl": {},
        "description": "A sample agent",
        "permission": "me",
        "tenant_id": "user_123456",
        "nickname": "John Doe",
        "tenant_avatar": null,
        "update_time": 1704067200000,
        "canvas_category": "agent_canvas"
      }
    ],
    "total": 1
  },
  "message": "success"
}
```

---


## 17. 设置画布 (Setting)

更新画布的元数据 (标题、描述、权限、头像等)。

- **URL**: `/setting`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Canvas ID |
| `title` | string | 是 | 标题 |
| `permission` | string | 是 | 权限设置 (me / team) |
| `description` | string | 否 | 描述 |
| `avatar` | string | 否 | 头像 (base64 字符串) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/setting" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "title": "New Title",
           "permission": "team"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": 1,
  "message": "success"
}
```

---


## 18. 追踪日志 (Trace)

获取画布运行的详细追踪日志。

- **URL**: `/trace`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID |
| `message_id` | string | 是 | 消息 ID (运行 ID) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/trace?canvas_id=c1&message_id=m1" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "component_1": {
      "start_time": 1704067200.123,
      "end_time": 1704067201.456,
      "inputs": {},
      "outputs": {},
      "status": "success"
    },
    "component_2": {
      "start_time": 1704067201.456,
      "end_time": 1704067202.789,
      "inputs": {},
      "outputs": {},
      "status": "success"
    }
  },
  "message": "success"
}
```

如果没有找到日志:
```json
{
  "code": 0,
  "data": {},
  "message": "success"
}
```

---


## 19. 获取会话列表 (Sessions)

获取画布的对话历史会话。

- **URL**: `/<canvas_id>/sessions`
- **Method**: `GET`

### 请求参数 (Path/Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID (Path) |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 30) |
| `user_id` | string | 否 | 用户 ID 筛选 |
| `keywords` | string | 否 | 搜索关键词 |
| `from_date` | string | 否 | 起始日期 |
| `to_date` | string | 否 | 结束日期 |
| `orderby` | string | 否 | 排序字段 (默认 update_time) |
| `desc` | boolean | 否 | 是否倒序 (默认 true) |
| `dsl` | boolean | 否 | 是否包含 DSL (默认 true) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/canvas_123/sessions" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 50,
    "sessions": [
      {
        "id": "session_abc123",
        "dialog_id": "canvas_123",
        "user_id": "external_user_1",
        "message": [
          {"role": "user", "content": "Hello", "id": "msg_1"},
          {"role": "assistant", "content": "Hi! How can I help?", "id": "msg_1", "created_at": 1704067200.123}
        ],
        "reference": [],
        "tokens": 150,
        "source": "agent",
        "dsl": {},
        "duration": 2.5,
        "round": 1,
        "thumb_up": 0,
        "errors": null,
        "create_time": 1704067200000,
        "create_date": "2024-01-01 00:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 00:00:00"
      }
    ]
  },
  "message": "success"
}
```

---


## 20. 获取 Prompt 模板 (Prompts)

获取系统内置的 Prompt 模板。

- **URL**: `/prompts`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/prompts" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "task_analysis": "You are an intelligent assistant...\n\nPlease analyze the following task...",
    "plan_generation": "Based on the analysis, generate a step-by-step plan...",
    "reflection": "Review the previous response and identify...",
    "citation_guidelines": "When citing sources, use the following format..."
  },
  "message": "success"
}
```

---


## 21. 下载文件 (Download)

下载画布相关的文件。

- **URL**: `/download`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | 文件 ID (location) |
| `created_by` | string | 是 | 创建者 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/download?id=file_location_uuid&created_by=user_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -o downloaded_file.pdf
```

### 响应示例
(二进制文件流)

---


# Chunk API 文档

**Base URL**: `http://localhost:9380/v1/chunk`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取 Chunk 列表 (List Chunks)

获取指定文档 (Document) 的 Chunk 列表，支持分页和关键词搜索。

- **URL**: `/list`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_id` | string | 是 | 文档 ID |
| `page` | int | 否 | 页码 (默认 1) |
| `size` | int | 否 | 每页数量 (默认 30) |
| `keywords` | string | 否 | 搜索关键词 |
| `available_int` | int | 否 | 筛选状态 (1: 启用, 0: 禁用) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/chunk/list" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "doc_id": "doc_123",
           "page": 1,
           "size": 10,
           "keywords": "test"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 100,
    "chunks": [
      {
        "chunk_id": "chunk_abc",
        "content_with_weight": "This is a chunk content...",
        "doc_id": "doc_123",
        "docnm_kwd": "example.pdf",
        "important_kwd": ["keyword1"],
        "question_kwd": ["question?"],
        "image_id": "",
        "available_int": 1,
        "positions": [],
        "doc_type_kwd": "pdf"
      }
    ],
    "doc": {
      "id": "doc_123",
      "thumbnail": null,
      "kb_id": "kb_456",
      "parser_id": "naive",
      "pipeline_id": null,
      "parser_config": {
        "pages": [[1, 1000000]],
        "table_context_size": 0,
        "image_context_size": 0
      },
      "source_type": "local",
      "type": "pdf",
      "created_by": "user_789",
      "name": "example.pdf",
      "location": "kb_456/doc_123",
      "size": 102400,
      "token_num": 5000,
      "chunk_num": 10,
      "progress": 1.0,
      "progress_msg": "Task done",
      "process_begin_at": "2024-01-01 10:00:00",
      "process_duration": 12.5,
      "meta_fields": {},
      "suffix": "pdf",
      "run": "3",
      "status": "1",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 10:00:00",
      "update_time": 1704067212000,
      "update_date": "2024-01-01 10:00:12"
    }
  },
  "message": "success"
}
```

---


## 2. 获取 Chunk 详情 (Get Chunk)

获取指定 Chunk 的详细信息。

- **URL**: `/get`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chunk_id` | string | 是 | Chunk ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/chunk/get?chunk_id=chunk_abc" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "chunk_abc",
    "content_with_weight": "This is a chunk content...",
    "doc_id": "doc_123",
    "kb_id": ["kb_456"],
    "docnm_kwd": "example.pdf",
    "title_tks": "example pdf",
    "important_kwd": ["keyword1", "keyword2"],
    "important_tks": "keyword1 keyword2",
    "question_kwd": ["What is this?"],
    "question_tks": "what is this",
    "tag_kwd": ["tag1"],
    "tag_feas": {},
    "available_int": 1,
    "img_id": "",
    "position_int": [],
    "doc_type_kwd": "pdf",
    "create_time": "2024-01-01 10:00:00",
    "create_timestamp_flt": 1704067200.0
  },
  "message": "success"
}
```

---


## 3. 设置 Chunk (Set Chunk)

更新指定 Chunk 的内容和属性。

- **URL**: `/set`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_id` | string | 是 | 文档 ID |
| `chunk_id` | string | 是 | Chunk ID |
| `content_with_weight` | string | 是 | Chunk 内容 |
| `important_kwd` | list[string] | 否 | 关键词列表 |
| `question_kwd` | list[string] | 否 | 问题列表 |
| `tag_kwd` | list[string] | 否 | 标签关键词列表 |
| `tag_feas` | object | 否 | 标签特征 |
| `available_int` | int | 否 | 状态 (1: 启用, 0: 禁用) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/chunk/set" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "doc_id": "doc_123",
           "chunk_id": "chunk_abc",
           "content_with_weight": "Updated content...",
           "important_kwd": ["AI", "RAG"]
         }'
```

### 成功响应示例
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

### 错误响应示例
```json
{
  "code": 102,
  "data": null,
  "message": "Tenant not found!"
}
```

```json
{
  "code": 102,
  "data": null,
  "message": "Document not found!"
}
```

```json
{
  "code": 102,
  "data": null,
  "message": "`important_kwd` should be a list"
}
```

---


## 4. 切换 Chunk 状态 (Switch Chunk)

批量启用或禁用 Chunk。

- **URL**: `/switch`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chunk_ids` | list[string] | 是 | Chunk ID 列表 |
| `available_int` | int | 是 | 目标状态 (1: 启用, 0: 禁用) |
| `doc_id` | string | 是 | 文档 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/chunk/switch" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "chunk_ids": ["chunk_abc", "chunk_def"],
           "available_int": 0,
           "doc_id": "doc_123"
         }'
```

### 成功响应示例
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

### 错误响应示例
```json
{
  "code": 102,
  "data": null,
  "message": "Document not found!"
}
```

```json
{
  "code": 102,
  "data": null,
  "message": "Index updating failure"
}
```

---


## 5. 删除 Chunk (Remove Chunk)

批量删除 Chunk。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chunk_ids` | list[string] | 是 | Chunk ID 列表 |
| `doc_id` | string | 是 | 文档 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/chunk/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "chunk_ids": ["chunk_abc"],
           "doc_id": "doc_123"
         }'
```

### 成功响应示例
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

### 错误响应示例
```json
{
  "code": 102,
  "data": null,
  "message": "Document not found!"
}
```

```json
{
  "code": 102,
  "data": null,
  "message": "Chunk deleting failure"
}
```

---


## 6. 创建 Chunk (Create Chunk)

在指定文档下创建一个新的 Chunk。

- **URL**: `/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_id` | string | 是 | 文档 ID |
| `content_with_weight` | string | 是 | Chunk 内容 |
| `important_kwd` | list[string] | 否 | 关键词列表 |
| `question_kwd` | list[string] | 否 | 问题列表 |
| `tag_feas` | object | 否 | 标签特征 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/chunk/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "doc_id": "doc_123",
           "content_with_weight": "New chunk content...",
           "important_kwd": ["New"]
         }'
```

### 成功响应示例
```json
{
  "code": 0,
  "data": {
    "chunk_id": "a1b2c3d4e5f67890"
  },
  "message": "success"
}
```

### 错误响应示例
```json
{
  "code": 102,
  "data": null,
  "message": "Document not found!"
}
```

```json
{
  "code": 102,
  "data": null,
  "message": "Tenant not found!"
}
```

```json
{
  "code": 102,
  "data": null,
  "message": "Knowledgebase not found!"
}
```

```json
{
  "code": 102,
  "data": null,
  "message": "`important_kwd` is required to be a list"
}
```

---


## 7. 检索测试 (Retrieval Test)

测试知识库检索效果。

- **URL**: `/retrieval_test`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string/list | 是 | 知识库 ID (Dataset ID) |
| `question` | string | 是 | 测试问题 |
| `page` | int | 否 | 页码 (默认 1) |
| `size` | int | 否 | 每页数量 (默认 30) |
| `doc_ids` | list[string] | 否 | 限定文档 ID 列表 |
| `use_kg` | bool | 否 | 是否使用知识图谱 (默认 False) |
| `top_k` | int | 否 | Top K (默认 1024) |
| `cross_languages` | list[string] | 否 | 跨语言检索列表 |
| `rerank_id` | string | 否 | Rerank 模型 ID |
| `keyword` | bool | 否 | 是否启用关键词提取 (默认 False) |
| `similarity_threshold` | float | 否 | 相似度阈值 (默认 0.0) |
| `vector_similarity_weight` | float | 否 | 向量相似度权重 (默认 0.3) |
| `highlight` | bool | 否 | 是否高亮匹配内容 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/chunk/retrieval_test" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "kb_123",
           "question": "What is RAG?",
           "size": 5,
           "highlight": true
         }'
```

### 成功响应示例
```json
{
  "code": 0,
  "data": {
    "total": 10,
    "chunks": [
      {
        "id": "chunk_abc",
        "content_with_weight": "RAG stands for Retrieval-Augmented Generation...",
        "doc_id": "doc_123",
        "kb_id": ["kb_456"],
        "docnm_kwd": "rag_guide.pdf",
        "important_kwd": ["RAG", "retrieval"],
        "question_kwd": [],
        "img_id": "",
        "available_int": 1,
        "position_int": [[1, 100, 200, 300, 400]],
        "similarity": 0.95,
        "term_similarity": 0.85,
        "vector_similarity": 0.92
      }
    ],
    "labels": ["technology", "ai"]
  },
  "message": "success"
}
```

### 错误响应示例
```json
{
  "code": 102,
  "data": false,
  "message": "Please specify dataset firstly."
}
```

```json
{
  "code": 103,
  "data": false,
  "message": "Only owner of dataset authorized for this operation."
}
```

```json
{
  "code": 102,
  "data": null,
  "message": "Knowledgebase not found!"
}
```

```json
{
  "code": 102,
  "data": false,
  "message": "No chunk found! Check the chunk status please!"
}
```

---


## 8. 获取知识图谱 (Knowledge Graph)

获取文档的知识图谱或思维导图数据。

- **URL**: `/knowledge_graph`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_id` | string | 是 | 文档 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/chunk/knowledge_graph?doc_id=doc_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 成功响应示例
```json
{
  "code": 0,
  "data": {
    "graph": {
      "nodes": [
        {
          "id": "entity_1",
          "label": "RAGFlow",
          "type": "technology"
        },
        {
          "id": "entity_2",
          "label": "LLM",
          "type": "concept"
        }
      ],
      "edges": [
        {
          "source": "entity_1",
          "target": "entity_2",
          "label": "uses"
        }
      ]
    },
    "mind_map": {
      "id": "root",
      "children": [
        {
          "id": "node_1",
          "children": [
            {
              "id": "node_1_1",
              "children": []
            }
          ]
        }
      ]
    }
  },
  "message": "success"
}
```

### 空数据响应示例
```json
{
  "code": 0,
  "data": {
    "graph": {},
    "mind_map": {}
  },
  "message": "success"
}
```

---


# Connector API 文档

**Base URL**: `http://localhost:9380/v1/connector`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 设置 Connector (Set Connector)

创建或更新 Connector 配置。如果不传 `id` 则为创建，传 `id` 则为更新。

- **URL**: `/set`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 否 | Connector ID。若提供则为更新操作，否则为创建。 |
| `name` | string | 是 (创建时) | Connector 名称。 |
| `source` | string | 是 (创建时) | 数据源类型 (e.g., "google_drive", "gmail")。 |
| `config` | object | 是 | 数据源配置信息 (JSON Object)。 |
| `refresh_freq` | int | 否 | 刷新频率 (秒)，默认 30。 |
| `prune_freq` | int | 否 | 清理频率 (秒)，默认 720。 |
| `timeout_secs` | int | 否 | 超时时间 (秒)，默认 1740。 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/set" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Drive Connector",
           "source": "google_drive",
           "config": {"folder_id": "xxx", "api_key": "xxx"},
           "refresh_freq": 60
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "tenant_id": "tenant_abc123",
    "name": "My Drive Connector",
    "source": "google_drive",
    "input_type": "poll",
    "config": {"folder_id": "xxx", "credentials": {}},
    "refresh_freq": 60,
    "prune_freq": 720,
    "timeout_secs": 1740,
    "indexing_start": null,
    "status": "schedule",
    "create_time": 1706150400000,
    "create_date": "2024-01-25 08:00:00",
    "update_time": 1706150400000,
    "update_date": "2024-01-25 08:00:00"
  },
  "message": "success"
}
```

---


## 2. 获取 Connector 列表 (List Connectors)

获取当前用户的 Connector 列表。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/connector/list" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "a1b2c3d4e5f6789012345678",
      "name": "My Drive Connector",
      "source": "google_drive",
      "status": "schedule"
    },
    {
      "id": "b2c3d4e5f67890123456789a",
      "name": "Gmail Connector",
      "source": "gmail",
      "status": "running"
    }
  ],
  "message": "success"
}
```

---


## 3. 获取 Connector 详情 (Get Connector)

根据 ID 获取 Connector 详情。

- **URL**: `/<connector_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `connector_id` | string | 是 | Connector ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/connector/connector_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "tenant_id": "tenant_abc123",
    "name": "My Drive Connector",
    "source": "google_drive",
    "input_type": "poll",
    "config": {"folder_id": "xxx", "credentials": {}},
    "refresh_freq": 60,
    "prune_freq": 720,
    "timeout_secs": 1740,
    "indexing_start": null,
    "status": "schedule",
    "create_time": 1706150400000,
    "create_date": "2024-01-25 08:00:00",
    "update_time": 1706150400000,
    "update_date": "2024-01-25 08:00:00"
  },
  "message": "success"
}
```

---


## 4. 获取同步日志 (List Logs)

获取 Connector 的同步任务日志。

- **URL**: `/<connector_id>/logs`
- **Method**: `GET`

### 请求参数 (Path & Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `connector_id` | string | 是 | Connector ID (Path Param) |
| `page` | int | 否 | 页码，默认 1 |
| `page_size` | int | 否 | 每页条数，默认 15 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/connector/connector_123/logs?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 100,
    "logs": [
      {
        "id": "log_a1b2c3d4e5f6789012345678",
        "connector_id": "a1b2c3d4e5f6789012345678",
        "kb_id": "kb_abc123def456",
        "update_date": "2024-01-25 12:00:00",
        "poll_range_start": "2024-01-01T00:00:00+00:00",
        "poll_range_end": "2024-01-25T12:00:00+00:00",
        "new_docs_indexed": 10,
        "total_docs_indexed": 150,
        "error_msg": "",
        "full_exception_trace": "",
        "error_count": 0,
        "name": "My Drive Connector",
        "source": "google_drive",
        "tenant_id": "tenant_abc123",
        "timeout_secs": 1740,
        "kb_name": "My Knowledge Base",
        "kb_avatar": null,
        "auto_parse": "1",
        "reindex": "0",
        "status": "done",
        "update_time": 1706184000000
      }
    ]
  },
  "message": "success"
}
```

---


## 5. 暂停/恢复/取消 Connector (Resume/Pause)

修改 Connector 的运行状态。

- **URL**: `/<connector_id>/resume`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `resume` | boolean | 否 | `true` 为恢复/启动 (SCHEDULE)，`false` 为取消/停止 (CANCEL)。默认为 `false`。 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/connector/connector_123/resume" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "resume": true
         }'
```

### 响应示例

**成功:**
```json
{
  "code": 0,
  "data": true
}
```

---


## 6. 重建索引 (Rebuild)

触发 Connector 对应知识库的索引重建。

- **URL**: `/<connector_id>/rebuild`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/connector/connector_123/rebuild" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "kb_456"
         }'
```

### 响应示例

**成功:**
```json
{
  "code": 0,
  "data": true
}
```

**失败:**
```json
{
  "code": 100,
  "data": false,
  "message": "Error message describing the failure"
}
```

---


## 7. 删除 Connector (Remove)

删除指定的 Connector。

- **URL**: `/<connector_id>/rm`
- **Method**: `POST`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `connector_id` | string | 是 | Connector ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/connector_123/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 8. 启动 Google OAuth (Start Google OAuth)

发起 Google Drive 或 Gmail 的 OAuth 授权流程 (Web 端)。

- **URL**: `/google/oauth/web/start`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query & Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `type` | string | 否 | OAuth 类型，可选 `google-drive` (默认) 或 `gmail` (Query Param)。 |
| `credentials` | object/string | 是 | Google OAuth 客户端凭证 (Body Param)。 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/google/oauth/web/start?type=google-drive" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "credentials": { "web": { "client_id": "...", "client_secret": "..." } }
         }'
```

### 响应示例

**成功:**
```json
{
  "code": 0,
  "data": {
    "flow_id": "550e8400-e29b-41d4-a716-446655440000",
    "authorization_url": "https://accounts.google.com/o/oauth2/v2/auth?response_type=code&client_id=xxx.apps.googleusercontent.com&redirect_uri=https%3A%2F%2Fexample.com%2Fcallback&scope=...&state=550e8400-e29b-41d4-a716-446655440000&access_type=offline&include_granted_scopes=true&prompt=consent",
    "expires_in": 900
  }
}
```

**错误 (凭证已包含 refresh_token):**
```json
{
  "code": 102,
  "message": "Uploaded credentials already include a refresh token."
}
```

**错误 (缺少 web 配置):**
```json
{
  "code": 102,
  "message": "Google OAuth JSON must include a 'web' client configuration to use browser-based authorization."
}
```

---


## 9. Google Gmail OAuth 回调 (Gmail Callback)

Google OAuth 授权完成后的回调接口 (通常由浏览器重定向调用)。

- **URL**: `/gmail/oauth/web/callback`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `state` | string | 是 | OAuth State ID (Flow ID)。 |
| `code` | string | 是 | 授权码。 |
| `error` | string | 否 | 错误信息。 |

### 响应
返回 HTML 页面，提示授权成功或失败，并自动关闭窗口。

**成功示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Google Gmail Authorization</title></head>
<body>
  <h1>Authorization complete</h1>
  <p>Authorization completed successfully.</p>
  <script>
    window.opener.postMessage({
      "type": "ragflow-gmail-oauth",
      "status": "success",
      "flowId": "550e8400-e29b-41d4-a716-446655440000",
      "message": "Authorization completed successfully."
    }, "*");
    window.close();
  </script>
</body>
</html>
```

**失败示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Google Gmail Authorization</title></head>
<body>
  <h1>Authorization failed</h1>
  <p>Authorization session expired. Please restart from the main window.</p>
</body>
</html>
```

---


## 10. Google Drive OAuth 回调 (Drive Callback)

Google OAuth 授权完成后的回调接口 (通常由浏览器重定向调用)。

- **URL**: `/google-drive/oauth/web/callback`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `state` | string | 是 | OAuth State ID (Flow ID)。 |
| `code` | string | 是 | 授权码。 |
| `error` | string | 否 | 错误信息。 |

### 响应
返回 HTML 页面，提示授权成功或失败，并自动关闭窗口。

**成功示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Google Drive Authorization</title></head>
<body>
  <h1>Authorization complete</h1>
  <p>Authorization completed successfully.</p>
  <script>
    window.opener.postMessage({
      "type": "ragflow-google-drive-oauth",
      "status": "success",
      "flowId": "550e8400-e29b-41d4-a716-446655440000",
      "message": "Authorization completed successfully."
    }, "*");
    window.close();
  </script>
</body>
</html>
```

**失败示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Google Drive Authorization</title></head>
<body>
  <h1>Authorization failed</h1>
  <p>Missing authorization code from Google.</p>
</body>
</html>
```

---


## 11. 轮询 Google OAuth 结果 (Poll Google Result)

前端轮询获取 Google OAuth 的授权结果 (凭证)。

- **URL**: `/google/oauth/web/result`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query & Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `type` | string | 否 | OAuth 类型，可选 `google-drive` 或 `gmail` (Query Param)。 |
| `flow_id` | string | 是 | 授权流程 ID (Body Param)。 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/google/oauth/web/result?type=google-drive" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "flow_id": "uuid_flow_id"
         }'
```

### 响应示例

**成功 (授权已完成):**
```json
{
  "code": 0,
  "data": {
    "credentials": "{\"token\": \"ya29.xxx\", \"refresh_token\": \"1//xxx\", \"token_uri\": \"https://oauth2.googleapis.com/token\", \"client_id\": \"xxx.apps.googleusercontent.com\", \"client_secret\": \"xxx\", \"scopes\": [\"https://www.googleapis.com/auth/drive.readonly\"]}"
  }
}
```

**等待中 (授权尚未完成):**
```json
{
  "code": 110,
  "message": "Authorization is still pending."
}
```

**权限错误:**
```json
{
  "code": 109,
  "message": "You are not allowed to access this authorization result."
}
```

---


## 12. 启动 Box OAuth (Start Box OAuth)

发起 Box 的 OAuth 授权流程。

- **URL**: `/box/oauth/web/start`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `client_id` | string | 是 | Box Client ID。 |
| `client_secret` | string | 是 | Box Client Secret。 |
| `redirect_uri` | string | 否 | 重定向 URI。 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/box/oauth/web/start" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "client_id": "box_client_id",
           "client_secret": "box_client_secret"
         }'
```

### 响应示例

**成功:**
```json
{
  "code": 0,
  "data": {
    "flow_id": "550e8400-e29b-41d4-a716-446655440000",
    "authorization_url": "https://account.box.com/api/oauth2/authorize?response_type=code&client_id=xxx&redirect_uri=https%3A%2F%2Fexample.com%2Fcallback&state=550e8400-e29b-41d4-a716-446655440000",
    "expires_in": 900
  }
}
```

**错误 (缺少必要参数):**
```json
{
  "code": 102,
  "message": "Box client_id and client_secret are required."
}
```

---


## 13. Box OAuth 回调 (Box Callback)

Box OAuth 授权完成后的回调接口。

- **URL**: `/box/oauth/web/callback`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `state` | string | 是 | OAuth State ID (Flow ID)。 |
| `code` | string | 是 | 授权码。 |
| `error` | string | 否 | 错误信息。 |

### 响应
返回 HTML 页面，提示授权成功或失败，并自动关闭窗口。

**成功示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Box Authorization</title></head>
<body>
  <h1>Authorization complete</h1>
  <p>Authorization completed successfully.</p>
  <script>
    window.opener.postMessage({
      "type": "ragflow-box-oauth",
      "status": "success",
      "flowId": "550e8400-e29b-41d4-a716-446655440000",
      "message": "Authorization completed successfully."
    }, "*");
    window.close();
  </script>
</body>
</html>
```

**失败示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Box Authorization</title></head>
<body>
  <h1>Authorization failed</h1>
  <p>Missing authorization code from Box.</p>
</body>
</html>
```

---


## 14. 轮询 Box OAuth 结果 (Poll Box Result)

前端轮询获取 Box OAuth 的授权结果 (凭证)。

- **URL**: `/box/oauth/web/result`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `flow_id` | string | 是 | 授权流程 ID。 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/box/oauth/web/result" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "flow_id": "uuid_flow_id"
         }'
```

### 响应示例

**成功 (授权已完成):**
```json
{
  "code": 0,
  "data": {
    "credentials": {
      "user_id": "user_abc123def456",
      "client_id": "box_client_id_xxx",
      "client_secret": "box_client_secret_xxx",
      "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
      "refresh_token": "abc123def456ghi789..."
    }
  }
}
```

**等待中 (授权尚未完成):**
```json
{
  "code": 110,
  "message": "Authorization is still pending."
}
```

**权限错误:**
```json
{
  "code": 109,
  "message": "You are not allowed to access this authorization result."
}
```


---


# Conversation API 文档

**Base URL**: `http://localhost:9380/v1/conversation`

**Authentication**:
大部分接口均需要认证（登录态）。
特殊接口如 `/getsse` 需要在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 设置/创建会话 (Set Conversation)

创建新会话或更新现有会话信息。

- **URL**: `/set`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_id` | string | 是 | 会话 ID (客户端生成或现有 ID) |
| `is_new` | boolean | 是 | 是否为新会话 |
| `name` | string | 否 | 会话名称 (默认为 "New conversation") |
| `dialog_id` | string | 否 | Dialog ID (创建新会话时必填) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/set" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_id": "conv_123",
           "is_new": true,
           "name": "My Chat",
           "dialog_id": "dialog_456"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "conv_123",
    "dialog_id": "dialog_456",
    "name": "My Chat",
    "message": [
      {
        "role": "assistant",
        "content": "Hi! I'm your assistant. What can I do for you?"
      }
    ],
    "user_id": "user_abc123",
    "reference": [],
    "create_time": 1706841600000,
    "create_date": "2024-02-02 12:00:00",
    "update_time": 1706841600000,
    "update_date": "2024-02-02 12:00:00"
  },
  "message": "success"
}
```

---


## 2. 获取会话详情 (Get Conversation)

获取指定会话的详细信息。

- **URL**: `/get`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_id` | string | 是 | 会话 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/conversation/get?conversation_id=conv_123"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "conv_123",
    "dialog_id": "dialog_456",
    "name": "My Chat",
    "message": [
      {
        "role": "assistant",
        "content": "Hi! I'm your assistant. What can I do for you?",
        "id": "msg_001"
      },
      {
        "role": "user",
        "content": "Hello",
        "id": "msg_002",
        "created_at": 1706841700.123
      },
      {
        "role": "assistant",
        "content": "Hello! How can I help you today?",
        "id": "msg_002",
        "created_at": 1706841702.456
      }
    ],
    "reference": [
      {
        "chunks": [
          {
            "id": "chunk_001",
            "content": "This is the chunk content...",
            "doc_id": "doc_001",
            "docnm_kwd": "document.pdf",
            "img_id": "",
            "positions": [[10, 20, 100, 50]]
          }
        ],
        "doc_aggs": [
          {
            "doc_id": "doc_001",
            "doc_name": "document.pdf",
            "count": 3
          }
        ],
        "total": 10
      }
    ],
    "user_id": "user_abc123",
    "avatar": "data:image/png;base64,...",
    "create_time": 1706841600000,
    "create_date": "2024-02-02 12:00:00",
    "update_time": 1706841800000,
    "update_date": "2024-02-02 12:03:20"
  },
  "message": "success"
}
```

---


## 3. 获取 SSE 会话信息 (Get SSE)

通过 API Token 获取会话的基本信息（通常用于外部集成）。

- **URL**: `/getsse/<dialog_id>`
- **Method**: `GET`
- **Authentication**: `Authorization: Bearer <API_TOKEN>`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | Dialog ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/conversation/getsse/dialog_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "dialog_123",
    "tenant_id": "tenant_abc",
    "name": "Customer Support Bot",
    "description": "A helpful assistant for customer inquiries",
    "avatar": "data:image/png;base64,...",
    "language": "English",
    "llm_id": "gpt-4",
    "llm_setting": {
      "temperature": 0.1,
      "top_p": 0.3,
      "frequency_penalty": 0.7,
      "presence_penalty": 0.4,
      "max_tokens": 512
    },
    "prompt_type": "simple",
    "prompt_config": {
      "system": "",
      "prologue": "Hi! I'm your assistant. What can I do for you?",
      "parameters": [],
      "empty_response": "Sorry! No relevant content was found in the knowledge base!"
    },
    "similarity_threshold": 0.2,
    "vector_similarity_weight": 0.3,
    "top_n": 6,
    "top_k": 1024,
    "do_refer": "1",
    "rerank_id": "",
    "kb_ids": ["kb_001", "kb_002"],
    "status": "1",
    "create_time": 1706841600000,
    "update_time": 1706841600000
  },
  "message": "success"
}
```

---


## 4. 删除会话 (Remove Conversation)

删除一个或多个会话。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_ids` | list[string] | 是 | 要删除的会话 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/rm" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_ids": ["conv_123", "conv_124"]
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


## 5. 获取会话列表 (List Conversations)

获取指定 Dialog 下的会话列表。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | Dialog ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/conversation/list?dialog_id=dialog_123"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "conv_123",
      "dialog_id": "dialog_123",
      "name": "Chat Session 1",
      "message": [
        {
          "role": "assistant",
          "content": "Hi! I'm your assistant."
        }
      ],
      "reference": [],
      "user_id": "user_abc123",
      "create_time": 1706841600000,
      "create_date": "2024-02-02 12:00:00",
      "update_time": 1706841800000,
      "update_date": "2024-02-02 12:03:20"
    },
    {
      "id": "conv_124",
      "dialog_id": "dialog_123",
      "name": "Chat Session 2",
      "message": [
        {
          "role": "assistant",
          "content": "Hello! How can I help you?"
        }
      ],
      "reference": [],
      "user_id": "user_abc123",
      "create_time": 1706841500000,
      "create_date": "2024-02-02 11:58:20",
      "update_time": 1706841500000,
      "update_date": "2024-02-02 11:58:20"
    }
  ],
  "message": "success"
}
```

---


## 6. 对话补全 (Completion)

发送消息并获取 AI 回复。支持流式 (SSE) 和非流式响应。

- **URL**: `/completion`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_id` | string | 是 | 会话 ID |
| `messages` | list[dict] | 是 | 消息历史列表 (`[{"role": "user", "content": "...", "id": "..."}]`) |
| `llm_id` | string | 否 | 指定使用的 LLM 模型 ID |
| `stream` | boolean | 否 | 是否流式返回 (默认 true) |
| `temperature` | float | 否 | 模型温度 |
| `top_p` | float | 否 | Top P |
| `frequency_penalty` | float | 否 | 频率惩罚 |
| `presence_penalty` | float | 否 | 存在惩罚 |
| `max_tokens` | int | 否 | 最大 Token 数 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/completion" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_id": "conv_123",
           "messages": [
             {"role": "assistant", "content": "Hi! How can I help you?"},
             {"role": "user", "content": "What is RAG?", "id": "msg_001"}
           ],
           "stream": true
         }'
```

### 响应示例 (流式)
```text
data:{"code": 0, "message": "", "data": {"answer": "RAG stands for", "reference": {"chunks": [], "doc_aggs": []}, "id": "msg_001", "session_id": "conv_123"}}

data:{"code": 0, "message": "", "data": {"answer": "RAG stands for Retrieval-Augmented Generation", "reference": {"chunks": [], "doc_aggs": []}, "id": "msg_001", "session_id": "conv_123"}}

data:{"code": 0, "message": "", "data": {"answer": "RAG stands for Retrieval-Augmented Generation. It is a technique that combines...", "reference": {"chunks": [{"id": "chunk_001", "content": "RAG is a powerful technique...", "doc_id": "doc_001", "docnm_kwd": "rag_guide.pdf", "img_id": "", "positions": [[10, 20, 100, 50]]}], "doc_aggs": [{"doc_id": "doc_001", "doc_name": "rag_guide.pdf", "count": 2}], "total": 5}, "id": "msg_001", "session_id": "conv_123"}}

data:{"code": 0, "message": "", "data": true}
```

### 响应示例 (非流式)
```json
{
  "code": 0,
  "data": {
    "answer": "RAG stands for Retrieval-Augmented Generation. It is a technique that combines information retrieval with text generation to provide more accurate and contextual responses.",
    "reference": {
      "chunks": [
        {
          "id": "chunk_001",
          "content": "RAG is a powerful technique that enhances language models...",
          "doc_id": "doc_001",
          "docnm_kwd": "rag_guide.pdf",
          "img_id": "",
          "positions": [[10, 20, 100, 50]]
        }
      ],
      "doc_aggs": [
        {
          "doc_id": "doc_001",
          "doc_name": "rag_guide.pdf",
          "count": 2
        }
      ],
      "total": 5
    },
    "id": "msg_001",
    "session_id": "conv_123"
  },
  "message": "success"
}
```

---


## 7. 音频转文字 (Sequence to Text)

上传音频文件并转换为文字 (ASR)。

- **URL**: `/sequence2txt`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Form Data)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file` | file | 是 | 音频文件 (wav, mp3, m4a, aac, flac, ogg, webm, opus, wma) |
| `stream` | string | 否 | 是否流式返回 ("true" 或 "false"，默认 "false") |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/sequence2txt" \
     -F "file=@/path/to/audio.mp3" \
     -F "stream=false"
```

### 响应示例 (非流式)
```json
{
  "code": 0,
  "data": {
    "text": "Hello, this is the transcribed text from the audio file."
  },
  "message": "success"
}
```

### 响应示例 (流式)
```text
data: {"event": "partial", "text": "Hello, this is"}

data: {"event": "partial", "text": "Hello, this is the transcribed"}

data: {"event": "final", "text": "Hello, this is the transcribed text from the audio file."}
```

---


## 8. 文字转语音 (TTS)

将文本转换为语音流。

- **URL**: `/tts`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `text` | string | 是 | 要转换的文本 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/tts" \
     -H "Content-Type: application/json" \
     -d '{
           "text": "Hello world"
         }' \
     --output output.mp3
```

### 响应
返回音频流 (`audio/mpeg`)，包含以下 HTTP 头：
- `Content-Type: audio/mpeg`
- `Cache-Control: no-cache`
- `Connection: keep-alive`
- `X-Accel-Buffering: no`

---


## 9. 删除消息 (Delete Message)

删除会话中的指定消息（包含用户问题和对应的助手回复）。

- **URL**: `/delete_msg`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_id` | string | 是 | 会话 ID |
| `message_id` | string | 是 | 消息 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/delete_msg" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_id": "conv_123",
           "message_id": "msg_456"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "conv_123",
    "dialog_id": "dialog_456",
    "name": "My Chat",
    "message": [
      {
        "role": "assistant",
        "content": "Hi! I'm your assistant. What can I do for you?"
      }
    ],
    "reference": [],
    "user_id": "user_abc123",
    "create_time": 1706841600000,
    "update_time": 1706842000000
  },
  "message": "success"
}
```

---


## 10. 消息点赞/点踩 (Thumb Up/Down)

对 AI 的回复进行点赞或点踩。

- **URL**: `/thumbup`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_id` | string | 是 | 会话 ID |
| `message_id` | string | 是 | 消息 ID |
| `thumbup` | boolean | 是 | true: 点赞, false: 点踩 |
| `feedback` | string | 否 | 反馈内容 (点踩时可选) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/thumbup" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_id": "conv_123",
           "message_id": "msg_456",
           "thumbup": false,
           "feedback": "The answer was not accurate"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "conv_123",
    "dialog_id": "dialog_456",
    "name": "My Chat",
    "message": [
      {
        "role": "assistant",
        "content": "Hi! I'm your assistant. What can I do for you?"
      },
      {
        "role": "user",
        "content": "What is RAG?",
        "id": "msg_456"
      },
      {
        "role": "assistant",
        "content": "RAG stands for Retrieval-Augmented Generation...",
        "id": "msg_456",
        "thumbup": false,
        "feedback": "The answer was not accurate"
      }
    ],
    "reference": [],
    "user_id": "user_abc123",
    "create_time": 1706841600000,
    "update_time": 1706842100000
  },
  "message": "success"
}
```

---


## 11. 知识库问答 (Ask)

直接向知识库提问 (Ask about)。返回流式数据。

- **URL**: `/ask`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题内容 |
| `kb_ids` | list[string] | 是 | 知识库 ID 列表 |
| `search_id` | string | 否 | 搜索配置 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/ask" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAG?",
           "kb_ids": ["kb_001"]
         }'
```

### 响应示例 (流式)
```text
data:{"code": 0, "message": "", "data": {"answer": "RAG stands for", "reference": {}}}

data:{"code": 0, "message": "", "data": {"answer": "RAG stands for Retrieval-Augmented Generation", "reference": {}}}

data:{"code": 0, "message": "", "data": {"answer": "RAG stands for Retrieval-Augmented Generation. It is a technique that combines retrieval and generation to provide more accurate responses. ##0$$", "reference": {"chunks": [{"id": "chunk_001", "content": "RAG (Retrieval-Augmented Generation) is a powerful technique...", "doc_id": "doc_001", "docnm_kwd": "rag_guide.pdf", "img_id": "", "positions": []}], "doc_aggs": [{"doc_id": "doc_001", "doc_name": "rag_guide.pdf", "count": 1}], "total": 3}}}

data:{"code": 0, "message": "", "data": true}
```

---


## 12. 生成思维导图 (Mindmap)

根据问题和知识库生成思维导图数据。

- **URL**: `/mindmap`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题/主题 |
| `kb_ids` | list[string] | 是 | 知识库 ID 列表 |
| `search_id` | string | 否 | 搜索配置 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/mindmap" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Machine Learning Overview",
           "kb_ids": ["kb_001"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "root",
    "topic": "Machine Learning Overview",
    "children": [
      {
        "id": "node_1",
        "topic": "Supervised Learning",
        "children": [
          {
            "id": "node_1_1",
            "topic": "Classification"
          },
          {
            "id": "node_1_2",
            "topic": "Regression"
          }
        ]
      },
      {
        "id": "node_2",
        "topic": "Unsupervised Learning",
        "children": [
          {
            "id": "node_2_1",
            "topic": "Clustering"
          },
          {
            "id": "node_2_2",
            "topic": "Dimensionality Reduction"
          }
        ]
      },
      {
        "id": "node_3",
        "topic": "Reinforcement Learning"
      }
    ]
  },
  "message": "success"
}
```

---


## 13. 相关问题建议 (Related Questions)

根据当前问题生成相关问题建议。

- **URL**: `/related_questions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 当前问题 |
| `search_id` | string | 否 | 搜索配置 ID (用于获取 LLM 设置) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/related_questions" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "How to install Docker?"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    "What are the system requirements for Docker?",
    "How to run a container in Docker?",
    "What is the difference between Docker and virtual machines?",
    "How to write a Dockerfile?",
    "How to use Docker Compose?"
  ],
  "message": "success"
}
```

---


## 错误响应

当发生错误时，API 会返回以下格式的响应：

### 数据错误
```json
{
  "code": 101,
  "message": "Conversation not found!"
}
```

### 权限错误
```json
{
  "code": 109,
  "message": "Only owner of conversation authorized for this operation."
}
```

### 服务器错误
```json
{
  "code": 500,
  "message": "Exception('Internal server error')"
}
```

### 流式错误响应
```text
data:{"code": 500, "message": "Error message here", "data": {"answer": "**ERROR**: Error message here", "reference": []}}
```

---


# Dialog API 文档

**Base URL**: `http://localhost:9380/v1/dialog`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 设置对话 (Set Dialog)

创建或更新一个对话 (Assistant)。如果不提供 `dialog_id`，则创建一个新的对话。

- **URL**: `/set`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 否 | Dialog ID (若未提供则创建新 Dialog) |
| `name` | string | 否 | Dialog 名称 (默认为 "New Dialog") |
| `description` | string | 否 | Dialog 描述 |
| `icon` | string | 否 | Dialog 图标 |
| `kb_ids` | list[string] | 否 | 关联的知识库 ID 列表 |
| `llm_id` | string | 否 | LLM 模型 ID (如 "deepseek-chat") |
| `llm_setting` | object | 否 | LLM 参数设置 (如 temperature) |
| `prompt_config` | object | 是 | Prompt 配置 (包含 system, parameters 等) |
| `top_n` | int | 否 | 引用片段数量 (默认 6) |
| `top_k` | int | 否 | 搜索候选项数量 (默认 1024) |
| `rerank_id` | string | 否 | Rerank 模型 ID |
| `similarity_threshold` | float | 否 | 相似度阈值 (默认 0.1) |
| `vector_similarity_weight` | float | 否 | 向量搜索权重 (1.0为纯向量，0为纯关键词，默认 0.3) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/dialog/set" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Assistant",
           "kb_ids": ["kb_123"],
           "llm_id": "chatgpt-3.5",
           "prompt_config": {
               "system": "You are a helpful assistant.",
               "parameters": []
           }
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "dialog_xyz",
    "tenant_id": "user_abc",
    "name": "My Assistant",
    "description": "A helpful dialog",
    "icon": "",
    "language": "English",
    "llm_id": "chatgpt-3.5",
    "llm_setting": {
      "temperature": 0.1,
      "top_p": 0.3,
      "frequency_penalty": 0.7,
      "presence_penalty": 0.4,
      "max_tokens": 512
    },
    "prompt_type": "simple",
    "prompt_config": {
      "system": "You are a helpful assistant.",
      "prologue": "Hi! I'm your assistant. What can I do for you?",
      "parameters": [],
      "empty_response": "Sorry! No relevant content was found in the knowledge base!"
    },
    "meta_data_filter": {},
    "similarity_threshold": 0.1,
    "vector_similarity_weight": 0.3,
    "top_n": 6,
    "top_k": 1024,
    "do_refer": "1",
    "rerank_id": "",
    "kb_ids": ["kb_123"],
    "status": "1",
    "create_time": 1700000000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-01 12:00:00"
  },
  "message": "success"
}
```

---


## 2. 获取对话详情 (Get Dialog)

获取指定对话的详细配置。

- **URL**: `/get`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | Dialog ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/dialog/get?dialog_id=dialog_xyz" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "dialog_xyz",
    "tenant_id": "user_abc",
    "name": "My Assistant",
    "description": "A helpful dialog",
    "icon": "",
    "language": "English",
    "llm_id": "chatgpt-3.5",
    "llm_setting": {
      "temperature": 0.1,
      "top_p": 0.3,
      "frequency_penalty": 0.7,
      "presence_penalty": 0.4,
      "max_tokens": 512
    },
    "prompt_type": "simple",
    "prompt_config": {
      "system": "You are a helpful assistant.",
      "prologue": "Hi! I'm your assistant. What can I do for you?",
      "parameters": [],
      "empty_response": "Sorry! No relevant content was found in the knowledge base!"
    },
    "meta_data_filter": {},
    "similarity_threshold": 0.1,
    "vector_similarity_weight": 0.3,
    "top_n": 6,
    "top_k": 1024,
    "do_refer": "1",
    "rerank_id": "",
    "kb_ids": ["kb_123"],
    "kb_names": ["Knowledge Base 1"],
    "status": "1",
    "create_time": 1700000000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-01 12:00:00"
  },
  "message": "success"
}
```

---


## 3. 获取对话列表 (List Dialogs)

获取当前用户的所有对话列表。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/dialog/list" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "dialog_xyz",
      "tenant_id": "user_abc",
      "name": "My Assistant",
      "description": "A helpful dialog",
      "icon": "",
      "language": "English",
      "llm_id": "chatgpt-3.5",
      "llm_setting": {
        "temperature": 0.1,
        "top_p": 0.3,
        "frequency_penalty": 0.7,
        "presence_penalty": 0.4,
        "max_tokens": 512
      },
      "prompt_type": "simple",
      "prompt_config": {
        "system": "You are a helpful assistant.",
        "prologue": "Hi! I'm your assistant. What can I do for you?",
        "parameters": [],
        "empty_response": "Sorry! No relevant content was found in the knowledge base!"
      },
      "meta_data_filter": {},
      "similarity_threshold": 0.1,
      "vector_similarity_weight": 0.3,
      "top_n": 6,
      "top_k": 1024,
      "do_refer": "1",
      "rerank_id": "",
      "kb_ids": ["kb_123"],
      "kb_names": ["Knowledge Base 1"],
      "status": "1",
      "create_time": 1700000000000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1700000000000,
      "update_date": "2024-01-01 12:00:00"
    }
  ],
  "message": "success"
}
```

---


## 4. 获取对话列表 (分页) (List Dialogs - Pagination)

支持分页、搜索和排序的对话列表查询。

- **URL**: `/next`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | int | 否 | 页码 (默认 0，表示不分页或第一页) |
| `page_size` | int | 否 | 每页数量 |
| `keywords` | string | 否 | 搜索关键词 (匹配名称) |
| `orderby` | string | 否 | 排序字段 (默认 "create_time") |
| `desc` | string/bool | 否 | 是否降序 (默认 true) |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `owner_ids` | list[string] | 否 | 租户 ID 列表 (用于管理员查询，普通用户忽略) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/dialog/next?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{}'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "dialogs": [
      {
        "id": "dialog_xyz",
        "tenant_id": "user_abc",
        "name": "My Assistant",
        "description": "A helpful dialog",
        "language": "English",
        "llm_id": "chatgpt-3.5",
        "llm_setting": {
          "temperature": 0.1,
          "top_p": 0.3,
          "frequency_penalty": 0.7,
          "presence_penalty": 0.4,
          "max_tokens": 512
        },
        "prompt_type": "simple",
        "prompt_config": {
          "system": "You are a helpful assistant.",
          "prologue": "Hi! I'm your assistant. What can I do for you?",
          "parameters": [],
          "empty_response": "Sorry! No relevant content was found in the knowledge base!"
        },
        "similarity_threshold": 0.1,
        "vector_similarity_weight": 0.3,
        "top_n": 6,
        "top_k": 1024,
        "do_refer": "1",
        "rerank_id": "",
        "kb_ids": ["kb_123"],
        "icon": "",
        "status": "1",
        "nickname": "John Doe",
        "tenant_avatar": "",
        "update_time": 1700000000000,
        "create_time": 1700000000000
      }
    ],
    "total": 1
  },
  "message": "success"
}
```

---


## 5. 删除对话 (Remove Dialog)

删除一个或多个对话。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_ids` | list[string] | 是 | 要删除的 Dialog ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/dialog/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "dialog_ids": ["dialog_xyz"]
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


# Document API 文档

**Base URL**: `http://localhost:9380/v1/document`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 上传文件 (Upload)

上传文件到指定知识库。

- **URL**: `/upload`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Form Data)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `file` | file | 是 | 要上传的文件 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/upload" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -F "kb_id=kb_123" \
     -F "file=@/path/to/file.pdf"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "550e8400e29b41d4a716446655440000",
      "kb_id": "kb_123",
      "parser_id": "naive",
      "pipeline_id": null,
      "parser_config": {
        "pages": [[1, 1000000]],
        "table_context_size": 0,
        "image_context_size": 0
      },
      "source_type": "local",
      "type": "doc",
      "created_by": "user_123",
      "name": "file.pdf",
      "location": "file.pdf",
      "size": 102400,
      "token_num": 0,
      "chunk_num": 0,
      "progress": 0,
      "progress_msg": "",
      "process_begin_at": null,
      "process_duration": 0,
      "meta_fields": {},
      "suffix": "pdf",
      "run": "0",
      "status": "1",
      "thumbnail": "thumbnail_550e8400e29b41d4a716446655440000.png",
      "create_time": 1706000000000,
      "update_time": 1706000000000
    }
  ],
  "message": "success"
}
```

---


## 2. 网页爬取 (Web Crawl)

爬取指定 URL 并保存为知识库中的文档。

- **URL**: `/web_crawl`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Form Data)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `name` | string | 是 | 文档名称 |
| `url` | string | 是 | 要爬取的 URL |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/web_crawl" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -F "kb_id=kb_123" \
     -F "name=example_page" \
     -F "url=https://example.com"
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


## 3. 创建虚拟文档 (Create)

在知识库中创建一个空文档（虚拟文档）。

- **URL**: `/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `name` | string | 是 | 文档名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "kb_123",
           "name": "virtual_doc.txt"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "550e8400e29b41d4a716446655440000",
    "kb_id": "kb_123",
    "parser_id": "naive",
    "pipeline_id": null,
    "parser_config": {
      "pages": [[1, 1000000]],
      "table_context_size": 0,
      "image_context_size": 0
    },
    "source_type": "local",
    "type": "virtual",
    "created_by": "user_123",
    "name": "virtual_doc.txt",
    "location": "",
    "size": 0,
    "token_num": 0,
    "chunk_num": 0,
    "progress": 0,
    "progress_msg": "",
    "process_begin_at": null,
    "process_duration": 0,
    "meta_fields": {},
    "suffix": "txt",
    "run": "0",
    "status": "1",
    "thumbnail": null,
    "create_time": 1706000000000,
    "update_time": 1706000000000
  },
  "message": "success"
}
```

---


## 4. 获取文档列表 (List Documents)

获取知识库中的文档列表，支持分页和筛选。

- **URL**: `/list`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 10) |
| `orderby` | string | 否 | 排序字段 (默认 create_time) |
| `desc` | string | 否 | 是否降序 ("true"/"false", 默认 "true") |
| `keywords` | string | 否 | 搜索关键词 |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `return_empty_metadata` | boolean | 否 | 是否返回空元数据 (默认 false) |
| `run_status` | list[string] | 否 | 运行状态筛选 |
| `types` | list[string] | 否 | 文件类型筛选 |
| `suffix` | list[string] | 否 | 后缀名筛选 |
| `metadata_condition` | object | 否 | 元数据筛选条件 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/list?kb_id=kb_123&page=1&page_size=20" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "types": ["pdf", "docx"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 100,
    "docs": [
      {
        "id": "550e8400e29b41d4a716446655440000",
        "kb_id": "kb_123",
        "parser_id": "naive",
        "pipeline_id": null,
        "pipeline_name": null,
        "parser_config": {
          "pages": [[1, 1000000]],
          "table_context_size": 0,
          "image_context_size": 0
        },
        "source_type": "local",
        "type": "doc",
        "created_by": "user_123",
        "nickname": "John",
        "name": "file.pdf",
        "location": "file.pdf",
        "size": 102400,
        "token_num": 5000,
        "chunk_num": 50,
        "progress": 1.0,
        "progress_msg": "Task completed",
        "process_begin_at": "2024-01-23 10:00:00",
        "process_duration": 30.5,
        "meta_fields": {
          "author": "admin",
          "category": "technical"
        },
        "suffix": "pdf",
        "run": "3",
        "status": "1",
        "thumbnail": "/v1/document/image/kb_123-thumbnail_xxx.png",
        "create_time": 1706000000000,
        "update_time": 1706000000000
      }
    ]
  },
  "message": "success"
}
```

---


## 5. 获取筛选信息 (Filter)

获取知识库文档的筛选统计信息。

- **URL**: `/filter`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/filter" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "kb_1"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 50,
    "filter": {
      "suffix": {
        "pdf": 25,
        "docx": 15,
        "txt": 10
      },
      "run_status": {
        "0": 5,
        "1": 10,
        "2": 3,
        "3": 30,
        "4": 2
      },
      "metadata": {
        "author": {
          "admin": 20,
          "user1": 15
        },
        "category": {
          "technical": 30,
          "business": 20
        },
        "empty_metadata": {
          "true": 5
        }
      }
    }
  },
  "message": "success"
}
```

---


## 6. 获取文档详情 (Infos)

批量获取文档详细信息。

- **URL**: `/infos`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_ids` | list[string] | 是 | 文档 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/infos" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "doc_ids": ["doc_1", "doc_2"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "550e8400e29b41d4a716446655440000",
      "kb_id": "kb_123",
      "parser_id": "naive",
      "pipeline_id": null,
      "parser_config": {
        "pages": [[1, 1000000]],
        "table_context_size": 0,
        "image_context_size": 0
      },
      "source_type": "local",
      "type": "doc",
      "created_by": "user_123",
      "name": "doc1.pdf",
      "location": "doc1.pdf",
      "size": 102400,
      "token_num": 5000,
      "chunk_num": 50,
      "progress": 1.0,
      "progress_msg": "Task completed",
      "process_begin_at": "2024-01-23 10:00:00",
      "process_duration": 30.5,
      "meta_fields": {
        "author": "admin"
      },
      "suffix": "pdf",
      "run": "3",
      "status": "1",
      "thumbnail": "thumbnail_xxx.png",
      "create_time": 1706000000000,
      "update_time": 1706000000000
    }
  ],
  "message": "success"
}
```

---


## 7. 元数据摘要 (Metadata Summary)

获取知识库的元数据摘要。

- **URL**: `/metadata/summary`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/metadata/summary" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "kb_1"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "summary": {
      "author": [
        ["admin", 25],
        ["user1", 15],
        ["user2", 10]
      ],
      "category": [
        ["technical", 30],
        ["business", 20]
      ],
      "tags": [
        ["important", 18],
        ["archived", 12]
      ]
    }
  },
  "message": "success"
}
```

---


## 8. 批量更新元数据 (Metadata Update)

批量更新或删除文档的元数据。

- **URL**: `/metadata/update`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `selector` | object | 否 | 选择器 (包含 document_ids 或 metadata_condition) |
| `updates` | list[object] | 否 | 更新内容 (key, value) |
| `deletes` | list[object] | 否 | 删除内容 (key) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/metadata/update" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "kb_1",
           "selector": {"document_ids": ["doc_1"]},
           "updates": [{"key": "author", "value": "admin"}]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "updated": 5,
    "matched_docs": 10
  },
  "message": "success"
}
```

---


## 9. 更新元数据配置 (Update Metadata Setting)

更新文档的元数据解析配置。

- **URL**: `/update_metadata_setting`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_id` | string | 是 | 文档 ID |
| `metadata` | object | 是 | 元数据配置 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/update_metadata_setting" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "doc_id": "doc_1",
           "metadata": {"title": "My Doc"}
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "550e8400e29b41d4a716446655440000",
    "kb_id": "kb_123",
    "parser_id": "naive",
    "pipeline_id": null,
    "parser_config": {
      "pages": [[1, 1000000]],
      "table_context_size": 0,
      "image_context_size": 0,
      "metadata": {
        "title": "My Doc"
      }
    },
    "source_type": "local",
    "type": "doc",
    "created_by": "user_123",
    "name": "doc1.pdf",
    "location": "doc1.pdf",
    "size": 102400,
    "token_num": 5000,
    "chunk_num": 50,
    "progress": 1.0,
    "progress_msg": "Task completed",
    "process_begin_at": "2024-01-23 10:00:00",
    "process_duration": 30.5,
    "meta_fields": {},
    "suffix": "pdf",
    "run": "3",
    "status": "1",
    "thumbnail": "thumbnail_xxx.png",
    "create_time": 1706000000000,
    "update_time": 1706000000000
  },
  "message": "success"
}
```

---


## 10. 获取缩略图 (Thumbnails)

批量获取文档的缩略图。

- **URL**: `/thumbnails`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_ids` | list[string] | 是 | 文档 ID 列表 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/document/thumbnails?doc_ids=doc_1&doc_ids=doc_2" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "550e8400e29b41d4a716446655440000": "/v1/document/image/kb_123-thumbnail_550e8400e29b41d4a716446655440000.png",
    "550e8400e29b41d4a716446655440001": "/v1/document/image/kb_123-thumbnail_550e8400e29b41d4a716446655440001.png"
  },
  "message": "success"
}
```

---


## 11. 更改文档状态 (Change Status)

启用或禁用文档。

- **URL**: `/change_status`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_ids` | list[string] | 是 | 文档 ID 列表 |
| `status` | string | 是 | 状态 ("0": 禁用, "1": 启用) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/change_status" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "doc_ids": ["doc_1", "doc_2"],
           "status": "1"
         }'
```

### 响应示例

成功响应：
```json
{
  "code": 0,
  "data": {
    "550e8400e29b41d4a716446655440000": {"status": "1"},
    "550e8400e29b41d4a716446655440001": {"status": "1"}
  },
  "message": "success"
}
```

部分失败响应：
```json
{
  "code": 0,
  "data": {
    "550e8400e29b41d4a716446655440000": {"status": "1"},
    "550e8400e29b41d4a716446655440001": {"error": "No authorization."},
    "550e8400e29b41d4a716446655440002": {"error": "Can't find this dataset!"}
  },
  "message": "success"
}
```

---


## 12. 删除文档 (Remove)

删除指定文档。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_id` | string/list | 是 | 文档 ID 或 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "doc_id": ["doc_1"]
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


## 13. 运行解析 (Run)

重新解析文档。

- **URL**: `/run`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_ids` | list[string] | 是 | 文档 ID 列表 |
| `run` | string | 是 | 运行状态 (如 "1" 表示运行) |
| `delete` | boolean | 否 | 是否删除已有分块 (默认 true) |
| `apply_kb` | boolean | 否 | 是否应用知识库配置 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/run" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "doc_ids": ["doc_1"],
           "run": "1"
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


## 14. 重命名文档 (Rename)

修改文档名称。

- **URL**: `/rename`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_id` | string | 是 | 文档 ID |
| `name` | string | 是 | 新名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/rename" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "doc_id": "doc_1",
           "name": "new_name.pdf"
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


## 15. 获取文档内容 (Get Document)

下载或获取文档原始内容。

- **URL**: `/get/<doc_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_id` | string | 是 | 文档 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/document/get/doc_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
(二进制文件流)

---


## 16. 下载附件 (Download Attachment)

下载文档相关的附件。

- **URL**: `/download/<attachment_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `attachment_id` | string | 是 | 附件 ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ext` | string | 否 | 文件扩展名 (默认 markdown) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/document/download/attach_123?ext=pdf" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
(二进制文件流)

---


## 17. 修改解析器 (Change Parser)

修改文档使用的解析器配置。

- **URL**: `/change_parser`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_id` | string | 是 | 文档 ID |
| `parser_id` | string | 否 | 解析器 ID (如 "pdf", "general") |
| `parser_config` | object | 否 | 解析器配置 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/change_parser" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "doc_id": "doc_1",
           "parser_id": "general",
           "parser_config": {"chunk_token_num": 128}
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


## 18. 获取图片 (Get Image)

获取文档中的图片。

- **URL**: `/image/<image_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `image_id` | string | 是 | 图片 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/document/image/bucket-name" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
(图片二进制流)

---


## 19. 上传并解析 (Upload and Parse)

上传文件并直接开始解析（通常用于对话中的文件上传）。

- **URL**: `/upload_and_parse`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Form Data)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_id` | string | 是 | 对话 ID |
| `file` | file | 是 | 文件 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/upload_and_parse" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -F "conversation_id=conv_1" \
     -F "file=@/path/to/doc.pdf"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    "550e8400e29b41d4a716446655440000",
    "550e8400e29b41d4a716446655440001"
  ],
  "message": "success"
}
```

---


## 20. 解析内容 (Parse)

解析 URL 或上传的文件内容。

- **URL**: `/parse`
- **Method**: `POST`
- **Content-Type**: `application/json` 或 `multipart/form-data`

### 请求参数 (JSON - 方式 1)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `url` | string | 是 | 要解析的 URL |

### 请求参数 (Form Data - 方式 2)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file` | file | 是 | 要解析的文件 |

### 请求示例 (URL)
```bash
curl -X POST "http://localhost:9380/v1/document/parse" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "url": "https://example.com"
         }'
```

### 请求示例 (File)
```bash
curl -X POST "http://localhost:9380/v1/document/parse" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -F "file=@/path/to/file.txt"
```

### 响应示例
```json
{
  "code": 0,
  "data": "\n -----------------\nFile: document.pdf\nContent as following: \nThis is the first paragraph of the document.\n\nThis is the second paragraph with important information about the topic.\n\nConclusion and summary of the document content.",
  "message": "success"
}
```

---


## 21. 设置元数据 (Set Meta)

设置文档的额外元数据信息。

- **URL**: `/set_meta`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `doc_id` | string | 是 | 文档 ID |
| `meta` | string | 是 | JSON 格式的元数据字符串 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/set_meta" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "doc_id": "doc_1",
           "meta": "{\"key\": \"value\"}"
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


## 22. 上传信息 (Upload Info)

上传文件或 URL 并提取信息。

- **URL**: `/upload_info`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Form Data / Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file` | file | 否 | 上传的文件 |
| `url` | string | 否 | URL (通过 Query 参数传递) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/document/upload_info?url=https://example.com" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "550e8400e29b41d4a716446655440000",
    "name": "example.pdf",
    "size": 102400,
    "extension": "pdf",
    "mime_type": "application/pdf",
    "created_by": "user_123",
    "created_at": 1706000000.123,
    "preview_url": null
  },
  "message": "success"
}
```

---


# Evaluation API 文档

**Base URL**: `http://localhost:9380/v1/evaluation`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建数据集 (Create Dataset)

创建一个新的评估数据集。

- **URL**: `/dataset/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 数据集名称 |
| `kb_ids` | list[string] | 是 | 关联的知识库 ID 列表 |
| `description` | string | 否 | 数据集描述 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/dataset/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Evaluation Dataset",
           "description": "Dataset for testing RAG performance",
           "kb_ids": ["kb_1", "kb_2"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f"
  }
}
```

---


## 2. 获取数据集列表 (List Datasets)

获取当前租户下的评估数据集列表。

- **URL**: `/dataset/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | int | 否 | 页码 (默认: 1) |
| `page_size` | int | 否 | 每页数量 (默认: 20) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/dataset/list?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "datasets": [
      {
        "id": "5a6b7c8d9e0f1a2b3c4d5e6f",
        "tenant_id": "tenant_abc123",
        "name": "My Evaluation Dataset",
        "description": "Dataset for testing RAG performance",
        "kb_ids": ["kb_1", "kb_2"],
        "created_by": "user_xyz789",
        "create_time": 1704067200000,
        "update_time": 1704067200000,
        "status": 1
      }
    ],
    "total": 1
  }
}
```

---


## 3. 获取数据集详情 (Get Dataset)

根据 ID 获取数据集详情。

- **URL**: `/dataset/<dataset_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/dataset/dataset_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "5a6b7c8d9e0f1a2b3c4d5e6f",
    "tenant_id": "tenant_abc123",
    "name": "My Evaluation Dataset",
    "description": "Dataset for testing RAG performance",
    "kb_ids": ["kb_1", "kb_2"],
    "created_by": "user_xyz789",
    "create_time": 1704067200000,
    "update_time": 1704067200000,
    "status": 1
  }
}
```

---


## 4. 更新数据集 (Update Dataset)

更新数据集信息。

- **URL**: `/dataset/<dataset_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 新的数据集名称 |
| `description` | string | 否 | 新的数据集描述 |
| `kb_ids` | list[string] | 否 | 新的关联知识库 ID 列表 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/evaluation/dataset/dataset_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "Updated Name"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f"
  }
}
```

---


## 5. 删除数据集 (Delete Dataset)

删除指定的数据集 (软删除)。

- **URL**: `/dataset/<dataset_id>`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/evaluation/dataset/dataset_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f"
  }
}
```

---


## 6. 添加测试用例 (Add Test Case)

向数据集添加单个测试用例。

- **URL**: `/dataset/<dataset_id>/case/add`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 测试问题 |
| `reference_answer` | string | 否 | 参考答案 (Ground Truth) |
| `relevant_doc_ids` | list[string] | 否 | 相关文档 ID 列表 |
| `relevant_chunk_ids` | list[string] | 否 | 相关切片 ID 列表 |
| `metadata` | object | 否 | 元数据 (Key-Value) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/dataset/dataset_123/case/add" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAGFlow?",
           "reference_answer": "RAGFlow is an open-source RAG engine."
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "case_id": "a1b2c3d4e5f6a7b8c9d0e1f2"
  }
}
```

---


## 7. 批量导入测试用例 (Import Test Cases)

批量导入测试用例到数据集。

- **URL**: `/dataset/<dataset_id>/case/import`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `cases` | list[object] | 是 | 测试用例列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/dataset/dataset_123/case/import" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "cases": [
             {
               "question": "Question 1",
               "reference_answer": "Answer 1"
             },
             {
               "question": "Question 2",
               "reference_answer": "Answer 2"
             }
           ]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "success_count": 2,
    "failure_count": 0,
    "total": 2
  }
}
```

---


## 8. 获取测试用例列表 (Get Test Cases)

获取指定数据集下的所有测试用例。

- **URL**: `/dataset/<dataset_id>/cases`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/dataset/dataset_123/cases" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "cases": [
      {
        "id": "a1b2c3d4e5f6a7b8c9d0e1f2",
        "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f",
        "question": "What is RAGFlow?",
        "reference_answer": "RAGFlow is an open-source RAG engine.",
        "relevant_doc_ids": ["doc_001", "doc_002"],
        "relevant_chunk_ids": ["chunk_001", "chunk_002"],
        "metadata": {"category": "general"},
        "create_time": 1704067200000
      }
    ],
    "total": 1
  }
}
```

---


## 9. 删除测试用例 (Delete Test Case)

删除指定的测试用例。

- **URL**: `/case/<case_id>`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `case_id` | string | 是 | 测试用例 ID |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/evaluation/case/case_456" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "case_id": "a1b2c3d4e5f6a7b8c9d0e1f2"
  }
}
```

---


## 10. 开始评估 (Start Evaluation)

启动一次评估任务。

- **URL**: `/run/start`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `dialog_id` | string | 是 | 对话 (Agent) ID |
| `name` | string | 否 | 评估任务名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/run/start" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "dataset_id": "dataset_123",
           "dialog_id": "dialog_789",
           "name": "Run 1"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "run_id": "run1a2b3c4d5e6f7a8b9c0d1e"
  }
}
```

---


## 11. 获取评估任务详情 (Get Evaluation Run)

获取评估任务的基本信息。

- **URL**: `/run/<run_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_id` | string | 是 | 评估任务 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/run/run_001" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "run": {
      "id": "run1a2b3c4d5e6f7a8b9c0d1e",
      "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f",
      "dialog_id": "dialog_abc123",
      "name": "Evaluation Run 2024-01-01 12:00:00",
      "config_snapshot": {},
      "metrics_summary": {
        "total_cases": 10,
        "avg_execution_time": 2.5,
        "avg_precision": 0.85,
        "avg_recall": 0.78,
        "avg_f1_score": 0.81
      },
      "status": "COMPLETED",
      "created_by": "user_xyz789",
      "create_time": 1704067200000,
      "complete_time": 1704070800000
    },
    "results": []
  }
}
```

---


## 12. 获取评估结果详情 (Get Run Results)

获取评估任务的详细结果。

- **URL**: `/run/<run_id>/results`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_id` | string | 是 | 评估任务 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/run/run_001/results" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "run": {
      "id": "run1a2b3c4d5e6f7a8b9c0d1e",
      "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f",
      "dialog_id": "dialog_abc123",
      "name": "Evaluation Run 2024-01-01 12:00:00",
      "config_snapshot": {},
      "metrics_summary": {
        "total_cases": 10,
        "avg_execution_time": 2.5,
        "avg_precision": 0.85,
        "avg_recall": 0.78,
        "avg_f1_score": 0.81
      },
      "status": "COMPLETED",
      "created_by": "user_xyz789",
      "create_time": 1704067200000,
      "complete_time": 1704070800000
    },
    "results": [
      {
        "id": "result_abc123",
        "run_id": "run1a2b3c4d5e6f7a8b9c0d1e",
        "case_id": "a1b2c3d4e5f6a7b8c9d0e1f2",
        "generated_answer": "RAGFlow is an open-source RAG engine based on deep document understanding.",
        "retrieved_chunks": [
          {
            "chunk_id": "chunk_001",
            "content": "RAGFlow is an open-source RAG engine...",
            "similarity": 0.95
          }
        ],
        "metrics": {
          "precision": 0.9,
          "recall": 0.85,
          "f1_score": 0.87,
          "hit_rate": 1.0,
          "mrr": 1.0,
          "answer_length": 78,
          "has_answer": 1.0
        },
        "execution_time": 2.35,
        "token_usage": null,
        "create_time": 1704067250000
      }
    ]
  }
}
```

---


## 13. 获取评估任务列表 (List Evaluation Runs)

列出评估任务。

- **URL**: `/run/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 否 | 按数据集筛选 |
| `dialog_id` | string | 否 | 按对话筛选 |
| `page` | int | 否 | 页码 (默认: 1) |
| `page_size` | int | 否 | 每页数量 (默认: 20) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/run/list?page=1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "runs": [],
    "total": 0
  }
}
```

> **注意**: 此接口尚未完全实现。

---


## 14. 删除评估任务 (Delete Evaluation Run)

删除评估任务。

- **URL**: `/run/<run_id>`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_id` | string | 是 | 评估任务 ID |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/evaluation/run/run_001" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "run_id": "run1a2b3c4d5e6f7a8b9c0d1e"
  }
}
```

> **注意**: 此接口尚未完全实现。

---


## 15. 获取优化建议 (Get Recommendations)

根据评估结果获取配置优化建议。

- **URL**: `/run/<run_id>/recommendations`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_id` | string | 是 | 评估任务 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/run/run_001/recommendations" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "recommendations": [
      {
        "issue": "Low Precision",
        "severity": "high",
        "description": "System is retrieving many irrelevant chunks",
        "suggestions": [
          "Increase similarity_threshold to filter out less relevant chunks",
          "Enable reranking to improve chunk ordering",
          "Reduce top_k to return fewer chunks"
        ]
      },
      {
        "issue": "Slow Response Time",
        "severity": "medium",
        "description": "Average response time is 5.50s",
        "suggestions": [
          "Reduce top_k to retrieve fewer chunks",
          "Optimize embedding model selection",
          "Consider caching frequently asked questions"
        ]
      }
    ]
  }
}
```

---


## 16. 对比评估任务 (Compare Runs)

对比多个评估任务的结果。

- **URL**: `/compare`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_ids` | list[string] | 是 | 待对比的评估任务 ID 列表 (至少2个) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/compare" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "run_ids": ["run_001", "run_002"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "comparison": {}
  }
}
```

> **注意**: 此接口尚未完全实现。

---


## 17. 导出结果 (Export Results)

导出评估结果 (JSON/CSV)。

- **URL**: `/run/<run_id>/export`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_id` | string | 是 | 评估任务 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/run/run_001/export" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "run": {
      "id": "run1a2b3c4d5e6f7a8b9c0d1e",
      "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f",
      "dialog_id": "dialog_abc123",
      "name": "Evaluation Run 2024-01-01 12:00:00",
      "config_snapshot": {},
      "metrics_summary": {
        "total_cases": 10,
        "avg_execution_time": 2.5,
        "avg_precision": 0.85,
        "avg_recall": 0.78,
        "avg_f1_score": 0.81
      },
      "status": "COMPLETED",
      "created_by": "user_xyz789",
      "create_time": 1704067200000,
      "complete_time": 1704070800000
    },
    "results": [
      {
        "id": "result_abc123",
        "run_id": "run1a2b3c4d5e6f7a8b9c0d1e",
        "case_id": "a1b2c3d4e5f6a7b8c9d0e1f2",
        "generated_answer": "RAGFlow is an open-source RAG engine.",
        "retrieved_chunks": [],
        "metrics": {
          "answer_length": 40,
          "has_answer": 1.0
        },
        "execution_time": 2.35,
        "token_usage": null,
        "create_time": 1704067250000
      }
    ]
  }
}
```

---


## 18. 单次评估 (Evaluate Single)

实时评估单个问答对。

- **URL**: `/evaluate_single`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 测试问题 |
| `dialog_id` | string | 是 | 对话 (Agent) ID |
| `reference_answer` | string | 否 | 参考答案 |
| `relevant_chunk_ids` | list[string] | 否 | 指定切片 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/evaluate_single" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "test question",
           "dialog_id": "dialog_789"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "answer": "",
    "metrics": {},
    "retrieved_chunks": []
  }
}
```

> **注意**: 此接口尚未完全实现，返回值为占位符。


---


# File2Document API 文档

**Base URL**: `http://localhost:9380/v1/file2document`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 文件转文档 (Convert File to Document)

将上传的文件转换为知识库文档。此接口会先删除指定文件已有的文档关联，然后重新创建文档并加入到指定的知识库中。

- **URL**: `/convert`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_ids` | list[string] | 是 | 文件 ID 列表 |
| `kb_ids` | list[string] | 是 | 目标知识库 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file2document/convert" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_ids": ["file_123", "file_456"],
           "kb_ids": ["kb_1"]
         }'
```

### 成功响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "a1b2c3d4e5f6789012345678",
      "file_id": "f1a2b3c4d5e6f7890123456789abcdef",
      "document_id": "d1a2b3c4d5e6f7890123456789abcdef",
      "create_time": 1738636800000,
      "create_date": "2025-02-04 12:00:00",
      "update_time": 1738636800000,
      "update_date": "2025-02-04 12:00:00"
    }
  ]
}
```

### 错误响应示例
```json
{
  "code": 102,
  "message": "File not found!"
}
```

```json
{
  "code": 102,
  "message": "Can't find this dataset!"
}
```

```json
{
  "code": 102,
  "message": "Document not found!"
}
```

---


## 2. 删除关联 (Remove File-Document Link)

删除文件与文档的关联，并清理相关文档数据。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_ids` | list[string] | 是 | 文件 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file2document/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_ids": ["f1a2b3c4d5e6f7890123456789abcdef"]
         }'
```

### 成功响应示例
```json
{
  "code": 0,
  "data": true
}
```

### 错误响应示例
```json
{
  "code": 100,
  "data": false,
  "message": "Lack of \"Files ID\""
}
```

```json
{
  "code": 102,
  "message": "Inform not found!"
}
```

```json
{
  "code": 102,
  "message": "Document not found!"
}
```

```json
{
  "code": 102,
  "message": "Tenant not found!"
}
```

```json
{
  "code": 102,
  "message": "Database error (Document removal)!"
}
```

---


# File Management API 文档

**Base URL**: `http://localhost:9380/v1/file`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 上传文件 (Upload)

上传一个或多个文件到指定文件夹。

- **URL**: `/upload`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Form Data)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file` | file | 是 | 要上传的文件 (支持多文件上传) |
| `parent_id` | string | 否 | 父文件夹 ID (默认上传到根目录) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file/upload" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -F "file=@/path/to/file1.txt" \
     -F "file=@/path/to/file2.pdf" \
     -F "parent_id=folder_123"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "file_uuid_1",
      "parent_id": "folder_123",
      "tenant_id": "tenant_1",
      "created_by": "user_1",
      "name": "file2.pdf",
      "location": "file2.pdf",
      "size": 1024,
      "type": "pdf",
      "source_type": "",
      "create_time": 1738656000000,
      "create_date": "2025-02-04 12:00:00",
      "update_time": 1738656000000,
      "update_date": "2025-02-04 12:00:00"
    }
  ],
  "message": "success"
}
```

---


## 2. 创建文件夹 (Create Folder)

创建一个新的文件夹或虚拟文件。

- **URL**: `/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 文件夹名称 |
| `parent_id` | string | 否 | 父文件夹 ID (默认根目录) |
| `type` | string | 否 | 类型 ("folder" 或 "virtual") |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "New Folder",
           "parent_id": "root_folder_id",
           "type": "folder"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "folder_uuid",
    "parent_id": "root_folder_id",
    "tenant_id": "tenant_1",
    "created_by": "user_1",
    "name": "New Folder",
    "location": "",
    "size": 0,
    "type": "folder",
    "source_type": "",
    "create_time": 1738656000000,
    "create_date": "2025-02-04 12:00:00",
    "update_time": 1738656000000,
    "update_date": "2025-02-04 12:00:00"
  },
  "message": "success"
}
```

---


## 3. 获取文件列表 (List Files)

分页获取指定文件夹下的文件列表。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `parent_id` | string | 否 | 父文件夹 ID (默认根目录) |
| `keywords` | string | 否 | 搜索关键字 |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 15) |
| `orderby` | string | 否 | 排序字段 (默认 "create_time") |
| `desc` | boolean | 否 | 是否倒序 (默认 True) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/file/list?parent_id=folder_123&page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 5,
    "files": [
      {
        "id": "file_1",
        "parent_id": "folder_123",
        "tenant_id": "tenant_1",
        "created_by": "user_1",
        "name": "document.pdf",
        "location": "document.pdf",
        "size": 2048,
        "type": "pdf",
        "source_type": "",
        "create_time": 1738656000000,
        "create_date": "2025-02-04 12:00:00",
        "update_time": 1738656000000,
        "update_date": "2025-02-04 12:00:00",
        "kbs_info": [
          {
            "kb_id": "kb_1",
            "kb_name": "My Knowledge Base",
            "document_id": "doc_1"
          }
        ]
      },
      {
        "id": "folder_456",
        "parent_id": "folder_123",
        "tenant_id": "tenant_1",
        "created_by": "user_1",
        "name": "Sub Folder",
        "location": "",
        "size": 4096,
        "type": "folder",
        "source_type": "",
        "create_time": 1738656000000,
        "create_date": "2025-02-04 12:00:00",
        "update_time": 1738656000000,
        "update_date": "2025-02-04 12:00:00",
        "kbs_info": [],
        "has_child_folder": true
      }
    ],
    "parent_folder": {
      "id": "folder_123",
      "parent_id": "root_id",
      "tenant_id": "tenant_1",
      "created_by": "user_1",
      "name": "Parent Name",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1738656000000,
      "create_date": "2025-02-04 12:00:00",
      "update_time": 1738656000000,
      "update_date": "2025-02-04 12:00:00"
    }
  },
  "message": "success"
}
```

**说明**:
- 对于文件类型，`kbs_info` 返回关联的知识库信息列表
- 对于文件夹类型，`kbs_info` 为空数组，`has_child_folder` 表示是否包含子文件夹，`size` 为文件夹内所有文件的总大小

---


## 4. 获取根文件夹 (Root Folder)

获取当前用户的根文件夹信息。

- **URL**: `/root_folder`
- **Method**: `GET`

### 请求参数
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/file/root_folder" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "root_folder": {
      "id": "root_id",
      "parent_id": "root_id",
      "tenant_id": "tenant_1",
      "created_by": "tenant_1",
      "name": "/",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1738656000000,
      "create_date": "2025-02-04 12:00:00",
      "update_time": 1738656000000,
      "update_date": "2025-02-04 12:00:00"
    }
  },
  "message": "success"
}
```

---


## 5. 获取父文件夹 (Parent Folder)

获取指定文件的直接父文件夹信息。

- **URL**: `/parent_folder`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 当前文件或文件夹 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/file/parent_folder?file_id=file_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "parent_folder": {
      "id": "folder_123",
      "parent_id": "root_id",
      "tenant_id": "tenant_1",
      "created_by": "user_1",
      "name": "My Folder",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1738656000000,
      "create_date": "2025-02-04 12:00:00",
      "update_time": 1738656000000,
      "update_date": "2025-02-04 12:00:00"
    }
  },
  "message": "success"
}
```

---


## 6. 获取所有父文件夹路径 (All Parent Folders)

获取指定文件的所有上级目录（路径）。

- **URL**: `/all_parent_folder`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 当前文件或文件夹 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/file/all_parent_folder?file_id=file_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "parent_folders": [
      {
        "id": "file_123",
        "parent_id": "folder_1",
        "tenant_id": "tenant_1",
        "created_by": "user_1",
        "name": "document.pdf",
        "location": "document.pdf",
        "size": 2048,
        "type": "pdf",
        "source_type": "",
        "create_time": 1738656000000,
        "create_date": "2025-02-04 12:00:00",
        "update_time": 1738656000000,
        "update_date": "2025-02-04 12:00:00"
      },
      {
        "id": "folder_1",
        "parent_id": "root_id",
        "tenant_id": "tenant_1",
        "created_by": "user_1",
        "name": "Docs",
        "location": "",
        "size": 0,
        "type": "folder",
        "source_type": "",
        "create_time": 1738656000000,
        "create_date": "2025-02-04 12:00:00",
        "update_time": 1738656000000,
        "update_date": "2025-02-04 12:00:00"
      },
      {
        "id": "root_id",
        "parent_id": "root_id",
        "tenant_id": "tenant_1",
        "created_by": "tenant_1",
        "name": "/",
        "location": "",
        "size": 0,
        "type": "folder",
        "source_type": "",
        "create_time": 1738656000000,
        "create_date": "2025-02-04 12:00:00",
        "update_time": 1738656000000,
        "update_date": "2025-02-04 12:00:00"
      }
    ]
  },
  "message": "success"
}
```

---


## 7. 删除文件/文件夹 (Remove)

删除指定的文件或文件夹。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_ids` | list[string] | 是 | 要删除的文件/文件夹 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_ids": ["file_1", "folder_2"]
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


## 8. 重命名 (Rename)

重命名文件或文件夹。

- **URL**: `/rename`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 文件 ID |
| `name` | string | 是 | 新名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file/rename" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_id": "file_123",
           "name": "new_name.pdf"
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


## 9. 下载/获取文件内容 (Get Content)

获取文件内容或下载文件。

- **URL**: `/get/<file_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 文件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/file/get/file_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
(二进制文件流)

---


## 10. 移动文件 (Move)

移动文件或文件夹到另一个文件夹。

- **URL**: `/mv`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `src_file_ids` | list[string] | 是 | 源文件 ID 列表 |
| `dest_file_id` | string | 是 | 目标文件夹 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file/mv" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "src_file_ids": ["file_1", "file_2"],
           "dest_file_id": "folder_destination"
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


# Knowledge Base API 文档

**Base URL**: `http://localhost:9380/v1/kb`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建知识库 (Create Knowledge Base)

创建一个新的知识库 (Knowledge Base)。

- **URL**: `/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 知识库名称 |
| `parser_id` | string | 否 | 解析器 ID (默认 naive) |
| `description` | string | 否 | 知识库描述 |
| `avatar` | string | 否 | 头像 (Base64) |
| `language` | string | 否 | 语言 (如 "English", "Chinese") |
| `permission` | string | 否 | 权限 ("me" 或 "team") |
| `embd_id` | string | 否 | Embedding 模型 ID |
| `pagerank` | int | 否 | PageRank 设置 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Knowledge Base",
           "language": "English",
           "permission": "me"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "kb_id": "a1b2c3d4e5f6789012345678"
  }
}
```

---


## 2. 更新知识库 (Update Knowledge Base)

更新知识库的基本信息和配置。

- **URL**: `/update`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `name` | string | 是 | 知识库名称 |
| `description` | string | 是 | 知识库描述 |
| `parser_id` | string | 是 | 解析器 ID |
| `avatar` | string | 否 | 头像 (Base64) |
| `language` | string | 否 | 语言 |
| `permission` | string | 否 | 权限 |
| `embd_id` | string | 否 | Embedding 模型 ID |
| `pagerank` | int | 否 | PageRank 设置 |
| `connectors` | list | 否 | 链接器设置 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/update" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "a1b2c3d4e5f6789012345678",
           "name": "Updated Name",
           "description": "Updated Description",
           "parser_id": "naive",
           "language": "English"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "name": "Updated Name",
    "description": "Updated Description",
    "avatar": null,
    "tenant_id": "user123456",
    "language": "English",
    "permission": "me",
    "embd_id": "BAAI/bge-large-zh-v1.5",
    "parser_id": "naive",
    "parser_config": {
      "pages": [[1, 1000000]],
      "table_context_size": 0,
      "image_context_size": 0
    },
    "pagerank": 0,
    "doc_num": 10,
    "token_num": 5000,
    "chunk_num": 100,
    "similarity_threshold": 0.2,
    "vector_similarity_weight": 0.3,
    "connectors": [],
    "create_time": 1700000000,
    "update_time": 1700001000
  }
}
```

---


## 3. 更新元数据设置 (Update Metadata Setting)

- **URL**: `/update_metadata_setting`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `metadata` | object | 是 | 元数据配置 (JSON 对象) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/update_metadata_setting" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "a1b2c3d4e5f6789012345678",
           "metadata": {
             "field1": "value1"
           }
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "name": "My KB",
    "description": "KB description",
    "avatar": null,
    "tenant_id": "user123456",
    "language": "English",
    "permission": "me",
    "embd_id": "BAAI/bge-large-zh-v1.5",
    "parser_id": "naive",
    "parser_config": {
      "pages": [[1, 1000000]],
      "table_context_size": 0,
      "image_context_size": 0,
      "metadata": {
        "field1": "value1"
      }
    },
    "pagerank": 0,
    "doc_num": 10,
    "token_num": 5000,
    "chunk_num": 100,
    "create_time": 1700000000,
    "update_time": 1700001000
  }
}
```

---


## 4. 获取知识库详情 (Get Knowledge Base Detail)

- **URL**: `/detail`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/detail?kb_id=a1b2c3d4e5f6789012345678" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "name": "My KB",
    "description": "KB description",
    "avatar": null,
    "language": "English",
    "permission": "me",
    "embd_id": "BAAI/bge-large-zh-v1.5",
    "parser_id": "naive",
    "pipeline_id": null,
    "pipeline_name": null,
    "pipeline_avatar": null,
    "parser_config": {
      "pages": [[1, 1000000]],
      "table_context_size": 0,
      "image_context_size": 0
    },
    "pagerank": 0,
    "doc_num": 10,
    "token_num": 5000,
    "chunk_num": 100,
    "size": 1048576,
    "graphrag_task_id": null,
    "graphrag_task_finish_at": null,
    "raptor_task_id": null,
    "raptor_task_finish_at": null,
    "mindmap_task_id": null,
    "mindmap_task_finish_at": null,
    "connectors": [],
    "create_time": 1700000000,
    "update_time": 1700001000
  }
}
```

---


## 5. 获取知识库列表 (List Knowledge Bases)

- **URL**: `/list`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 10) |
| `keywords` | string | 否 | 搜索关键词 |
| `orderby` | string | 否 | 排序字段 (默认 create_time) |
| `desc` | string/bool | 否 | 是否倒序 (默认 true) |
| `parser_id` | string | 否 | 解析器 ID 筛选 |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `owner_ids` | list[string] | 否 | 所有者 ID 列表 (仅管理员/内部使用) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/list?page=1&page_size=20" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{}'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 5,
    "kbs": [
      {
        "id": "a1b2c3d4e5f6789012345678",
        "name": "My KB",
        "description": "KB description",
        "avatar": null,
        "tenant_id": "user123456",
        "language": "English",
        "permission": "me",
        "embd_id": "BAAI/bge-large-zh-v1.5",
        "parser_id": "naive",
        "doc_num": 10,
        "token_num": 5000,
        "chunk_num": 100,
        "nickname": "John Doe",
        "tenant_avatar": null,
        "update_time": 1700001000
      }
    ]
  }
}
```

---


## 6. 删除知识库 (Remove Knowledge Base)

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "a1b2c3d4e5f6789012345678"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 7. 获取标签 (List Tags)

- **URL**: `/{kb_id}/tags`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/a1b2c3d4e5f6789012345678/tags" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": ["技术文档", "产品手册", "FAQ"]
}
```

---


## 8. 批量获取标签 (List Tags from KBs)

- **URL**: `/tags`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_ids` | string | 否 | 知识库 ID 列表 (逗号分隔) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/tags?kb_ids=kb_1,kb_2" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": ["技术文档", "产品手册", "FAQ", "用户指南"]
}
```

---


## 9. 删除标签 (Remove Tags)

- **URL**: `/{kb_id}/rm_tags`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tags` | list[string] | 是 | 要删除的标签列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/a1b2c3d4e5f6789012345678/rm_tags" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "tags": ["技术文档"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 10. 重命名标签 (Rename Tag)

- **URL**: `/{kb_id}/rename_tag`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `from_tag` | string | 是 | 原标签名 |
| `to_tag` | string | 是 | 新标签名 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/a1b2c3d4e5f6789012345678/rename_tag" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "from_tag": "技术文档",
           "to_tag": "技术资料"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 11. 获取知识图谱 (Get Knowledge Graph)

- **URL**: `/{kb_id}/knowledge_graph`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/a1b2c3d4e5f6789012345678/knowledge_graph" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "graph": {
      "nodes": [
        {
          "id": "node_1",
          "label": "Entity A",
          "pagerank": 0.85
        },
        {
          "id": "node_2",
          "label": "Entity B",
          "pagerank": 0.72
        }
      ],
      "edges": [
        {
          "source": "node_1",
          "target": "node_2",
          "weight": 0.9
        }
      ]
    },
    "mind_map": {}
  }
}
```

---


## 12. 删除知识图谱 (Delete Knowledge Graph)

- **URL**: `/{kb_id}/knowledge_graph`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/kb/a1b2c3d4e5f6789012345678/knowledge_graph" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 13. 获取元数据 (Get Meta)

- **URL**: `/get_meta`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_ids` | string | 是 | 知识库 ID 列表 (逗号分隔) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/get_meta?kb_ids=kb_1,kb_2" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "author": {
      "John Doe": ["doc_id_1", "doc_id_2"],
      "Jane Smith": ["doc_id_3"]
    },
    "category": {
      "技术文档": ["doc_id_1"],
      "用户手册": ["doc_id_2", "doc_id_3"]
    },
    "year": {
      "2024": ["doc_id_1", "doc_id_2"],
      "2025": ["doc_id_3"]
    }
  }
}
```

---


## 14. 获取基本信息 (Get Basic Info)

- **URL**: `/basic_info`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/basic_info?kb_id=a1b2c3d4e5f6789012345678" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "processing": 2,
    "finished": 15,
    "failed": 1,
    "cancelled": 0,
    "downloaded": 3
  }
}
```

---


## 15. 获取管道日志 (List Pipeline Logs)

- **URL**: `/list_pipeline_logs`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 10) |
| `keywords` | string | 否 | 搜索关键词 |
| `orderby` | string | 否 | 排序字段 (默认 create_time) |
| `desc` | string/bool | 否 | 是否倒序 (默认 true) |
| `create_date_from` | string | 否 | 开始日期 |
| `create_date_to` | string | 否 | 结束日期 |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `operation_status` | list[string] | 否 | 状态筛选 |
| `types` | list[string] | 否 | 文件类型筛选 |
| `suffix` | list[string] | 否 | 后缀筛选 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/list_pipeline_logs?kb_id=a1b2c3d4e5f6789012345678&page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "operation_status": ["1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 100,
    "logs": [
      {
        "id": "log_123456",
        "document_id": "doc_789012",
        "tenant_id": "user123456",
        "kb_id": "a1b2c3d4e5f6789012345678",
        "pipeline_id": null,
        "pipeline_title": "naive",
        "parser_id": "naive",
        "document_name": "example.pdf",
        "document_suffix": "pdf",
        "document_type": "pdf",
        "source_from": "local",
        "progress": 1.0,
        "progress_msg": "Parsing completed successfully",
        "process_begin_at": "2024-01-15 10:30:00",
        "process_duration": 12.5,
        "dsl": {},
        "task_type": "file",
        "operation_status": "3",
        "avatar": null,
        "status": "1",
        "create_time": 1705300200,
        "create_date": "2024-01-15 10:30:00",
        "update_time": 1705300213,
        "update_date": "2024-01-15 10:30:13"
      }
    ]
  }
}
```

---


## 16. 获取管道数据集日志 (List Pipeline Dataset Logs)

- **URL**: `/list_pipeline_dataset_logs`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 10) |
| `orderby` | string | 否 | 排序字段 (默认 create_time) |
| `desc` | string/bool | 否 | 是否倒序 (默认 true) |
| `create_date_from` | string | 否 | 开始日期 |
| `create_date_to` | string | 否 | 结束日期 |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `operation_status` | list[string] | 否 | 状态筛选 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/list_pipeline_dataset_logs?kb_id=a1b2c3d4e5f6789012345678&page=1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{}'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 5,
    "logs": [
      {
        "id": "log_dataset_001",
        "tenant_id": "user123456",
        "kb_id": "a1b2c3d4e5f6789012345678",
        "progress": 1.0,
        "progress_msg": "GraphRAG completed",
        "process_begin_at": "2024-01-15 11:00:00",
        "process_duration": 300.5,
        "task_type": "graphrag",
        "operation_status": "3",
        "avatar": null,
        "status": "1",
        "create_time": 1705302000,
        "create_date": "2024-01-15 11:00:00",
        "update_time": 1705302301,
        "update_date": "2024-01-15 11:05:01"
      }
    ]
  }
}
```

---


## 17. 删除管道日志 (Delete Pipeline Logs)

- **URL**: `/delete_pipeline_logs`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `log_ids` | list[string] | 是 | 日志 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/delete_pipeline_logs?kb_id=a1b2c3d4e5f6789012345678" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "log_ids": ["log_1", "log_2"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 18. 管道日志详情 (Pipeline Log Detail)

- **URL**: `/pipeline_log_detail`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `log_id` | string | 是 | 日志 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/pipeline_log_detail?log_id=log_123456" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "log_123456",
    "document_id": "doc_789012",
    "tenant_id": "user123456",
    "kb_id": "a1b2c3d4e5f6789012345678",
    "pipeline_id": null,
    "pipeline_title": "naive",
    "parser_id": "naive",
    "document_name": "example.pdf",
    "document_suffix": "pdf",
    "document_type": "pdf",
    "source_from": "local",
    "progress": 1.0,
    "progress_msg": "Parsing completed successfully",
    "process_begin_at": "2024-01-15 10:30:00",
    "process_duration": 12.5,
    "dsl": {},
    "task_type": "file",
    "operation_status": "3",
    "avatar": null,
    "status": "1",
    "create_time": 1705300200,
    "create_date": "2024-01-15 10:30:00",
    "update_time": 1705300213,
    "update_date": "2024-01-15 10:30:13"
  }
}
```

---


## 19. 运行 GraphRAG 任务 (Run GraphRAG)

- **URL**: `/run_graphrag`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/run_graphrag" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "a1b2c3d4e5f6789012345678"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "graphrag_task_id": "task_graphrag_001"
  }
}
```

---


## 20. 追踪 GraphRAG 任务 (Trace GraphRAG)

- **URL**: `/trace_graphrag`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/trace_graphrag?kb_id=a1b2c3d4e5f6789012345678" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "task_graphrag_001",
    "doc_id": "graph_raptor_x",
    "from_page": 0,
    "to_page": 100000000,
    "task_type": "graphrag",
    "priority": 0,
    "begin_at": "2024-01-15 12:00:00",
    "process_duration": 150.5,
    "progress": 0.8,
    "progress_msg": "Building knowledge graph...",
    "retry_count": 0,
    "digest": "",
    "chunk_ids": "",
    "create_time": 1705305600,
    "update_time": 1705305750
  }
}
```

---


## 21. 运行 RAPTOR 任务 (Run RAPTOR)

- **URL**: `/run_raptor`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/run_raptor" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "a1b2c3d4e5f6789012345678"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "raptor_task_id": "task_raptor_001"
  }
}
```

---


## 22. 追踪 RAPTOR 任务 (Trace RAPTOR)

- **URL**: `/trace_raptor`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/trace_raptor?kb_id=a1b2c3d4e5f6789012345678" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "task_raptor_001",
    "doc_id": "graph_raptor_x",
    "from_page": 0,
    "to_page": 100000000,
    "task_type": "raptor",
    "priority": 0,
    "begin_at": "2024-01-15 13:00:00",
    "process_duration": 200.0,
    "progress": 0.5,
    "progress_msg": "Building hierarchical summaries...",
    "retry_count": 0,
    "digest": "",
    "chunk_ids": "",
    "create_time": 1705309200,
    "update_time": 1705309400
  }
}
```

---


## 23. 运行 Mindmap 任务 (Run Mindmap)

- **URL**: `/run_mindmap`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/run_mindmap" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "a1b2c3d4e5f6789012345678"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "mindmap_task_id": "task_mindmap_001"
  }
}
```

---


## 24. 追踪 Mindmap 任务 (Trace Mindmap)

- **URL**: `/trace_mindmap`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/trace_mindmap?kb_id=a1b2c3d4e5f6789012345678" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "task_mindmap_001",
    "doc_id": "graph_raptor_x",
    "from_page": 0,
    "to_page": 100000000,
    "task_type": "mindmap",
    "priority": 0,
    "begin_at": "2024-01-15 14:00:00",
    "process_duration": 100.0,
    "progress": 1.0,
    "progress_msg": "Mindmap generation completed",
    "retry_count": 0,
    "digest": "",
    "chunk_ids": "",
    "create_time": 1705312800,
    "update_time": 1705312900
  }
}
```

---


## 25. 取消/解绑任务 (Unbind Task)

- **URL**: `/unbind_task`
- **Method**: `DELETE`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `pipeline_task_type` | string | 是 | 任务类型 ("graphrag", "raptor", "mindmap") |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/kb/unbind_task?kb_id=a1b2c3d4e5f6789012345678&pipeline_task_type=graphrag" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 26. 检查 Embedding (Check Embedding)

用于检查新的 Embedding 模型与知识库中现有向量的兼容性。

- **URL**: `/check_embedding`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `embd_id` | string | 是 | 目标 Embedding 模型 ID |
| `check_num` | int | 否 | 采样数量 (默认 5) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/kb/check_embedding" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "a1b2c3d4e5f6789012345678",
           "embd_id": "BAAI/bge-large-zh-v1.5",
           "check_num": 5
         }'
```

### 响应示例 (兼容)
```json
{
  "code": 0,
  "data": {
    "summary": {
      "kb_id": "a1b2c3d4e5f6789012345678",
      "model": "BAAI/bge-large-zh-v1.5",
      "sampled": 5,
      "valid": 5,
      "avg_cos_sim": 0.952341,
      "min_cos_sim": 0.912456,
      "max_cos_sim": 0.987654,
      "match_mode": "content_only"
    },
    "results": [
      {
        "chunk_id": "chunk_001",
        "doc_id": "doc_789012",
        "doc_name": "example.pdf",
        "vector_field": "q_1024_vec",
        "vector_dim": 1024,
        "cos_sim": 0.952341
      },
      {
        "chunk_id": "chunk_002",
        "doc_id": "doc_789012",
        "doc_name": "example.pdf",
        "vector_field": "q_1024_vec",
        "vector_dim": 1024,
        "cos_sim": 0.967890
      }
    ]
  }
}
```

### 响应示例 (不兼容)
```json
{
  "code": 108,
  "message": "Embedding model switch failed: the average similarity between old and new vectors is below 0.9, indicating incompatible vector spaces.",
  "data": {
    "summary": {
      "kb_id": "a1b2c3d4e5f6789012345678",
      "model": "text-embedding-ada-002",
      "sampled": 5,
      "valid": 5,
      "avg_cos_sim": 0.456789,
      "min_cos_sim": 0.321456,
      "max_cos_sim": 0.567890,
      "match_mode": "content_only"
    },
    "results": [
      {
        "chunk_id": "chunk_001",
        "doc_id": "doc_789012",
        "doc_name": "example.pdf",
        "vector_field": "q_1024_vec",
        "vector_dim": 1024,
        "cos_sim": 0.456789
      }
    ]
  }
}
```

---


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


# LLM Management API 文档

**Base URL**: `http://localhost:9380/v1/llm`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取 LLM 工厂列表 (List Factories)

获取系统支持的 LLM 工厂及其支持的模型类型列表。

- **URL**: `/factories`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/llm/factories" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "name": "OpenAI",
      "logo": "base64_string...",
      "tags": "LLM, Text Embedding, Image2Text, TTS",
      "status": "1",
      "model_types": ["chat", "embedding", "image2text", "tts"]
    },
    {
      "name": "VolcEngine",
      "logo": "base64_string...",
      "tags": "LLM, Text Embedding, Rerank",
      "status": "1",
      "model_types": ["chat", "embedding", "rerank"]
    },
    {
      "name": "Ollama",
      "logo": "base64_string...",
      "tags": "LLM, Text Embedding, Image2Text",
      "status": "1",
      "model_types": ["chat", "embedding", "image2text", "speech2text", "rerank", "tts", "ocr"]
    }
  ]
}
```

---


## 2. 设置 API Key (Set API Key)

为特定的 LLM 工厂设置 API Key，并测试其可用性。通常建议使用 `/add_llm` 接口，因为它处理了不同工厂的字段组合逻辑。

- **URL**: `/set_api_key`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `llm_factory` | string | 是 | LLM 工厂名称 |
| `api_key` | string | 是 | 完整的 API Key (可能是 JSON 字符串) |
| `base_url` | string | 否 | Base URL |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/llm/set_api_key" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "llm_factory": "OpenAI",
           "api_key": "sk-xxxxxx"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

### 错误响应示例
```json
{
  "code": 102,
  "message": "\nFail to access embedding model(text-embedding-3-small) using this api key.Invalid API key provided."
}
```

---


## 3. 添加 LLM (Add LLM)

添加或配置一个新的 LLM 模型。根据不同的 `llm_factory`，可能需要提供不同的认证字段（这些字段会组合成 `api_key`）。

- **URL**: `/add_llm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `llm_factory` | string | 是 | LLM 工厂名称 (e.g., "OpenAI", "VolcEngine") |
| `llm_name` | string | 否 | 模型名称 (若不提供，部分工厂会自动生成) |
| `model_type` | string | 是 | 模型类型 (chat, embedding, rerank, etc.) |
| `api_key` | string | 否 | API Key (OpenAI 等通用工厂必填) |
| `api_base` | string | 否 | API Base URL |
| `max_tokens` | integer | 否 | 最大 Token 数 |
| `ark_api_key` | string | 否 | VolcEngine 专用 |
| `endpoint_id` | string | 否 | VolcEngine 专用 |
| `hunyuan_sid` | string | 否 | Tencent Hunyuan 专用 |
| `hunyuan_sk` | string | 否 | Tencent Hunyuan 专用 |
| `...` | ... | 否 | 其他工厂专用字段 |

> 注意：对于需要多个认证字段的工厂 (如 VolcEngine, Tencent)，请直接将这些字段放在 JSON 根节点中，后端会自动组合。

### 请求示例 (OpenAI)
```bash
curl -X POST "http://localhost:9380/v1/llm/add_llm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "llm_factory": "OpenAI",
           "llm_name": "gpt-3.5-turbo",
           "model_type": "chat",
           "api_key": "sk-xxxxxx"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

### 错误响应示例
```json
{
  "code": 102,
  "message": "LLM factory InvalidFactory is not allowed"
}
```

```json
{
  "code": 102,
  "message": "\nFail to access model(OpenAI/gpt-4o).Invalid API key provided."
}
```

---


## 4. 删除 LLM (Delete LLM)

删除已配置的 LLM 模型。

- **URL**: `/delete_llm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `llm_factory` | string | 是 | LLM 工厂名称 |
| `llm_name` | string | 是 | 模型名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/llm/delete_llm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "llm_factory": "OpenAI",
           "llm_name": "gpt-3.5-turbo"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 5. 启用/禁用 LLM (Enable/Disable LLM)

切换 LLM 模型的启用状态。

- **URL**: `/enable_llm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `llm_factory` | string | 是 | LLM 工厂名称 |
| `llm_name` | string | 是 | 模型名称 |
| `status` | string | 否 | 状态 "1" (启用) 或 "0" (禁用)，默认为 "1" |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/llm/enable_llm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "llm_factory": "OpenAI",
           "llm_name": "gpt-3.5-turbo",
           "status": "1"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 6. 删除工厂配置 (Delete Factory)

删除该租户下某个工厂的所有 LLM 配置。

- **URL**: `/delete_factory`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `llm_factory` | string | 是 | LLM 工厂名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/llm/delete_factory" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "llm_factory": "VolcEngine"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 7. 获取我的 LLM 列表 (My LLMs)

获取当前用户（租户）配置的所有 LLM 模型。

- **URL**: `/my_llms`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `include_details` | string | 否 | 是否包含详细信息 (true/false)，默认为 false |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/llm/my_llms?include_details=true" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例 (include_details=false，默认)
```json
{
  "code": 0,
  "data": {
    "OpenAI": {
      "tags": "LLM, Text Embedding, Image2Text, TTS",
      "llm": [
        {
          "type": "chat",
          "name": "gpt-4o",
          "used_token": 15000,
          "status": "1"
        },
        {
          "type": "embedding",
          "name": "text-embedding-3-small",
          "used_token": 5000,
          "status": "1"
        }
      ]
    },
    "VolcEngine": {
      "tags": "LLM, Text Embedding, Rerank",
      "llm": [
        {
          "type": "chat",
          "name": "doubao-pro-32k",
          "used_token": 2000,
          "status": "1"
        }
      ]
    }
  }
}
```

### 响应示例 (include_details=true)
```json
{
  "code": 0,
  "data": {
    "OpenAI": {
      "tags": "LLM, Text Embedding, Image2Text, TTS",
      "llm": [
        {
          "type": "chat",
          "name": "gpt-4o",
          "used_token": 15000,
          "api_base": "https://api.openai.com/v1",
          "max_tokens": 128000,
          "status": "1"
        },
        {
          "type": "embedding",
          "name": "text-embedding-3-small",
          "used_token": 5000,
          "api_base": "",
          "max_tokens": 8191,
          "status": "1"
        }
      ]
    },
    "Ollama": {
      "tags": "LLM, Text Embedding, Image2Text",
      "llm": [
        {
          "type": "chat",
          "name": "llama3.1:8b",
          "used_token": 0,
          "api_base": "http://localhost:11434",
          "max_tokens": 8192,
          "status": "1"
        }
      ]
    }
  }
}
```

---


## 8. 获取可用 LLM 列表 (List LLMs)

获取所有可用的 LLM 模型，包括系统内置的和用户配置的。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `model_type` | string | 否 | 筛选模型类型 (e.g., "chat", "embedding") |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/llm/list?model_type=chat" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "OpenAI": [
      {
        "llm_name": "gpt-4o",
        "model_type": "chat",
        "fid": "OpenAI",
        "max_tokens": 128000,
        "tags": "LLM, 128k",
        "is_tools": true,
        "status": "1",
        "available": true
      },
      {
        "llm_name": "gpt-4o-mini",
        "model_type": "chat",
        "fid": "OpenAI",
        "max_tokens": 128000,
        "tags": "LLM, 128k",
        "is_tools": true,
        "status": "1",
        "available": true
      },
      {
        "llm_name": "text-embedding-3-small",
        "model_type": "embedding",
        "fid": "OpenAI",
        "max_tokens": 8191,
        "tags": "Text Embedding",
        "is_tools": false,
        "status": "1",
        "available": true
      }
    ],
    "Ollama": [
      {
        "llm_name": "llama3.1:8b",
        "model_type": "chat",
        "fid": "Ollama",
        "available": true,
        "status": "1"
      }
    ],
    "Builtin": [
      {
        "llm_name": "flag-embedding",
        "model_type": "embedding",
        "fid": "Builtin",
        "max_tokens": 8192,
        "tags": "Text Embedding",
        "is_tools": false,
        "status": "1",
        "available": true
      }
    ]
  }
}
```

### 响应示例 (筛选 model_type=embedding)
```json
{
  "code": 0,
  "data": {
    "OpenAI": [
      {
        "llm_name": "text-embedding-3-small",
        "model_type": "embedding",
        "fid": "OpenAI",
        "max_tokens": 8191,
        "tags": "Text Embedding",
        "is_tools": false,
        "status": "1",
        "available": true
      },
      {
        "llm_name": "text-embedding-3-large",
        "model_type": "embedding",
        "fid": "OpenAI",
        "max_tokens": 8191,
        "tags": "Text Embedding",
        "is_tools": false,
        "status": "1",
        "available": true
      }
    ],
    "Builtin": [
      {
        "llm_name": "flag-embedding",
        "model_type": "embedding",
        "fid": "Builtin",
        "max_tokens": 8192,
        "tags": "Text Embedding",
        "is_tools": false,
        "status": "1",
        "available": true
      }
    ]
  }
}
```


---


# MCP Server API 文档

**Base URL**: `http://localhost:9380/v1/mcp_server`

**Authentication**:
所有接口均需要认证（除 `/test_mcp` 外，但通常也建议携带）。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取 MCP Server 列表 (List MCP Servers)

获取当前用户的 MCP Server 列表。

- **URL**: `/list`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认不限制) |
| `orderby` | string | 否 | 排序字段 (默认 `create_time`) |
| `desc` | boolean | 否 | 是否降序 (默认 `true`) |
| `keywords` | string | 否 | 搜索关键字 |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_ids` | list[string] | 否 | 指定 MCP ID 列表进行筛选 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/list?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_ids": []
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "mcp_servers": [
      {
        "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
        "name": "My MCP Server",
        "server_type": "sse",
        "url": "http://example.com/sse",
        "description": "A sample MCP server",
        "variables": {
          "tools": {
            "get_weather": {
              "name": "get_weather",
              "description": "Get weather info",
              "enabled": true
            }
          }
        },
        "create_date": "2024-01-15 10:30:00",
        "update_date": "2024-01-15 10:30:00"
      }
    ],
    "total": 1
  }
}
```

---


## 2. 获取 MCP Server 详情 (Get MCP Server Detail)

获取指定 MCP Server 的详细信息。

- **URL**: `/detail`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_id` | string | 是 | MCP Server ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/mcp_server/detail?mcp_id=mcp_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "name": "My MCP Server",
    "tenant_id": "tenant_abc123",
    "url": "http://example.com/sse",
    "server_type": "sse",
    "description": null,
    "variables": {
      "tools": {
        "get_weather": {
          "name": "get_weather",
          "description": "Get weather info",
          "enabled": true
        }
      }
    },
    "headers": {},
    "create_time": 1705312200000,
    "create_date": "2024-01-15 10:30:00",
    "update_time": 1705312200000,
    "update_date": "2024-01-15 10:30:00"
  }
}
```

---


## 3. 创建 MCP Server (Create MCP Server)

创建一个新的 MCP Server。

- **URL**: `/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | MCP Server 名称 (最大 255 字节) |
| `url` | string | 是 | MCP Server 地址 |
| `server_type` | string | 是 | 类型 (`sse` 或 `stdio`) |
| `headers` | json/string | 否 | 请求头配置 |
| `variables` | json/string | 否 | 环境变量配置 |
| `timeout` | float | 否 | 超时时间 (默认 10秒) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "Weather MCP",
           "url": "http://weather-mcp.example.com/sse",
           "server_type": "sse",
           "headers": {"Authorization": "Basic xxx"}
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "tenant_id": "tenant_abc123",
    "name": "Weather MCP",
    "url": "http://weather-mcp.example.com/sse",
    "server_type": "sse",
    "headers": {"Authorization": "Basic xxx"},
    "variables": {
      "tools": {
        "get_weather": {
          "name": "get_weather",
          "description": "Get weather info for a location",
          "inputSchema": {
            "type": "object",
            "properties": {
              "city": {"type": "string", "description": "City name"}
            },
            "required": ["city"]
          },
          "enabled": true
        }
      }
    }
  }
}
```

---


## 4. 更新 MCP Server (Update MCP Server)

更新现有的 MCP Server 信息。

- **URL**: `/update`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_id` | string | 是 | 要更新的 MCP Server ID |
| `name` | string | 否 | 新名称 |
| `url` | string | 否 | 新地址 |
| `server_type` | string | 否 | 新类型 |
| `headers` | json/string | 否 | 新请求头 |
| `variables` | json/string | 否 | 新变量 |
| `timeout` | float | 否 | 超时时间 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/update" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_id": "mcp_1",
           "name": "Updated Weather MCP"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "name": "Updated Weather MCP",
    "tenant_id": "tenant_abc123",
    "url": "http://weather-mcp.example.com/sse",
    "server_type": "sse",
    "description": null,
    "variables": {
      "tools": {
        "get_weather": {
          "name": "get_weather",
          "description": "Get weather info",
          "enabled": true
        }
      }
    },
    "headers": {},
    "create_time": 1705312200000,
    "create_date": "2024-01-15 10:30:00",
    "update_time": 1705398600000,
    "update_date": "2024-01-16 10:30:00"
  }
}
```

---


## 5. 删除 MCP Server (Remove MCP Server)

删除一个或多个 MCP Server。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_ids` | list[string] | 是 | 要删除的 MCP Server ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_ids": ["mcp_1", "mcp_2"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---


## 6. 导入 MCP Server (Import MCP Servers)

批量导入 MCP Server 配置。

- **URL**: `/import`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcpServers` | dict | 是 | Server 名称到配置的映射 |
| `timeout` | float | 否 | 连接测试超时时间 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/import" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcpServers": {
             "my-server": {
               "type": "sse",
               "url": "http://localhost:8080/sse"
             }
           }
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "results": [
      {
        "server": "my-server",
        "success": true,
        "action": "created",
        "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
        "new_name": "my-server"
      },
      {
        "server": "existing-server",
        "success": true,
        "action": "created",
        "id": "b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7",
        "new_name": "existing-server_0",
        "message": "Renamed from 'existing-server' to 'existing-server_0' avoid duplication"
      },
      {
        "server": "invalid-server",
        "success": false,
        "message": "Missing required fields (type or url)"
      }
    ]
  }
}
```

---


## 7. 导出 MCP Server (Export MCP Servers)

导出指定的 MCP Server 配置。

- **URL**: `/export`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_ids` | list[string] | 是 | 要导出的 MCP Server ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/export" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_ids": ["mcp_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "mcpServers": {
      "My MCP Server": {
        "type": "sse",
        "url": "http://example.com/sse",
        "name": "My MCP Server",
        "authorization_token": "",
        "tools": {
          "get_weather": {
            "name": "get_weather",
            "description": "Get weather info",
            "inputSchema": {
              "type": "object",
              "properties": {
                "city": {"type": "string"}
              },
              "required": ["city"]
            },
            "enabled": true
          }
        }
      }
    }
  }
}
```

---


## 8. 获取工具列表 (List Tools)

从指定的 MCP Server 中获取可用工具列表。

- **URL**: `/list_tools`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_ids` | list[string] | 是 | MCP Server ID 列表 |
| `timeout` | float | 否 | 超时时间 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/list_tools" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_ids": ["mcp_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6": [
      {
        "name": "get_weather",
        "description": "Get weather info for a location",
        "inputSchema": {
          "type": "object",
          "properties": {
            "city": {
              "type": "string",
              "description": "City name"
            }
          },
          "required": ["city"]
        },
        "enabled": true
      },
      {
        "name": "get_forecast",
        "description": "Get weather forecast",
        "inputSchema": {
          "type": "object",
          "properties": {
            "city": {"type": "string"},
            "days": {"type": "integer", "default": 7}
          },
          "required": ["city"]
        },
        "enabled": false
      }
    ]
  }
}
```

---


## 9. 测试工具 (Test Tool)

调用指定的 MCP 工具进行测试。

- **URL**: `/test_tool`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_id` | string | 是 | MCP Server ID |
| `tool_name` | string | 是 | 工具名称 |
| `arguments` | dict | 是 | 工具参数 |
| `timeout` | float | 否 | 超时时间 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/test_tool" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_id": "mcp_1",
           "tool_name": "get_weather",
           "arguments": {"city": "Beijing"}
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "content": [
      {
        "type": "text",
        "text": "Weather in Beijing: Sunny, 25°C, Humidity 45%"
      }
    ],
    "isError": false
  }
}
```

---


## 10. 缓存工具 (Cache Tools)

更新 MCP Server 的工具缓存配置。

- **URL**: `/cache_tools`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_id` | string | 是 | MCP Server ID |
| `tools` | list[dict] | 是 | 工具列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/cache_tools" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_id": "mcp_1",
           "tools": [{"name": "get_weather", "enabled": true}]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "get_weather": {
      "name": "get_weather",
      "description": "Get weather info",
      "inputSchema": {
        "type": "object",
        "properties": {
          "city": {"type": "string"}
        },
        "required": ["city"]
      },
      "enabled": true
    }
  }
}
```

---


## 11. 测试 MCP 连接 (Test MCP)

测试连接到一个 MCP Server 并获取其工具列表（不保存）。

- **URL**: `/test_mcp`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `url` | string | 是 | MCP Server URL |
| `server_type` | string | 是 | 类型 (`sse` 或 `stdio`) |
| `headers` | json/string | 否 | 请求头 |
| `variables` | json/string | 否 | 变量 |
| `timeout` | float | 否 | 超时时间 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/test_mcp" \
     -H "Content-Type: application/json" \
     -d '{
           "url": "http://localhost:8080/sse",
           "server_type": "sse"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "name": "get_weather",
      "description": "Get weather info for a location",
      "inputSchema": {
        "type": "object",
        "properties": {
          "city": {
            "type": "string",
            "description": "City name"
          }
        },
        "required": ["city"]
      },
      "enabled": true
    },
    {
      "name": "get_forecast",
      "description": "Get weather forecast for upcoming days",
      "inputSchema": {
        "type": "object",
        "properties": {
          "city": {"type": "string"},
          "days": {"type": "integer", "default": 7}
        },
        "required": ["city"]
      },
      "enabled": true
    }
  ]
}
```


---


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
    "avatar": null,
    "tenant_id": "tenant_xxx",
    "owner_name": null,
    "memory_type": ["raw", "semantic"],
    "storage_type": "table",
    "embd_id": "embd_123",
    "llm_id": "llm_123",
    "permissions": "me",
    "description": null,
    "memory_size": 5242880,
    "forgetting_policy": "FIFO",
    "temperature": 0.5,
    "system_prompt": "...",
    "user_prompt": null,
    "create_time": 1700000000,
    "create_date": "2024-01-01 00:00:00",
    "update_time": 1700000000,
    "update_date": "2024-01-01 00:00:00"
  },
  "message": true
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
    "avatar": null,
    "tenant_id": "tenant_xxx",
    "owner_name": null,
    "memory_type": ["raw", "semantic"],
    "storage_type": "table",
    "embd_id": "embd_123",
    "llm_id": "llm_123",
    "permissions": "me",
    "description": null,
    "memory_size": 5242880,
    "forgetting_policy": "FIFO",
    "temperature": 0.7,
    "system_prompt": "...",
    "user_prompt": null,
    "create_time": 1700000000,
    "create_date": "2024-01-01 00:00:00",
    "update_time": 1700000001,
    "update_date": "2024-01-01 00:00:01"
  },
  "message": true
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
  "data": null,
  "message": true
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
        "avatar": null,
        "tenant_id": "tenant_xxx",
        "owner_name": "User Name",
        "memory_type": ["raw"],
        "storage_type": "table",
        "permissions": "me",
        "description": null,
        "create_time": 1700000000,
        "create_date": "2024-01-01 00:00:00"
      }
    ],
    "total_count": 1
  },
  "message": true
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
    "avatar": null,
    "tenant_id": "tenant_xxx",
    "owner_name": "User Name",
    "memory_type": ["raw"],
    "storage_type": "table",
    "embd_id": "embd_123",
    "llm_id": "llm_123",
    "permissions": "me",
    "description": null,
    "memory_size": 5242880,
    "forgetting_policy": "FIFO",
    "temperature": 0.5,
    "system_prompt": "...",
    "user_prompt": null,
    "create_time": 1700000000,
    "create_date": "2024-01-01 00:00:00",
    "update_time": 1700000000,
    "update_date": "2024-01-01 00:00:00"
  },
  "message": true
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
      "message_list": [
        {
          "message_id": 1,
          "agent_id": "agent_xxx",
          "agent_name": "Agent Name",
          "content": "...",
          "task": {}
        }
      ],
      "total": 1
    },
    "storage_type": "table"
  },
  "message": true
}
```


---


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

**成功响应:**
```json
{
  "code": 0,
  "message": "Successfully added to memories.",
  "data": null
}
```

**部分失败响应:**
```json
{
  "code": 100,
  "message": "Some messages failed to add.",
  "data": [
    {
      "memory_id": "mem_123",
      "success": true,
      "message": "Message saved successfully."
    },
    {
      "memory_id": "mem_456",
      "success": false,
      "message": "Memory 'mem_456' not found."
    }
  ]
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

**成功响应:**
```json
{
  "code": 0,
  "message": true,
  "data": null
}
```

**失败响应 (Memory 不存在):**
```json
{
  "code": 101,
  "message": "Memory 'mem_123' not found.",
  "data": null
}
```

**失败响应 (操作失败):**
```json
{
  "code": 100,
  "message": "Failed to forget message '1001' in memory 'mem_123'.",
  "data": null
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

**成功响应:**
```json
{
  "code": 0,
  "message": true,
  "data": null
}
```

**失败响应 (Memory 不存在):**
```json
{
  "code": 101,
  "message": "Memory 'mem_123' not found.",
  "data": null
}
```

**失败响应 (参数错误):**
```json
{
  "code": 102,
  "message": "Status must be a boolean.",
  "data": null
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

**成功响应:**
```json
{
  "code": 0,
  "message": true,
  "data": [
    {
      "message_id": 1001,
      "message_type": "raw",
      "source_id": 0,
      "memory_id": "mem_123",
      "user_id": "",
      "agent_id": "agent_1",
      "session_id": "session_abc",
      "valid_at": "2024-01-01 12:00:00",
      "invalid_at": null,
      "forget_at": null,
      "status": 1,
      "content": "User Input: Hello\nAgent Response: Hi there!"
    },
    {
      "message_id": 1002,
      "message_type": "semantic",
      "source_id": 1001,
      "memory_id": "mem_123",
      "user_id": "",
      "agent_id": "agent_1",
      "session_id": "session_abc",
      "valid_at": "2024-01-01 12:00:00",
      "invalid_at": null,
      "forget_at": null,
      "status": 1,
      "content": "The user greeted the agent."
    }
  ]
}
```

**失败响应 (参数缺失):**
```json
{
  "code": 102,
  "message": "memory_ids is required.",
  "data": null
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

**成功响应:**
```json
{
  "code": 0,
  "message": true,
  "data": [
    {
      "message_id": 1001,
      "message_type": "raw",
      "source_id": 0,
      "memory_id": "mem_123",
      "user_id": "",
      "agent_id": "agent_1",
      "session_id": "session_abc",
      "valid_at": "2024-01-01 12:00:00",
      "invalid_at": null,
      "forget_at": null,
      "status": 1,
      "content": "User Input: Hello world\nAgent Response: Hi there!"
    }
  ]
}
```

**失败响应 (参数缺失):**
```json
{
  "code": 102,
  "message": "memory_id, query can't be empty.",
  "data": null
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

**成功响应:**
```json
{
  "code": 0,
  "message": true,
  "data": {
    "message_id": 1001,
    "message_type": "raw",
    "source_id": 0,
    "memory_id": "mem_123",
    "user_id": "",
    "agent_id": "agent_1",
    "session_id": "session_abc",
    "valid_at": "2024-01-01 12:00:00",
    "invalid_at": null,
    "forget_at": null,
    "status": 1,
    "content": "User Input: Hello\nAgent Response: Hi there!"
  }
}
```

**失败响应 (Memory 不存在):**
```json
{
  "code": 101,
  "message": "Memory 'mem_123' not found.",
  "data": null
}
```

**失败响应 (Message 不存在):**
```json
{
  "code": 101,
  "message": "Message '1001' in memory 'mem_123' not found.",
  "data": null
}
```


---


# Plugin API 文档

**Base URL**: `http://localhost:9380/v1/plugin`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取 LLM 工具列表 (Get LLM Tools)

获取系统支持的 LLM 工具列表。

- **URL**: `/llm_tools`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| - | - | - | - |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/plugin/llm_tools" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "message": "success",
  "data": [
    {
      "name": "bad_calculator",
      "displayName": "$t:bad_calculator.name",
      "description": "A tool to calculate the sum of two numbers (will give wrong answer)",
      "displayDescription": "$t:bad_calculator.description",
      "parameters": {
        "a": {
          "type": "number",
          "description": "The first number",
          "displayDescription": "$t:bad_calculator.params.a",
          "required": true
        },
        "b": {
          "type": "number",
          "description": "The second number",
          "displayDescription": "$t:bad_calculator.params.b",
          "required": true
        }
      }
    }
  ]
}
```

---
@api/docs/api_app.md 这个接口文档是根据@api/apps/api_app.py 生成的。
请参考 api_app.md 这个文档的结构：每个 api 要尽可能涵盖“请求参数 (xxx)”、“请求示例”、“响应示例” 三部分，并且“请求参数”要以 @api/docs/api_app.md:19-20 这种形式整理。
现在，请遍历@api/apps/chunk_app.py  里的每个接口，生成相应的接口文档，以 markdown 的形式，保存到 api/docs/ 下面。




请根据@api/apps/user_app.py @api/db/services @api/db/db_models.py ，修改@api/docs/user_app.md 里所有的响应示例

---


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


---


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
    "description": "A helpful Assistant",
    "avatar": "",
    "tenant_id": "tenant_1",
    "language": "English",
    "dataset_ids": ["kb_123"],
    "llm": {
      "model_name": "gpt-3.5-turbo",
      "temperature": 0.1,
      "top_p": 0.3,
      "frequency_penalty": 0.7,
      "presence_penalty": 0.4,
      "max_tokens": 512
    },
    "prompt": {
      "prompt": "You are a helpful Chat...",
      "variables": [{"key": "knowledge", "optional": false}],
      "opener": "Hi!",
      "show_quote": true,
      "empty_response": "Sorry! No relevant content was found in the knowledge base!",
      "tts": false,
      "refine_multiturn": true,
      "similarity_threshold": 0.2,
      "keywords_similarity_weight": 0.7,
      "top_n": 6,
      "rerank_model": ""
    },
    "prompt_type": "simple",
    "do_refer": "1",
    "status": "1",
    "create_time": 1700000000,
    "update_time": 1700000000,
    "create_date": "2024-01-01 00:00:00",
    "update_date": "2024-01-01 00:00:00"
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

**部分删除成功时的响应示例**:
```json
{
  "code": 0,
  "data": {
    "success_count": 2,
    "errors": ["Assistant(chat_xxx) not found."]
  },
  "message": "Partially deleted 2 chats with 1 errors"
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
      "description": "A helpful Assistant",
      "avatar": "",
      "tenant_id": "tenant_1",
      "language": "English",
      "datasets": [
        {
          "id": "kb_123",
          "name": "My Dataset",
          "description": "Dataset description",
          "tenant_id": "tenant_1",
          "embd_id": "BAAI/bge-large-zh-v1.5",
          "chunk_num": 100,
          "doc_num": 10,
          "token_num": 50000,
          "parser_id": "naive",
          "permission": "me",
          "similarity_threshold": 0.2,
          "vector_similarity_weight": 0.3,
          "status": "1",
          "create_time": 1700000000,
          "update_time": 1700000000
        }
      ],
      "llm": {
        "model_name": "gpt-3.5-turbo",
        "temperature": 0.1,
        "top_p": 0.3,
        "frequency_penalty": 0.7,
        "presence_penalty": 0.4,
        "max_tokens": 512
      },
      "prompt": {
        "prompt": "You are a helpful Chat...",
        "variables": [{"key": "knowledge", "optional": false}],
        "opener": "Hi!",
        "show_quote": true,
        "empty_response": "Sorry! No relevant content was found in the knowledge base!",
        "tts": false,
        "refine_multiturn": true,
        "similarity_threshold": 0.2,
        "keywords_similarity_weight": 0.7,
        "top_n": 6,
        "rerank_model": ""
      },
      "prompt_type": "simple",
      "do_refer": "1",
      "status": "1",
      "create_time": 1700000000,
      "update_time": 1700000000,
      "create_date": "2024-01-01 00:00:00",
      "update_date": "2024-01-01 00:00:00"
    }
  ],
  "message": "success"
}
```

**注意**: 
- 创建对话接口返回 `dataset_ids`（知识库 ID 列表）
- 获取对话列表接口返回 `datasets`（完整的知识库对象列表）

---


# Dataset Management API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建数据集 (Create Dataset)

创建一个新的数据集 (Knowledge Base)。

- **URL**: `/datasets`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 数据集名称 |
| `avatar` | string | 否 | 数据集头像 (Base64 编码) |
| `description` | string | 否 | 数据集描述 |
| `embedding_model` | string | 否 | 嵌入模型名称 (若省略则使用 Tenant 默认模型) |
| `permission` | string | 否 | 可见性 ('me' 或 'team') |
| `chunk_method` | string | 否 | 切片方法 (默认为 "naive")。可选值: "naive", "book", "email", "laws", "manual", "one", "paper", "picture", "presentation", "qa", "table", "tag" |
| `parser_config` | object | 否 | 解析器配置 (若省略则使用服务端默认配置) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Knowledge Base",
           "permission": "me",
           "chunk_method": "naive"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "name": "My Knowledge Base",
    "avatar": "",
    "tenant_id": "user123456789",
    "language": "English",
    "description": "",
    "embedding_model": "BAAI/bge-large-zh-v1.5",
    "permission": "me",
    "created_by": "user123456789",
    "document_count": 0,
    "token_num": 0,
    "chunk_count": 0,
    "similarity_threshold": 0.2,
    "vector_similarity_weight": 0.3,
    "chunk_method": "naive",
    "pipeline_id": null,
    "parser_config": {
      "pages": [[1, 1000000]],
      "table_context_size": 0,
      "image_context_size": 0,
      "llm_id": "deepseek-chat"
    },
    "pagerank": 0,
    "graphrag_task_id": null,
    "graphrag_task_finish_at": null,
    "raptor_task_id": null,
    "raptor_task_finish_at": null,
    "mindmap_task_id": null,
    "mindmap_task_finish_at": null,
    "status": "1",
    "create_time": 1700000000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-01 12:00:00"
  },
  "message": "success"
}
```

---


## 2. 删除数据集 (Delete Datasets)

删除一个或多个数据集。

- **URL**: `/datasets`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ids` | list[string] | 是 | 要删除的数据集 ID 列表。若为 `null` 则删除所有数据集；若为空数组则不删除任何数据集。 |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/datasets" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["kb_1", "kb_2"]
         }'
```

### 响应示例 (成功)
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

### 响应示例 (部分成功)
```json
{
  "code": 0,
  "data": {
    "success_count": 1,
    "errors": ["Remove document 'doc_123' error for dataset 'kb_2'"]
  },
  "message": "Successfully deleted 1 datasets, 1 failed. Details: Remove document 'doc_123' error for dataset 'kb_2'..."
}
```

---


## 3. 更新数据集 (Update Dataset)

更新指定数据集的信息。

- **URL**: `/datasets/<dataset_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 新的数据集名称 |
| `avatar` | string | 否 | 新的头像 (Base64 编码) |
| `description` | string | 否 | 新的描述 |
| `embedding_model` | string | 否 | 新的嵌入模型名称 |
| `permission` | string | 否 | 新的权限设置 ('me' 或 'team') |
| `chunk_method` | string | 否 | 新的切片方法 |
| `pagerank` | integer | 否 | PageRank 值 (仅当 doc_engine 为 elasticsearch 时有效) |
| `parser_config` | object | 否 | 新的解析器配置 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/datasets/kb_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "description": "Updated description"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "name": "My Knowledge Base",
    "avatar": "",
    "tenant_id": "user123456789",
    "language": "English",
    "description": "Updated description",
    "embedding_model": "BAAI/bge-large-zh-v1.5",
    "permission": "me",
    "created_by": "user123456789",
    "document_count": 5,
    "token_num": 12345,
    "chunk_count": 100,
    "similarity_threshold": 0.2,
    "vector_similarity_weight": 0.3,
    "chunk_method": "naive",
    "pipeline_id": null,
    "parser_config": {
      "pages": [[1, 1000000]],
      "table_context_size": 0,
      "image_context_size": 0,
      "llm_id": "deepseek-chat"
    },
    "pagerank": 0,
    "graphrag_task_id": null,
    "graphrag_task_finish_at": null,
    "raptor_task_id": null,
    "raptor_task_finish_at": null,
    "mindmap_task_id": null,
    "mindmap_task_finish_at": null,
    "status": "1",
    "create_time": 1700000000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1700001000000,
    "update_date": "2024-01-01 12:16:40"
  },
  "message": "success"
}
```

---


## 4. 获取数据集列表 (List Datasets)

获取当前用户或 Tenant 的数据集列表。

- **URL**: `/datasets`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 否 | 按数据集 ID 筛选 |
| `name` | string | 否 | 按数据集名称筛选 |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 30) |
| `orderby` | string | 否 | 排序字段 (默认 "create_time") |
| `desc` | boolean | 否 | 是否降序 (默认 true) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "a1b2c3d4e5f6789012345678",
      "name": "Dataset 1",
      "avatar": "",
      "tenant_id": "user123456789",
      "language": "English",
      "description": "My first dataset",
      "embedding_model": "BAAI/bge-large-zh-v1.5",
      "permission": "me",
      "created_by": "user123456789",
      "document_count": 10,
      "token_num": 50000,
      "chunk_count": 500,
      "similarity_threshold": 0.2,
      "vector_similarity_weight": 0.3,
      "chunk_method": "naive",
      "pipeline_id": null,
      "parser_config": {
        "pages": [[1, 1000000]],
        "table_context_size": 0,
        "image_context_size": 0,
        "llm_id": "deepseek-chat"
      },
      "pagerank": 0,
      "graphrag_task_id": null,
      "graphrag_task_finish_at": null,
      "raptor_task_id": null,
      "raptor_task_finish_at": null,
      "mindmap_task_id": null,
      "mindmap_task_finish_at": null,
      "status": "1",
      "create_time": 1700000000000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1700000000000,
      "update_date": "2024-01-01 12:00:00"
    }
  ],
  "total": 100,
  "message": "success"
}
```

---


## 5. 获取知识图谱 (Get Knowledge Graph)

获取数据集的知识图谱数据 (节点和边)。

- **URL**: `/datasets/<dataset_id>/knowledge_graph`
- **Method**: `GET`

### 请求参数

无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/kb_123/knowledge_graph" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "graph": {
      "nodes": [
        {
          "id": "node_1",
          "label": "Entity A",
          "pagerank": 0.85
        },
        {
          "id": "node_2",
          "label": "Entity B",
          "pagerank": 0.72
        }
      ],
      "edges": [
        {
          "source": "node_1",
          "target": "node_2",
          "weight": 0.9,
          "label": "related_to"
        }
      ]
    },
    "mind_map": {}
  },
  "message": "success"
}
```

---


## 6. 删除知识图谱 (Delete Knowledge Graph)

删除数据集的知识图谱数据。

- **URL**: `/datasets/<dataset_id>/knowledge_graph`
- **Method**: `DELETE`

### 请求参数

无

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/datasets/kb_123/knowledge_graph" \
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


## 7. 运行 GraphRAG (Run GraphRAG)

对数据集运行 GraphRAG 任务 (需确保文档已解析)。

- **URL**: `/datasets/<dataset_id>/run_graphrag`
- **Method**: `POST`

### 请求参数

无

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/kb_123/run_graphrag" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "graphrag_task_id": "a1b2c3d4e5f6789012345678"
  },
  "message": "success"
}
```

---


## 8. 追踪 GraphRAG 状态 (Trace GraphRAG)

获取 GraphRAG 任务的执行状态。

- **URL**: `/datasets/<dataset_id>/trace_graphrag`
- **Method**: `GET`

### 请求参数

无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/kb_123/trace_graphrag" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "doc_id": "graph_raptor_x",
    "from_page": 0,
    "to_page": 100000000,
    "task_type": "graphrag",
    "priority": 0,
    "begin_at": "2024-01-01 12:00:00",
    "process_duration": 120.5,
    "progress": 0.5,
    "progress_msg": "12:00:00 Task has been received.\n12:01:00 Processing entities...",
    "retry_count": 0,
    "digest": "",
    "chunk_ids": "",
    "create_time": 1700000000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1700000120000,
    "update_date": "2024-01-01 12:02:00"
  },
  "message": "success"
}
```

### 响应示例 (任务未找到)
```json
{
  "code": 0,
  "data": {},
  "message": "success"
}
```

---


## 9. 运行 RAPTOR (Run RAPTOR)

对数据集运行 RAPTOR 任务 (递归摘要)。

- **URL**: `/datasets/<dataset_id>/run_raptor`
- **Method**: `POST`

### 请求参数

无

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/kb_123/run_raptor" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "raptor_task_id": "a1b2c3d4e5f6789012345678"
  },
  "message": "success"
}
```

---


## 10. 追踪 RAPTOR 状态 (Trace RAPTOR)

获取 RAPTOR 任务的执行状态。

- **URL**: `/datasets/<dataset_id>/trace_raptor`
- **Method**: `GET`

### 请求参数

无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/kb_123/trace_raptor" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "doc_id": "graph_raptor_x",
    "from_page": 0,
    "to_page": 100000000,
    "task_type": "raptor",
    "priority": 0,
    "begin_at": "2024-01-01 12:00:00",
    "process_duration": 300.0,
    "progress": 1.0,
    "progress_msg": "12:00:00 Task has been received.\n12:05:00 RAPTOR completed successfully.",
    "retry_count": 0,
    "digest": "",
    "chunk_ids": "chunk_1 chunk_2 chunk_3",
    "create_time": 1700000000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1700000300000,
    "update_date": "2024-01-01 12:05:00"
  },
  "message": "success"
}
```

### 响应示例 (任务未找到)
```json
{
  "code": 0,
  "data": {},
  "message": "success"
}
```

---


# Dify Retrieval API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 检索 (Retrieval)

Dify 兼容的检索接口，支持从指定的知识库中检索相关内容。

- **URL**: `/dify/retrieval`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `knowledge_id` | string | 是 | Knowledge base ID (知识库 ID) |
| `query` | string | 是 | Query text (检索关键词) |
| `use_kg` | boolean | 否 | Whether to use knowledge graph (是否使用知识图谱，默认 false) |
| `retrieval_setting` | object | 否 | Retrieval configuration (检索设置) |
| `retrieval_setting.score_threshold` | number | 否 | Similarity threshold (相似度阈值，默认 0.0) |
| `retrieval_setting.top_k` | integer | 否 | Number of results to return (返回结果数量，默认 1024) |
| `metadata_condition` | object | 否 | Metadata filter condition (元数据过滤条件) |
| `metadata_condition.logic` | string | 否 | Logic connection (逻辑关系 'and' 或 'or') |
| `metadata_condition.conditions` | array | 否 | List of conditions (条件列表) |
| `metadata_condition.conditions[].name` | string | 否 | Field name (字段名) |
| `metadata_condition.conditions[].comparison_operator` | string | 否 | Operator (操作符，如 =, <, > 等) |
| `metadata_condition.conditions[].value` | string | 否 | Field value (字段值) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/dify/retrieval" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "knowledge_id": "kb_123456",
           "query": "什么是 RAGFlow？",
           "retrieval_setting": {
             "score_threshold": 0.5,
             "top_k": 5
           },
           "metadata_condition": {
             "logic": "and",
             "conditions": [
               {
                 "name": "author",
                 "comparison_operator": "=",
                 "value": "admin"
               }
             ]
           }
         }'
```

### 响应示例

**成功响应 (200)**
```json
{
  "records": [
    {
      "content": "RAGFlow is an open-source RAG engine based on deep document understanding...",
      "score": 0.89,
      "title": "RAGFlow_Introduction.pdf",
      "metadata": {
        "doc_id": "abc123def456",
        "author": "admin",
        "category": "技术文档"
      }
    },
    {
      "content": "RAGFlow 支持多种文档格式，包括 PDF、Word、Excel 等...",
      "score": 0.75,
      "title": "RAGFlow_用户手册.docx",
      "metadata": {
        "doc_id": "xyz789ghi012",
        "version": "1.0"
      }
    }
  ]
}
```

**知识库不存在 (404)**
```json
{
  "code": 102,
  "message": "Knowledgebase not found!"
}
```

**未找到相关 chunk (404)**
```json
{
  "code": 102,
  "message": "No chunk found! Check the chunk status please!"
}
```

**服务器错误 (500)**
```json
{
  "code": 100,
  "message": "Internal server error message"
}
```

### 响应字段说明

| 字段名 | 类型 | 描述 |
| :--- | :--- | :--- |
| `records` | array | 检索结果列表 |
| `records[].content` | string | Chunk 内容文本 |
| `records[].score` | number | 相似度分数 (0-1) |
| `records[].title` | string | 文档名称 |
| `records[].metadata` | object | 元数据信息 |
| `records[].metadata.doc_id` | string | 文档 ID |
| `records[].metadata.*` | any | 其他用户自定义的元数据字段 |


---


# Document Management API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 上传文件 (Upload Documents)

上传文档到指定数据集。

- **URL**: `/datasets/<dataset_id>/documents`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Body/Form)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file` | file | 是 | 要上传的文档文件 (支持多个文件) |
| `parent_path` | string | 否 | 父文件夹路径，使用 '/' 分隔 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/dataset_123/documents" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -F "file=@/path/to/document.pdf" \
     -F "parent_path=/"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "doc_1",
      "name": "document.pdf",
      "thumbnail": null,
      "dataset_id": "dataset_123",
      "chunk_method": "naive",
      "parser_config": {
        "pages": [[1, 1000000]],
        "table_context_size": 0,
        "image_context_size": 0
      },
      "source_type": "local",
      "type": "doc",
      "created_by": "user_123",
      "location": "dataset_123/doc_1",
      "size": 102400,
      "token_count": 0,
      "chunk_count": 0,
      "progress": 0.0,
      "progress_msg": "",
      "process_begin_at": null,
      "process_duration": 0.0,
      "meta_fields": {},
      "suffix": "pdf",
      "run": "UNSTART",
      "status": "1",
      "create_time": "2024-01-01 12:00:00",
      "create_date": "2024-01-01",
      "update_time": "2024-01-01 12:00:00",
      "update_date": "2024-01-01"
    }
  ],
  "message": "success"
}
```

---


## 2. 更新文档 (Update Document)

更新数据集中文档的元信息或配置。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 新的文档名称 (需包含扩展名) |
| `chunk_method` | string | 否 | 解析方法 (如: naive, manual, qa, table, etc.) |
| `parser_config` | object | 否 | 解析器配置 |
| `enabled` | boolean | 否 | 启用/禁用文档 |
| `meta_fields` | object | 否 | 元数据字段 (JSON Object) |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "new_name.pdf",
           "enabled": true
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "doc_1",
    "name": "new_name.pdf",
    "thumbnail": null,
    "dataset_id": "dataset_123",
    "chunk_method": "naive",
    "pipeline_id": null,
    "parser_config": {
      "pages": [[1, 1000000]],
      "table_context_size": 0,
      "image_context_size": 0
    },
    "source_type": "local",
    "type": "doc",
    "created_by": "user_123",
    "location": "dataset_123/doc_1",
    "size": 102400,
    "token_count": 5000,
    "chunk_count": 50,
    "progress": 1.0,
    "progress_msg": "Done",
    "process_begin_at": "2024-01-01 12:00:00",
    "process_duration": 10.5,
    "meta_fields": {},
    "suffix": "pdf",
    "run": "DONE",
    "status": "1",
    "create_time": "2024-01-01 12:00:00",
    "create_date": "2024-01-01",
    "update_time": "2024-01-01 12:05:00",
    "update_date": "2024-01-01"
  },
  "message": "success"
}
```

---


## 3. 下载文档 (Download Document)

下载数据集中的文档文件。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" --output document.pdf
```

### 响应示例
(文件流，Content-Type: application/octet-stream)

---


## 4. 获取文档列表 (List Documents)

列出数据集中的文档。

- **URL**: `/datasets/<dataset_id>/documents`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `id` | string | 否 | 按文档 ID 过滤 |
| `name` | string | 否 | 按文档名称过滤 |
| `keywords` | string | 否 | 搜索关键字 |
| `orderby` | string | 否 | 排序字段 (默认: create_time) |
| `desc` | boolean | 否 | 是否降序 (默认: true) |
| `suffix` | array[string] | 否 | 按文件后缀过滤 (e.g., pdf, docx) |
| `run` | array[string] | 否 | 按运行状态过滤 (UNSTART, RUNNING, CANCEL, DONE, FAIL) |
| `create_time_from` | integer | 否 | 创建时间起始 (Unix timestamp) |
| `create_time_to` | integer | 否 | 创建时间结束 (Unix timestamp) |
| `metadata_condition` | json string | 否 | 元数据过滤条件 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/dataset_123/documents?page=1&page_size=10&keywords=report" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 100,
    "docs": [
      {
        "id": "doc_1",
        "name": "report.pdf",
        "thumbnail": null,
        "dataset_id": "dataset_123",
        "chunk_method": "naive",
        "pipeline_id": null,
        "parser_config": {
          "pages": [[1, 1000000]],
          "table_context_size": 0,
          "image_context_size": 0
        },
        "source_type": "local",
        "type": "doc",
        "created_by": "user_123",
        "location": "dataset_123/doc_1",
        "size": 102400,
        "token_count": 5000,
        "chunk_count": 50,
        "progress": 1.0,
        "progress_msg": "Done",
        "process_begin_at": "2024-01-01 12:00:00",
        "process_duration": 10.5,
        "meta_fields": {
          "author": "Alice"
        },
        "suffix": "pdf",
        "run": "DONE",
        "status": "1",
        "create_time": "2024-01-01 12:00:00",
        "create_date": "2024-01-01",
        "update_time": "2024-01-01 12:05:00",
        "update_date": "2024-01-01",
        "title": null
      }
    ]
  },
  "message": "success"
}
```

---


## 5. 元数据摘要 (Metadata Summary)

获取数据集的元数据摘要信息。

- **URL**: `/datasets/<dataset_id>/metadata/summary`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/dataset_123/metadata/summary" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "summary": {
      "author": ["Alice", "Bob"],
      "department": ["Engineering", "Sales"],
      "year": ["2023", "2024"]
    }
  },
  "message": "success"
}
```

---


## 6. 元数据批量更新 (Metadata Batch Update)

批量更新或删除文档的元数据。

- **URL**: `/datasets/<dataset_id>/metadata/update`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `selector` | object | 否 | 选择器，包含 `metadata_condition` (filter) 或 `document_ids` |
| `updates` | list[object] | 否 | 更新操作列表，每项含 `key`, `value` |
| `deletes` | list[object] | 否 | 删除操作列表，每项含 `key` |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/dataset_123/metadata/update" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "selector": {
             "document_ids": ["doc_1", "doc_2"]
           },
           "updates": [
             {"key": "author", "value": "Alice"}
           ]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "updated": 2,
    "matched_docs": 2
  },
  "message": "success"
}
```

---


## 7. 删除文档 (Delete Documents)

删除数据集中的一个或多个文档。

- **URL**: `/datasets/<dataset_id>/documents`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ids` | list[string] | 否 | 要删除的文档 ID 列表 (若为空则删除该知识库下所有文档) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/datasets/dataset_123/documents" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["doc_1", "doc_2"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "success"
}
```

---


## 8. 解析文档 (Parse Documents)

开始解析文档（生成 Chunk）。

- **URL**: `/datasets/<dataset_id>/chunks`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `document_ids` | list[string] | 是 | 要解析的文档 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/dataset_123/chunks" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "document_ids": ["doc_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "success"
}
```

---


## 9. 停止解析 (Stop Parsing)

停止文档的解析任务。

- **URL**: `/datasets/<dataset_id>/chunks`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `document_ids` | list[string] | 是 | 要停止解析的文档 ID 列表 |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/datasets/dataset_123/chunks" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "document_ids": ["doc_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "success"
}
```

---


## 10. 获取 Chunk 列表 (List Chunks)

获取文档的 Chunk 列表或搜索 Chunk。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>/chunks`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `id` | string | 否 | 按 Chunk ID 精确查找 |
| `keywords` | string | 否 | 搜索关键字 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1/chunks?page=1&keywords=test" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 10,
    "chunks": [
      {
        "id": "chunk_1",
        "content": "This is a chunk content.",
        "document_id": "doc_1",
        "docnm_kwd": "report.pdf",
        "important_keywords": ["keyword1", "keyword2"],
        "questions": ["What is this?"],
        "dataset_id": "dataset_123",
        "image_id": "",
        "available": true,
        "positions": [[1, 100, 200, 300, 400]]
      }
    ],
    "doc": {
      "id": "doc_1",
      "name": "report.pdf",
      "thumbnail": null,
      "dataset_id": "dataset_123",
      "chunk_method": "naive",
      "pipeline_id": null,
      "parser_config": {
        "pages": [[1, 1000000]],
        "table_context_size": 0,
        "image_context_size": 0
      },
      "source_type": "local",
      "type": "doc",
      "created_by": "user_123",
      "location": "dataset_123/doc_1",
      "size": 102400,
      "token_count": 5000,
      "chunk_count": 50,
      "progress": 1.0,
      "progress_msg": "Done",
      "process_begin_at": "2024-01-01 12:00:00",
      "process_duration": 10.5,
      "meta_fields": {},
      "suffix": "pdf",
      "run": "DONE",
      "status": "1",
      "create_time": "2024-01-01 12:00:00",
      "create_date": "2024-01-01",
      "update_time": "2024-01-01 12:05:00",
      "update_date": "2024-01-01"
    }
  },
  "message": "success"
}
```

---


## 11. 添加 Chunk (Add Chunk)

手动添加 Chunk 到文档。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>/chunks`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `content` | string | 是 | Chunk 内容 |
| `important_keywords` | list[string] | 否 | 关键词列表 |
| `questions` | list[string] | 否 | 相关问题列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1/chunks" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "content": "New chunk content",
           "important_keywords": ["new", "chunk"],
           "questions": ["What is new?"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "chunk": {
      "id": "a1b2c3d4e5f6g7h8",
      "content": "New chunk content",
      "document_id": "doc_1",
      "important_keywords": ["new", "chunk"],
      "questions": ["What is new?"],
      "dataset_id": "dataset_123",
      "create_timestamp": 1704110400.0,
      "create_time": "2024-01-01 12:00:00"
    }
  },
  "message": "success"
}
```

---


## 12. 删除 Chunk (Remove Chunks)

删除文档中的一个或多个 Chunk。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>/chunks`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chunk_ids` | list[string] | 否 | 要删除的 Chunk ID 列表 (若空则删除文档下所有 Chunk) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1/chunks" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "chunk_ids": ["chunk_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "deleted 1 chunks"
}
```

---


## 13. 更新 Chunk (Update Chunk)

更新 Chunk 的内容或属性。

- **URL**: `/datasets/<dataset_id>/documents/<document_id>/chunks/<chunk_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `document_id` | string | 是 | 文档 ID |
| `chunk_id` | string | 是 | Chunk ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `content` | string | 否 | 新的 Chunk 内容 |
| `important_keywords` | list[string] | 否 | 关键词列表 |
| `questions` | list[string] | 否 | 相关问题列表 |
| `available` | boolean | 否 | 是否启用 |
| `positions` | list[list[int]] | 否 | 位置信息，每个元素为长度为 5 的整数数组 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1/chunks/chunk_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "content": "Updated content",
           "important_keywords": ["updated"],
           "available": true
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "success"
}
```

---


## 14. 检索测试 (Retrieval Test)

执行检索测试。

- **URL**: `/retrieval`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_ids` | list[string] | 是 | 搜索的数据集 ID 列表 |
| `question` | string | 是 | 查询问题 |
| `document_ids` | list[string] | 否 | 限定文档 ID 列表 |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `similarity_threshold` | number | 否 | 相似度阈值 (默认: 0.2) |
| `vector_similarity_weight` | number | 否 | 向量相似度权重 (默认: 0.3) |
| `top_k` | integer | 否 | 返回数量 (默认: 1024) |
| `highlight` | boolean | 否 | 是否高亮匹配内容 (默认: true) |
| `rerank_id` | string | 否 | 重排模型 ID |
| `keyword` | boolean | 否 | 是否进行关键词增强 |
| `cross_languages` | list[string] | 否 | 跨语言搜索配置 |
| `use_kg` | boolean | 否 | 是否使用知识图谱 |
| `toc_enhance` | boolean | 否 | 是否启用目录增强 |
| `metadata_condition` | object | 否 | 元数据过滤条件 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/retrieval" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "dataset_ids": ["dataset_123"],
           "question": "what is ragflow?",
           "top_k": 5,
           "similarity_threshold": 0.2,
           "vector_similarity_weight": 0.3,
           "highlight": true
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 5,
    "chunks": [
      {
        "id": "chunk_1",
        "content": "RAGFlow is an open-source RAG engine based on deep document understanding.",
        "document_id": "doc_1",
        "document_keyword": "ragflow_intro.pdf",
        "dataset_id": "dataset_123",
        "important_keywords": ["RAGFlow", "RAG", "document understanding"],
        "questions": [],
        "similarity": 0.95,
        "vector_similarity": 0.92,
        "term_similarity": 0.98,
        "positions": [[1, 100, 200, 300, 400]]
      },
      {
        "id": "chunk_2",
        "content": "RAGFlow provides deep document parsing capabilities.",
        "document_id": "doc_1",
        "document_keyword": "ragflow_intro.pdf",
        "dataset_id": "dataset_123",
        "important_keywords": ["document parsing"],
        "questions": [],
        "similarity": 0.88,
        "vector_similarity": 0.85,
        "term_similarity": 0.91,
        "positions": [[2, 50, 100, 150, 200]]
      }
    ],
    "doc_aggs": {
      "doc_1": 2
    }
  },
  "message": "success"
}
```

---


# File Management API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 上传文件 (Upload File)

上传文件到系统。

- **URL**: `/file/upload`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (FormData)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file` | file | 是 | 要上传的文件 |
| `parent_id` | string | 否 | 父文件夹 ID (若不传则上传到根目录) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/upload" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -F "file=@/path/to/document.pdf" \
     -F "parent_id=folder_123"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "file_uuid",
      "parent_id": "folder_123",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "document.pdf",
      "location": "document.pdf",
      "size": 1024,
      "type": "pdf",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 12:00:00"
    }
  ],
  "message": "success"
}
```

---


## 2. 创建文件/文件夹 (Create File/Folder)

创建一个新的文件夹或虚拟文件。

- **URL**: `/file/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 文件/文件夹名称 |
| `type` | string | 否 | 类型: `FOLDER` 或 `VIRTUAL` (默认 `VIRTUAL`) |
| `parent_id` | string | 否 | 父文件夹 ID (默认根目录) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "New Folder",
           "type": "FOLDER",
           "parent_id": "root_id"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "folder_uuid",
    "parent_id": "root_id",
    "tenant_id": "tenant_id",
    "created_by": "tenant_id",
    "name": "New Folder",
    "location": "",
    "size": 0,
    "type": "folder",
    "source_type": "",
    "create_time": 1704067200000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1704067200000,
    "update_date": "2024-01-01 12:00:00"
  },
  "message": "success"
}
```

---


## 3. 获取文件列表 (List Files)

列出指定文件夹下的文件。

- **URL**: `/file/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `parent_id` | string | 否 | 文件夹 ID (默认根目录) |
| `keywords` | string | 否 | 搜索关键字 |
| `page` | integer | 否 | 页码 (默认 1) |
| `page_size` | integer | 否 | 每页数量 (默认 15) |
| `orderby` | string | 否 | 排序字段 (默认 `create_time`) |
| `desc` | boolean | 否 | 是否降序 (默认 `true`) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/list?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 50,
    "files": [
      {
        "id": "file_1",
        "parent_id": "folder_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "doc.pdf",
        "location": "doc.pdf",
        "size": 2048,
        "type": "pdf",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 10:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 10:00:00",
        "kbs_info": [
          {
            "kb_id": "kb_id_1",
            "kb_name": "My Dataset",
            "document_id": "doc_id_1"
          }
        ]
      },
      {
        "id": "folder_2",
        "parent_id": "folder_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "subfolder",
        "location": "",
        "size": 4096,
        "type": "folder",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 09:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 09:00:00",
        "kbs_info": [],
        "has_child_folder": true
      }
    ],
    "parent_folder": {
      "id": "folder_id",
      "parent_id": "root_id",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "root",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 08:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 08:00:00"
    }
  },
  "message": "success"
}
```

---


## 4. 获取根目录 (Get Root Folder)

获取用户的根文件夹信息。

- **URL**: `/file/root_folder`
- **Method**: `GET`

### 请求参数
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/root_folder" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "root_folder": {
      "id": "root_id",
      "parent_id": "root_id",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "/",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 00:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 00:00:00"
    }
  },
  "message": "success"
}
```

---


## 5. 获取父文件夹 (Get Parent Folder)

获取指定文件的父文件夹信息。

- **URL**: `/file/parent_folder`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 目标文件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/parent_folder?file_id=file_xxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "parent_folder": {
      "id": "parent_id",
      "parent_id": "root_id",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "Parent Folder",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 00:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 00:00:00"
    }
  },
  "message": "success"
}
```

---


## 6. 获取所有父文件夹 (Get All Parent Folders)

获取文件的所有上级目录（路径）。

- **URL**: `/file/all_parent_folder`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 目标文件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/all_parent_folder?file_id=file_xxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "parent_folders": [
      {
        "id": "file_xxx",
        "parent_id": "folder_level_1",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "current_file.pdf",
        "location": "current_file.pdf",
        "size": 1024,
        "type": "pdf",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 12:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 12:00:00"
      },
      {
        "id": "folder_level_1",
        "parent_id": "root_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "Project A",
        "location": "",
        "size": 0,
        "type": "folder",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 10:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 10:00:00"
      },
      {
        "id": "root_id",
        "parent_id": "root_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "/",
        "location": "",
        "size": 0,
        "type": "folder",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 00:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 00:00:00"
      }
    ]
  },
  "message": "success"
}
```

---


## 7. 删除文件 (Remove Files)

删除一个或多个文件/文件夹。如果删除文件夹，其中的文件也会被删除。

- **URL**: `/file/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_ids` | list[string] | 是 | 要删除的文件 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_ids": ["file_1", "file_2"]
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


## 8. 重命名文件 (Rename File)

重命名文件。

- **URL**: `/file/rename`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 目标文件 ID |
| `name` | string | 是 | 新名称 (扩展名需保持一致) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/rename" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_id": "file_xxx",
           "name": "new_name.pdf"
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


## 9. 下载文件 (Download File)

下载文件内容。

- **URL**: `/file/get/<file_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 文件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/get/file_uuid_xxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     --output my_file.pdf
```

### 响应示例
(返回二进制文件流，响应头包含 `Content-Type` 字段，如 `application/pdf` 或 `image/png`)

---


## 10. 下载附件 (Download Attachment)

下载系统生成的附件。

- **URL**: `/file/download/<attachment_id>`
- **Method**: `GET`

### 请求参数

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `attachment_id` | string | 是 | 附件 ID (URL Path) |
| `ext` | string | 否 | 扩展名/格式 (Query, 默认 `markdown`) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/download/att_uuid?ext=pdf" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
(返回二进制文件流，响应头包含 `Content-Type` 字段，如 `application/pdf` 或 `text/markdown`)

---


## 11. 移动文件 (Move Files)

移动一个或多个文件到另一个文件夹。

- **URL**: `/file/mv`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `src_file_ids` | list[string] | 是 | 源文件 ID 列表 |
| `dest_file_id` | string | 是 | 目标文件夹 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/mv" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "src_file_ids": ["file_1", "file_2"],
           "dest_file_id": "folder_target"
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


## 12. 转换文件 (Convert File)

将文件解析并添加到知识库。

- **URL**: `/file/convert`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_ids` | list[string] | 是 | 目标知识库 ID 列表 |
| `file_ids` | list[string] | 是 | 要转换的文件 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/convert" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_ids": ["kb_1"],
           "file_ids": ["file_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "file2doc_id",
      "file_id": "file_1",
      "document_id": "doc_1",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 12:00:00"
    }
  ],
  "message": "success"
}
```

---


# Session & Chat API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建会话 (Create Session)

为指定的助手 (Assistant/Chat) 创建一个新的会话。

- **URL**: `/chats/<chat_id>/sessions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID (Dialog ID) |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 会话名称 (默认: "New session") |
| `user_id` | string | 否 | 用户标识 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chats/chat_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Chat Session",
           "user_id": "user_abc"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "550e8400e29b41d4a716446655440000",
    "chat_id": "chat_123",
    "name": "My Chat Session",
    "user_id": "user_abc",
    "create_time": 1704067200000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1704067200000,
    "update_date": "2024-01-01 12:00:00",
    "messages": [
      {
        "role": "assistant",
        "content": "Hi! I'm your assistant. What can I do for you?"
      }
    ]
  }
}
```

---


## 2. 创建 Agent 会话 (Create Agent Session)

为指定的 Agent 创建一个新的会话。

- **URL**: `/agents/<agent_id>/sessions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `user_id` | string | 否 | 用户标识 (默认为 tenant_id) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agents/agent_123/sessions?user_id=user_abc" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "550e8400e29b41d4a716446655440001",
    "agent_id": "agent_123",
    "user_id": "user_abc",
    "message": [
      {
        "role": "assistant",
        "content": "Hello! How can I assist you today?"
      }
    ],
    "source": "agent",
    "dsl": {
      "components": {},
      "history": [],
      "path": [],
      "answer": []
    }
  }
}
```

---


## 3. 更新会话 (Update Session)

更新会话信息（如重命名）。

- **URL**: `/chats/<chat_id>/sessions/<session_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |
| `session_id` | string | 是 | 会话 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 新的会话名称 (不能为空) |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/chats/chat_123/sessions/session_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "Renamed Session"
         }'
```

### 响应示例
```json
{
  "code": 0
}
```

---


## 4. 对话补全 (Chat Completion)

与助手进行对话。

- **URL**: `/chats/<chat_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 否 | 用户提问内容 (若 session_id 未提供则为空字符串) |
| `session_id` | string | 否 | 会话 ID (若提供则基于历史上下文) |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `metadata_condition` | object | 否 | 元数据过滤条件 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chats/chat_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAG?",
           "session_id": "session_1",
           "stream": true
         }'
```

### 响应示例 (Stream)
```text
data:{"code": 0, "data": {"answer": "RAG stands for Retrieval-Augmented Generation...", "reference": {"total": 3, "chunks": [{"id": "chunk_1", "content": "...", "document_id": "doc_1", "document_name": "example.pdf", "dataset_id": "kb_1", "image_id": "", "positions": [[1, 100, 200, 300, 400]]}], "doc_aggs": [{"doc_id": "doc_1", "doc_name": "example.pdf", "count": 1}]}, "audio_binary": null, "id": "msg_123", "session_id": "session_1"}}

data:{"code": 0, "data": true}
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "answer": "RAG stands for Retrieval-Augmented Generation...",
    "reference": {
      "total": 3,
      "chunks": [
        {
          "id": "chunk_1",
          "content": "RAG is a technique that combines retrieval and generation...",
          "document_id": "doc_1",
          "document_name": "example.pdf",
          "dataset_id": "kb_1",
          "image_id": "",
          "positions": [[1, 100, 200, 300, 400]]
        }
      ],
      "doc_aggs": [
        {
          "doc_id": "doc_1",
          "doc_name": "example.pdf",
          "count": 1
        }
      ]
    },
    "audio_binary": null,
    "id": "msg_123",
    "session_id": "session_1",
    "prompt": "...",
    "created_at": 1704067200.123
  }
}
```

---


## 5. OpenAI 兼容对话 (Chat Completion OpenAI Compatible)

OpenAI 兼容的对话接口。

- **URL**: `/chats_openai/<chat_id>/chat/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `messages` | list[object] | 是 | 消息列表 (包含 role 和 content) |
| `model` | string | 是 | 模型名称 (占位符，实际由后端配置决定) |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `extra_body` | object | 否 | 额外参数 (如 `reference`: boolean, `metadata_condition`: object) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chats_openai/chat_123/chat/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "model": "gpt-3.5-turbo",
           "messages": [
             {"role": "user", "content": "Hello"}
           ],
           "stream": true
         }'
```

### 响应示例 (Stream)
```text
data:{"id": "chatcmpl-chat_123", "choices": [{"delta": {"content": "Hello", "role": "assistant", "function_call": null, "tool_calls": null, "reasoning_content": null}, "finish_reason": null, "index": 0, "logprobs": null}], "created": 1704067200, "model": "model", "object": "chat.completion.chunk", "system_fingerprint": "", "usage": null}

data:{"id": "chatcmpl-chat_123", "choices": [{"delta": {"content": null, "reasoning_content": null}, "finish_reason": "stop", "index": 0, "logprobs": null}], "created": 1704067200, "model": "model", "object": "chat.completion.chunk", "system_fingerprint": "", "usage": {"prompt_tokens": 5, "completion_tokens": 50, "total_tokens": 55}}

data:[DONE]
```

### 响应示例 (Non-Stream)
```json
{
  "id": "chatcmpl-chat_123",
  "object": "chat.completion",
  "created": 1704067200,
  "model": "gpt-3.5-turbo",
  "usage": {
    "prompt_tokens": 5,
    "completion_tokens": 50,
    "total_tokens": 55,
    "completion_tokens_details": {
      "reasoning_tokens": 100,
      "accepted_prediction_tokens": 50,
      "rejected_prediction_tokens": 0
    }
  },
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "Hello! How can I help you today?"
      },
      "logprobs": null,
      "finish_reason": "stop",
      "index": 0
    }
  ]
}
```

### 响应示例 (Non-Stream with Reference)
```json
{
  "id": "chatcmpl-chat_123",
  "object": "chat.completion",
  "created": 1704067200,
  "model": "gpt-3.5-turbo",
  "usage": {
    "prompt_tokens": 5,
    "completion_tokens": 50,
    "total_tokens": 55,
    "completion_tokens_details": {
      "reasoning_tokens": 100,
      "accepted_prediction_tokens": 50,
      "rejected_prediction_tokens": 0
    }
  },
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "Based on the documents...",
        "reference": [
          {
            "id": "chunk_1",
            "content": "...",
            "document_id": "doc_1",
            "document_name": "example.pdf",
            "dataset_id": "kb_1"
          }
        ]
      },
      "logprobs": null,
      "finish_reason": "stop",
      "index": 0
    }
  ]
}
```

---


## 6. OpenAI 兼容 Agent 对话 (Agent Completion OpenAI Compatible)

OpenAI 兼容的 Agent 对话接口。

- **URL**: `/agents_openai/<agent_id>/chat/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `messages` | list[object] | 是 | 消息列表 |
| `model` | string | 是 | 模型名称 |
| `stream` | boolean | 否 | 是否流式返回 (默认: false, 注意此接口默认值与其他不同) |
| `session_id` | string | 否 | 会话 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agents_openai/agent_123/chat/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "messages": [{"role": "user", "content": "Run analysis"}],
           "model": "agent-model"
         }'
```

### 响应示例 (Non-Stream)
```json
{
  "id": "agent_123",
  "object": "chat.completion",
  "created": 1704067200,
  "model": "agent-model",
  "usage": {
    "prompt_tokens": 10,
    "completion_tokens": 100,
    "total_tokens": 110
  },
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "The analysis results show..."
      },
      "logprobs": null,
      "finish_reason": "stop",
      "index": 0
    }
  ]
}
```

---


## 7. Agent 补全 (Agent Completion)

执行 Agent 对话/任务。

- **URL**: `/agents/<agent_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `return_trace` | boolean | 否 | 是否返回执行轨迹 (默认: false) |
| `...` | any | 否 | 其他传递给 Agent 的参数 (如 inputs, question 等) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agents/agent_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Analyze this data",
           "stream": true,
           "return_trace": true
         }'
```

### 响应示例 (Stream)
```text
data:{"event": "message", "data": {"content": "Analyzing...", "session_id": "session_1"}}

data:{"event": "node_finished", "data": {"component_id": "begin_0", "trace": [{"component_id": "begin_0", "...": "..."}]}}

data:{"event": "message_end", "data": {"content": "Analysis complete.", "reference": {}, "session_id": "session_1"}}

data:[DONE]
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "event": "message_end",
    "data": {
      "content": "The analysis shows that...",
      "reference": {
        "chunks": [
          {
            "id": "chunk_1",
            "content": "...",
            "document_id": "doc_1",
            "document_name": "data.csv",
            "dataset_id": "kb_1"
          }
        ],
        "doc_aggs": [
          {
            "doc_id": "doc_1",
            "doc_name": "data.csv",
            "count": 1
          }
        ]
      },
      "trace": [
        {
          "component_id": "begin_0",
          "trace": [{"component_id": "begin_0"}]
        },
        {
          "component_id": "generate_1",
          "trace": [{"component_id": "generate_1"}]
        }
      ]
    }
  }
}
```

---


## 8. 获取会话列表 (List Sessions)

获取助手的会话列表。

- **URL**: `/chats/<chat_id>/sessions`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `orderby` | string | 否 | 排序字段 (默认: create_time) |
| `desc` | boolean | 否 | 是否降序 (默认: true) |
| `id` | string | 否 | 按会话 ID 过滤 |
| `name` | string | 否 | 按会话名称过滤 |
| `user_id` | string | 否 | 按用户 ID 过滤 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/chats/chat_123/sessions?page=1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "session_1",
      "chat_id": "chat_123",
      "name": "New session",
      "user_id": "user_abc",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 12:00:00",
      "messages": [
        {
          "role": "assistant",
          "content": "Hi! How can I help you?",
          "created_at": 1704067200.0
        },
        {
          "role": "user",
          "content": "What is RAG?",
          "id": "msg_user_1"
        },
        {
          "role": "assistant",
          "content": "RAG stands for...",
          "id": "msg_assistant_1",
          "created_at": 1704067210.0,
          "reference": [
            {
              "id": "chunk_1",
              "content": "...",
              "document_id": "doc_1",
              "document_name": "example.pdf",
              "dataset_id": "kb_1",
              "image_id": "",
              "positions": [[1, 100, 200, 300, 400]]
            }
          ]
        }
      ]
    }
  ]
}
```

---


## 9. 获取 Agent 会话列表 (List Agent Sessions)

获取 Agent 的会话列表。

- **URL**: `/agents/<agent_id>/sessions`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `orderby` | string | 否 | 排序字段 (默认: update_time) |
| `desc` | boolean | 否 | 是否降序 (默认: true) |
| `dsl` | boolean | 否 | 是否包含 DSL (默认: true) |
| `id` | string | 否 | 按 ID 过滤 |
| `user_id` | string | 否 | 按用户 ID 过滤 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/agents/agent_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "session_agent_1",
      "agent_id": "agent_123",
      "user_id": "user_abc",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704153600000,
      "update_date": "2024-01-02 12:00:00",
      "tokens": 1500,
      "source": "agent",
      "duration": 2.5,
      "round": 3,
      "thumb_up": 1,
      "messages": [
        {
          "role": "assistant",
          "content": "Hello! How can I assist you?",
          "created_at": 1704067200.0
        },
        {
          "role": "user",
          "content": "Analyze this data",
          "id": "msg_user_1"
        },
        {
          "role": "assistant",
          "content": "The analysis shows...",
          "id": "msg_assistant_1",
          "created_at": 1704067210.0,
          "reference": [
            {
              "id": "chunk_1",
              "content": "...",
              "document_id": "doc_1",
              "document_name": "data.csv",
              "dataset_id": "kb_1",
              "image_id": "",
              "positions": []
            }
          ]
        }
      ],
      "dsl": {
        "components": {},
        "history": [],
        "path": [],
        "answer": []
      }
    }
  ]
}
```

---


## 10. 删除会话 (Delete Sessions)

删除一个或多个会话。

- **URL**: `/chats/<chat_id>/sessions`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ids` | list[string] | 否 | 要删除的会话 ID 列表 (若为空则删除该 chat 下的全部会话) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/chats/chat_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["session_1"]
         }'
```

### 响应示例 (全部成功)
```json
{
  "code": 0
}
```

### 响应示例 (部分成功)
```json
{
  "code": 0,
  "message": "Partially deleted 2 sessions with 1 errors",
  "data": {
    "success_count": 2,
    "errors": [
      "The chat doesn't own the session session_not_exist"
    ]
  }
}
```

---


## 11. 删除 Agent 会话 (Delete Agent Sessions)

删除一个或多个 Agent 会话。

- **URL**: `/agents/<agent_id>/sessions`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ids` | list[string] | 否 | 要删除的会话 ID 列表 (若为空则删除该 agent 下的全部会话) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/agents/agent_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["session_agent_1"]
         }'
```

### 响应示例 (全部成功)
```json
{
  "code": 0
}
```

### 响应示例 (部分成功)
```json
{
  "code": 0,
  "message": "Partially deleted 2 sessions with 1 errors",
  "data": {
    "success_count": 2,
    "errors": [
      "The agent doesn't own the session session_not_exist"
    ]
  }
}
```

---


## 12. 知识库问答 (Ask KB)

直接针对知识库提问。

- **URL**: `/sessions/ask`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题内容 |
| `dataset_ids` | list[string] | 是 | 知识库 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/sessions/ask" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is in the doc?",
           "dataset_ids": ["kb_1"]
         }'
```

### 响应示例 (Stream)
```text
data:{"code": 0, "message": "", "data": {"answer": "Based on the documents...", "reference": {}}}

data:{"code": 0, "message": "", "data": {"answer": "Based on the documents, the content includes...", "reference": {"total": 2, "chunks": [{"id": "chunk_1", "content": "...", "document_id": "doc_1", "document_name": "example.pdf", "dataset_id": "kb_1"}], "doc_aggs": [{"doc_id": "doc_1", "doc_name": "example.pdf", "count": 1}]}}}

data:{"code": 0, "message": "", "data": true}
```

---


## 13. 相关问题生成 (Related Questions)

根据问题生成相关搜索建议。

- **URL**: `/sessions/related_questions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 原始问题 |
| `industry` | string | 否 | 行业背景 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/sessions/related_questions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Deep learning"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    "What is deep learning?",
    "Deep learning vs machine learning",
    "Deep learning applications",
    "Neural network architectures",
    "How to get started with deep learning"
  ]
}
```

---


## 14. 聊天机器人补全 (Chatbot Completion)

用于嵌入式聊天机器人 (Iframe/External) 的对话接口。

- **URL**: `/chatbots/<dialog_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | 对话 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 用户提问 |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `session_id` | string | 否 | 会话 ID |
| `quote` | boolean | 否 | 是否返回引用 (默认: false) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chatbots/dialog_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Hello"
         }'
```

### 响应示例 (Stream - 新会话)
```text
data:{"code": 0, "message": "", "data": {"answer": "Hi! I'm your assistant. What can I do for you?", "reference": {}, "audio_binary": null, "id": null, "session_id": "550e8400e29b41d4a716446655440000"}}

data:{"code": 0, "message": "", "data": true}
```

### 响应示例 (Stream - 已有会话)
```text
data:{"code": 0, "message": "", "data": {"answer": "Hello! How can I help you today?", "reference": {"chunks": [...], "doc_aggs": [...]}, "audio_binary": null, "id": "msg_123", "session_id": "session_1"}}

data:{"code": 0, "message": "", "data": true}
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "answer": "Hello! How can I help you today?",
    "reference": {
      "chunks": [],
      "doc_aggs": []
    },
    "audio_binary": null,
    "id": "msg_123",
    "session_id": "session_1",
    "prompt": "...",
    "created_at": 1704067200.123
  }
}
```

---


## 15. 获取聊天机器人信息 (Chatbot Info)

获取嵌入式聊天机器人的基本信息。

- **URL**: `/chatbots/<dialog_id>/info`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | 对话 ID |

### 响应示例
```json
{
  "code": 0,
  "data": {
    "title": "Customer Service Bot",
    "avatar": "data:image/png;base64,iVBORw0KGgo...",
    "prologue": "Hi! I'm your assistant. What can I do for you?"
  }
}
```

---


## 16. Agent 机器人补全 (Agentbot Completion)

用于嵌入式 Agent 机器人 (Iframe/External) 的执行接口。

- **URL**: `/agentbots/<agent_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `question` | string | 否 | 用户问题 |
| `session_id` | string | 否 | 会话 ID |
| `...` | any | 否 | Agent 输入参数 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agentbots/agent_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Process this request",
           "stream": true
         }'
```

### 响应示例 (Stream)
```text
data:{"event": "message", "data": {"content": "Processing your request...", "session_id": "session_1"}}

data:{"event": "message", "data": {"content": "Processing your request... Done!", "session_id": "session_1"}}

data:{"event": "message_end", "data": {"content": "Processing your request... Done!", "reference": {}, "session_id": "session_1"}}

data:[DONE]
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "event": "message_end",
    "data": {
      "content": "Request processed successfully.",
      "reference": {},
      "session_id": "session_1"
    }
  }
}
```

---


## 17. 获取 Agent 机器人输入项 (Agentbot Inputs)

获取 Agent 机器人的初始输入表单配置。

- **URL**: `/agentbots/<agent_id>/inputs`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 响应示例
```json
{
  "code": 0,
  "data": {
    "title": "Data Analysis Agent",
    "avatar": "data:image/png;base64,iVBORw0KGgo...",
    "inputs": [
      {
        "key": "file",
        "type": "file",
        "name": "Upload File",
        "required": true
      },
      {
        "key": "query",
        "type": "text",
        "name": "Analysis Query",
        "required": false
      }
    ],
    "prologue": "Welcome! Please upload your data file to begin analysis.",
    "mode": "chat"
  }
}
```

---


## 18. 搜索机器人问答 (Searchbot Ask)

用于搜索机器人 (Searchbot) 的问答接口。

- **URL**: `/searchbots/ask`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `kb_ids` | list[string] | 是 | 知识库 ID 列表 |
| `search_id` | string | 否 | 搜索应用 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/ask" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is machine learning?",
           "kb_ids": ["kb_1", "kb_2"]
         }'
```

### 响应示例 (Stream)
```text
data:{"code": 0, "message": "", "data": {"answer": "Machine learning is...", "reference": {}}}

data:{"code": 0, "message": "", "data": {"answer": "Machine learning is a subset of artificial intelligence...", "reference": {"total": 5, "chunks": [{"id": "chunk_1", "content": "...", "document_id": "doc_1", "document_name": "ml_guide.pdf", "dataset_id": "kb_1"}], "doc_aggs": [{"doc_id": "doc_1", "doc_name": "ml_guide.pdf", "count": 2}]}}}

data:{"code": 0, "message": "", "data": true}
```

---


## 19. 搜索机器人检索测试 (Searchbot Retrieval Test)

搜索机器人的检索测试接口。

- **URL**: `/searchbots/retrieval_test`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `kb_id` | string 或 list[string] | 是 | 知识库 ID (列表) |
| `top_k` | integer | 否 | 返回数量 (默认: 1024) |
| `similarity_threshold` | number | 否 | 相似度阈值 (默认: 0.0) |
| `vector_similarity_weight` | number | 否 | 向量相似度权重 (默认: 0.3) |
| `doc_ids` | list[string] | 否 | 文档 ID 过滤列表 |
| `page` | integer | 否 | 页码 (默认: 1) |
| `size` | integer | 否 | 每页数量 (默认: 30) |
| `rerank_id` | string | 否 | Rerank 模型 ID |
| `use_kg` | boolean | 否 | 是否使用知识图谱 (默认: false) |
| `highlight` | boolean | 否 | 是否高亮显示 |
| `keyword` | boolean | 否 | 是否启用关键词提取 (默认: false) |
| `cross_languages` | list[string] | 否 | 跨语言搜索列表 |
| `search_id` | string | 否 | 搜索应用 ID |
| `meta_data_filter` | object | 否 | 元数据过滤配置 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/retrieval_test" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAG?",
           "kb_id": ["kb_1"],
           "top_k": 10,
           "similarity_threshold": 0.2
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 25,
    "chunks": [
      {
        "chunk_id": "chunk_001",
        "content_with_weight": "RAG (Retrieval-Augmented Generation) is a technique...",
        "content_ltks": "rag retrieval augmented generation technique",
        "doc_id": "doc_1",
        "docnm_kwd": "rag_guide.pdf",
        "kb_id": "kb_1",
        "similarity": 0.89,
        "vector_similarity": 0.85,
        "term_similarity": 0.92,
        "positions": [[1, 50, 100, 200, 150]],
        "image_id": ""
      },
      {
        "chunk_id": "chunk_002",
        "content_with_weight": "RAG combines the power of retrieval...",
        "content_ltks": "rag combines power retrieval",
        "doc_id": "doc_1",
        "docnm_kwd": "rag_guide.pdf",
        "kb_id": "kb_1",
        "similarity": 0.82,
        "vector_similarity": 0.80,
        "term_similarity": 0.84,
        "positions": [[2, 60, 110, 210, 160]],
        "image_id": ""
      }
    ],
    "doc_aggs": [
      {
        "doc_id": "doc_1",
        "doc_name": "rag_guide.pdf",
        "count": 5
      }
    ],
    "labels": ["technology", "ai"]
  }
}
```

---


## 20. 搜索机器人相关问题 (Searchbot Related Questions)

生成搜索机器人的相关推荐问题。

- **URL**: `/searchbots/related_questions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `search_id` | string | 否 | 搜索应用 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/related_questions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAG?"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    "How does RAG work?",
    "RAG vs fine-tuning comparison",
    "Best practices for RAG implementation",
    "RAG architecture overview",
    "Common RAG use cases"
  ]
}
```

---


## 21. 获取搜索机器人详情 (Searchbot Detail)

获取搜索机器人的详细配置。

- **URL**: `/searchbots/detail`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `search_id` | string | 是 | 搜索应用 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/searchbots/detail?search_id=search_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "search_123",
    "name": "Knowledge Search",
    "avatar": "data:image/png;base64,iVBORw0KGgo...",
    "description": "A search application for internal knowledge base",
    "tenant_id": "tenant_1",
    "created_by": "user_1",
    "create_time": 1704067200000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1704153600000,
    "update_date": "2024-01-02 12:00:00",
    "status": "1",
    "search_config": {
      "kb_ids": ["kb_1", "kb_2"],
      "doc_ids": [],
      "similarity_threshold": 0.2,
      "vector_similarity_weight": 0.3,
      "use_kg": false,
      "rerank_id": "",
      "top_k": 1024,
      "summary": true,
      "chat_id": "llm_model_1",
      "llm_setting": {
        "temperature": 0.1,
        "top_p": 0.3
      },
      "cross_languages": [],
      "highlight": true,
      "keyword": false,
      "web_search": false,
      "related_search": true,
      "query_mindmap": false
    }
  }
}
```

---


## 22. 搜索机器人思维导图 (Searchbot Mindmap)

生成搜索结果的思维导图。

- **URL**: `/searchbots/mindmap`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `kb_ids` | list[string] | 是 | 知识库 ID 列表 |
| `search_id` | string | 否 | 搜索应用 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/mindmap" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Explain machine learning concepts",
           "kb_ids": ["kb_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "name": "Machine Learning Concepts",
    "children": [
      {
        "name": "Supervised Learning",
        "children": [
          {"name": "Classification"},
          {"name": "Regression"}
        ]
      },
      {
        "name": "Unsupervised Learning",
        "children": [
          {"name": "Clustering"},
          {"name": "Dimensionality Reduction"}
        ]
      },
      {
        "name": "Reinforcement Learning",
        "children": [
          {"name": "Q-Learning"},
          {"name": "Policy Gradient"}
        ]
      }
    ]
  }
}
```

---


# Search API 文档

**Base URL**: `http://localhost:9380/v1/search`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建搜索应用 (Create Search App)

创建一个新的搜索应用。

- **URL**: `/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 搜索应用名称 (不超过 255 字节) |
| `description` | string | 否 | 描述信息 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/search/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Search App",
           "description": "A search app for internal docs"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "search_id": "a1b2c3d4e5f6789012345678"
  }
}
```

**失败响应 (名称为空):**
```json
{
  "code": 102,
  "message": "Search name can't be empty."
}
```

**失败响应 (名称过长):**
```json
{
  "code": 102,
  "message": "Search name length is 300 which is large than 255."
}
```

---


## 2. 更新搜索应用 (Update Search App)

更新搜索应用的配置、名称等信息。

- **URL**: `/update`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `search_id` | string | 是 | 搜索应用 ID |
| `name` | string | 是 | 搜索应用名称 |
| `search_config` | object | 是 | 搜索配置 (包含 kb_ids, similarity_threshold 等) |
| `tenant_id` | string | 是 | 租户 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/search/update" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "search_id": "search_123456",
           "name": "Updated Search App",
           "tenant_id": "tenant_1",
           "search_config": {
             "kb_ids": ["kb_1", "kb_2"],
             "similarity_threshold": 0.5,
             "vector_similarity_weight": 0.3,
             "top_k": 1024
           }
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "avatar": null,
    "tenant_id": "user_abc123",
    "name": "Updated Search App",
    "description": "A search app for internal docs",
    "created_by": "user_abc123",
    "search_config": {
      "kb_ids": ["kb_1", "kb_2"],
      "doc_ids": [],
      "similarity_threshold": 0.5,
      "vector_similarity_weight": 0.3,
      "use_kg": false,
      "rerank_id": "",
      "top_k": 1024,
      "summary": false,
      "chat_id": "",
      "llm_setting": {},
      "chat_settingcross_languages": [],
      "highlight": false,
      "keyword": false,
      "web_search": false,
      "related_search": false,
      "query_mindmap": false
    },
    "status": "1",
    "create_time": 1700000000000,
    "create_date": "2023-11-14 22:13:20",
    "update_time": 1700000000000,
    "update_date": "2023-11-14 22:13:20"
  }
}
```

**失败响应 (无权限):**
```json
{
  "code": 109,
  "message": "No authorization.",
  "data": false
}
```

**失败响应 (找不到搜索应用):**
```json
{
  "code": 102,
  "message": "Cannot find search a1b2c3d4e5f6789012345678"
}
```

**失败响应 (名称重复):**
```json
{
  "code": 102,
  "message": "Duplicated search name."
}
```

---


## 3. 获取搜索应用详情 (Get Search App Detail)

获取指定搜索应用的详细信息。

- **URL**: `/detail`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `search_id` | string | 是 | 搜索应用 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/search/detail?search_id=search_123456" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "avatar": null,
    "tenant_id": "user_abc123",
    "name": "My Search App",
    "description": "A search app for internal docs",
    "created_by": "user_abc123",
    "search_config": {
      "kb_ids": ["kb_1"],
      "doc_ids": [],
      "similarity_threshold": 0.2,
      "vector_similarity_weight": 0.3,
      "use_kg": false,
      "rerank_id": "",
      "top_k": 1024,
      "summary": false,
      "chat_id": "",
      "llm_setting": {},
      "chat_settingcross_languages": [],
      "highlight": false,
      "keyword": false,
      "web_search": false,
      "related_search": false,
      "query_mindmap": false
    },
    "update_time": 1700000000000,
    "nickname": "Admin",
    "tenant_avatar": null
  }
}
```

**失败响应 (无权限):**
```json
{
  "code": 103,
  "message": "Has no permission for this operation.",
  "data": false
}
```

**失败响应 (找不到搜索应用):**
```json
{
  "code": 102,
  "message": "Can't find this Search App!"
}
```

---


## 4. 获取搜索应用列表 (List Search Apps)

获取搜索应用列表，支持分页和关键词搜索。

- **URL**: `/list`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `keywords` | string | 否 | 搜索关键词 |
| `page` | integer | 否 | 页码 (默认 0, 表示不分页或第一页) |
| `page_size` | integer | 否 | 每页数量 (默认 0) |
| `orderby` | string | 否 | 排序字段 (默认 "create_time") |
| `desc` | boolean | 否 | 是否降序 (默认 true) |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `owner_ids` | list[string] | 否 | 指定 Tenant ID 列表进行筛选 (若不传则查询当前用户权限下的) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/search/list?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "owner_ids": []
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "search_apps": [
      {
        "id": "a1b2c3d4e5f6789012345678",
        "avatar": null,
        "tenant_id": "user_abc123",
        "name": "My Search App",
        "description": "A search app for internal docs",
        "created_by": "user_abc123",
        "status": "1",
        "update_time": 1700000000000,
        "create_time": 1700000000000,
        "nickname": "Admin",
        "tenant_avatar": null
      }
    ],
    "total": 1
  }
}
```

---


## 5. 删除搜索应用 (Delete Search App)

删除指定的搜索应用。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `search_id` | string | 是 | 要删除的搜索应用 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/search/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "search_id": "search_123456"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": true
}
```

**失败响应 (无权限):**
```json
{
  "code": 109,
  "message": "No authorization.",
  "data": false
}
```

**失败响应 (删除失败):**
```json
{
  "code": 102,
  "message": "Failed to delete search App a1b2c3d4e5f6789012345678"
}
```

---


# System API 文档

**Base URL**: `http://localhost:9380/v1/system`

**Authentication**:
大部分接口需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取版本信息 (Version)

获取当前应用程序的版本号。

- **URL**: `/version`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/version" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": "v0.18.0"
}
```

---


## 2. 获取系统状态 (Status)

获取系统各个组件 (ES, Storage, Database, Redis) 的运行状态。

- **URL**: `/status`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/status" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "doc_engine": {
      "type": "elasticsearch",
      "status": "green",
      "elapsed": "10.5"
    },
    "storage": {
      "storage": "minio",
      "status": "green",
      "elapsed": "5.2"
    },
    "database": {
      "database": "mysql",
      "status": "green",
      "elapsed": "2.1"
    },
    "redis": {
      "status": "green",
      "elapsed": "1.0"
    },
    "task_executor_heartbeats": {
      "task_executor_1": []
    }
  }
}
```

**部分失败响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "doc_engine": {
      "type": "unknown",
      "status": "red",
      "elapsed": "50.0",
      "error": "Connection refused"
    },
    "storage": {
      "storage": "minio",
      "status": "green",
      "elapsed": "5.2"
    },
    "database": {
      "database": "mysql",
      "status": "green",
      "elapsed": "2.1"
    },
    "redis": {
      "status": "red",
      "elapsed": "1.0",
      "error": "Lost connection!"
    },
    "task_executor_heartbeats": {}
  }
}
```

---


## 3. 健康检查 (Healthz)

用于容器或负载均衡器的健康检查。

- **URL**: `/healthz`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/healthz"
```

### 响应示例

**成功响应 (HTTP 200):**
```json
{
  "status": "ok",
  "db": "ok",
  "redis": "ok",
  "doc_engine": "ok",
  "storage": "ok"
}
```

**失败响应 (HTTP 500):**
```json
{
  "status": "nok",
  "db": "ok",
  "redis": "nok",
  "doc_engine": "ok",
  "storage": "ok",
  "_meta": {
    "redis": {
      "elapsed": "1.0",
      "error": "Connection refused"
    }
  }
}
```

---


## 4. Ping

简单的连通性测试。

- **URL**: `/ping`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/ping"
```

### 响应示例
```text
pong
```

---


## 5. 创建新 Token (New Token)

生成一个新的 API Token。

- **URL**: `/new_token`
- **Method**: `POST`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | Token 名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/system/new_token?name=my_token" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "tenant_id": "abc123def456",
    "token": "ragflow-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "beta": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "create_time": 1700000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": null,
    "update_date": null
  }
}
```

**失败响应 (Tenant 不存在):**
```json
{
  "code": 102,
  "message": "Tenant not found!"
}
```

---


## 6. 获取 Token 列表 (Token List)

列出当前用户的所有 API Token。

- **URL**: `/token_list`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/token_list" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": [
    {
      "tenant_id": "abc123def456",
      "token": "ragflow-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
      "beta": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
      "create_time": 1700000000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1700001000,
      "update_date": "2024-01-01 12:16:40",
      "dialog_id": null,
      "source": null
    }
  ]
}
```

**失败响应 (Tenant 不存在):**
```json
{
  "code": 102,
  "message": "Tenant not found!"
}
```

---


## 7. 删除 Token (Remove Token)

删除指定的 API Token。

- **URL**: `/token/<token>`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `token` | string | 是 | 要删除的 API Token |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/system/token/ragflow-xxxxxxxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": true
}
```

**失败响应 (Tenant 不存在):**
```json
{
  "code": 102,
  "message": "Tenant not found!"
}
```

---


## 8. 获取系统配置 (Config)

获取系统配置信息（如是否开启注册）。

- **URL**: `/config`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/config"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "registerEnabled": true
  }
}
```


---


# Tenant API 文档

**Base URL**: `http://localhost:9380/v1/tenant`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取租户用户列表 (User List)

获取指定租户下的用户列表。

- **URL**: `/<tenant_id>/user/list`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tenant_id` | string | 是 | Tenant ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/tenant/tenant_1/user/list" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "data": [
    {
      "id": "5c99a15c4e6011efb0c80242ac120006",
      "user_id": "a1b2c3d4e5f6",
      "status": "1",
      "role": "normal",
      "nickname": "Alice",
      "email": "alice@example.com",
      "avatar": "base64_string...",
      "is_authenticated": "1",
      "is_active": "1",
      "is_anonymous": "0",
      "update_date": "2024-01-01 12:00:00",
      "is_superuser": false,
      "delta_seconds": 120
    }
  ],
  "message": "success"
}
```

**失败响应 (无权限):**
```json
{
  "code": 109,
  "data": false,
  "message": "No authorization."
}
```

---


## 2. 邀请用户 (Invite User)

邀请用户加入租户。

- **URL**: `/<tenant_id>/user`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tenant_id` | string | 是 | Tenant ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 被邀请人的邮箱 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/tenant/tenant_1/user" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "email": "bob@example.com"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "data": {
    "id": "b2c3d4e5f6a7",
    "avatar": "base64_string...",
    "email": "bob@example.com",
    "nickname": "Bob"
  },
  "message": "success"
}
```

**失败响应 (无权限):**
```json
{
  "code": 109,
  "data": false,
  "message": "No authorization."
}
```

**失败响应 (用户不存在):**
```json
{
  "code": 102,
  "data": false,
  "message": "User not found."
}
```

**失败响应 (用户已在团队中):**
```json
{
  "code": 102,
  "data": false,
  "message": "bob@example.com is already in the team."
}
```

**失败响应 (邀请邮件发送失败):**
```json
{
  "code": 100,
  "data": false,
  "message": "Failed to send invite email."
}
```

---


## 3. 移除用户 (Remove User)

移除租户下的用户。

- **URL**: `/<tenant_id>/user/<user_id>`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tenant_id` | string | 是 | Tenant ID |
| `user_id` | string | 是 | User ID |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/tenant/tenant_1/user/user_2" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

**失败响应 (无权限):**
```json
{
  "code": 109,
  "data": false,
  "message": "No authorization."
}
```

---


## 4. 获取租户列表 (Tenant List)

获取当前用户所属的租户列表。

- **URL**: `/list`
- **Method**: `GET`

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/tenant/list" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "data": [
    {
      "tenant_id": "a1b2c3d4e5f6",
      "role": "normal",
      "nickname": "Team Owner",
      "email": "owner@example.com",
      "avatar": "base64_string...",
      "update_date": "2024-01-01 12:00:00",
      "delta_seconds": 3600
    }
  ],
  "message": "success"
}
```

---


## 5. 同意加入 (Agree Join)

同意加入租户。

- **URL**: `/agree/<tenant_id>`
- **Method**: `PUT`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tenant_id` | string | 是 | Tenant ID |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/tenant/agree/tenant_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

---


# User API 文档

**Base URL**: `http://localhost:9380/v1/user`

**Authentication**:
部分接口需要认证 (See `login_required` in code)。请在 Header 中携带 API Key 或 Session Token：
`Authorization: Bearer <YOUR_ACCESS_TOKEN>`

## 1. 用户登录 (Login)

用户登录接口。

- **URL**: `/login`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 用户邮箱 |
| `password` | string | 是 | 用户密码 (加密后) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/login" \
     -H "Content-Type: application/json" \
     -d '{
           "email": "user@example.com",
           "password": "encrypted_password_xxx"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "Welcome back!",
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "access_token": "f6g7h8i9j0k1l2m3n4o5p6a1b2c3d4e5",
    "nickname": "User Nickname",
    "email": "user@example.com",
    "avatar": "base64_encoded_avatar_string...",
    "language": "English",
    "color_schema": "Bright",
    "timezone": "UTC+8\tAsia/Shanghai",
    "last_login_time": "2024-01-15 10:30:00",
    "is_authenticated": "1",
    "is_active": "1",
    "is_anonymous": "0",
    "login_channel": "password",
    "status": "1",
    "is_superuser": false,
    "create_time": 1700000000000,
    "create_date": "2024-01-01 00:00:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-15 10:30:00"
  }
}
```

**失败响应 (用户未注册):**
```json
{
  "code": 109,
  "message": "Email: user@example.com is not registered!",
  "data": false
}
```

**失败响应 (密码错误):**
```json
{
  "code": 109,
  "message": "Email and password do not match!",
  "data": false
}
```

**失败响应 (账号被禁用):**
```json
{
  "code": 110,
  "message": "This account has been disabled, please contact the administrator!",
  "data": false
}
```

---


## 2. 获取登录渠道 (Login Channels)

获取所有支持的认证渠道。

- **URL**: `/login/channels`
- **Method**: `GET`

### 请求参数 (Query)

无参数。

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/user/login/channels"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": [
    {
      "channel": "github",
      "display_name": "GitHub",
      "icon": "github"
    },
    {
      "channel": "feishu",
      "display_name": "Feishu",
      "icon": "sso"
    }
  ]
}
```

**失败响应:**
```json
{
  "code": 500,
  "message": "Load channels failure, error: ...",
  "data": []
}
```

---


## 3. OAuth 登录 (OAuth Login)

重定向到指定渠道的 OAuth 登录页面。

- **URL**: `/login/<channel>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `channel` | string | 是 | 登录渠道 (e.g., github, feishu) |

### 请求示例
```bash
# 在浏览器中访问
http://localhost:9380/v1/user/login/github
```

### 响应示例
Redirect to OAuth provider authorization URL.

---


## 4. OAuth 回调 (OAuth Callback)

处理 OAuth/OIDC 回调。

- **URL**: `/oauth/callback/<channel>`
- **Method**: `GET`

### 请求参数 (Path/Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `channel` | string | 是 | 登录渠道 (Path 参数) |
| `code` | string | 是 | OAuth 授权码 (Query 参数) |
| `state` | string | 是 | OAuth State (Query 参数) |

### 请求示例
```bash
# 回调 URL 示例
http://localhost:9380/v1/user/oauth/callback/github?code=xyz&state=abc
```

### 响应示例
Redirect to frontend:
- 成功: `/?auth=<user_auth_token>`
- 失败: `/?error=<error_message>`

---


## 5. GitHub 回调 (GitHub Callback - Deprecated)

**Deprecated**: 请使用 `/oauth/callback/<channel>`。

- **URL**: `/github_callback`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `code` | string | 是 | GitHub 授权码 |

### 请求示例
```bash
http://localhost:9380/v1/user/github_callback?code=xyz
```

### 响应示例
Redirect to frontend:
- 成功: `/?auth=<user_auth_token>`
- 失败: `/?error=<error_message>`

---


## 6. 飞书回调 (Feishu Callback)

飞书 OAuth 回调。

- **URL**: `/feishu_callback`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `code` | string | 是 | 飞书授权码 |

### 请求示例
```bash
http://localhost:9380/v1/user/feishu_callback?code=xyz
```

### 响应示例
Redirect to frontend:
- 成功: `/?auth=<user_auth_token>`
- 失败: `/?error=<error_message>`

---


## 7. 登出 (Logout)

用户登出。

- **URL**: `/logout`
- **Method**: `GET`
- **Authentication**: Required

### 请求参数

无参数。

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/user/logout" \
     -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": true
}
```

---


## 8. 更新设置 (Update Settings)

更新用户信息 (昵称, 密码等)。

- **URL**: `/setting`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: Required

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `nickname` | string | 否 | 新昵称 |
| `avatar` | string | 否 | 头像 (base64 编码) |
| `language` | string | 否 | 语言设置 (English/Chinese) |
| `color_schema` | string | 否 | 颜色主题 (Bright/Dark) |
| `timezone` | string | 否 | 时区设置 |
| `password` | string | 否 | 当前密码 (若修改密码则必填, 加密) |
| `new_password` | string | 否 | 新密码 (加密) |

**注意**: 以下字段不可修改: `email`, `status`, `is_superuser`, `login_channel`, `is_anonymous`, `is_active`, `is_authenticated`, `last_login_time`

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/setting" \
     -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "nickname": "New Name"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": true
}
```

**失败响应 (密码错误):**
```json
{
  "code": 109,
  "message": "Password error!",
  "data": false
}
```

**失败响应 (更新失败):**
```json
{
  "code": 500,
  "message": "Update failure!",
  "data": false
}
```

---


## 9. 获取用户信息 (User Profile)

获取当前用户信息。

- **URL**: `/info`
- **Method**: `GET`
- **Authentication**: Required

### 请求参数

无参数。

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/user/info" \
     -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "access_token": "f6g7h8i9j0k1l2m3n4o5p6a1b2c3d4e5",
    "nickname": "User Nickname",
    "email": "user@example.com",
    "avatar": "base64_encoded_avatar_string...",
    "language": "English",
    "color_schema": "Bright",
    "timezone": "UTC+8\tAsia/Shanghai",
    "last_login_time": "2024-01-15 10:30:00",
    "is_authenticated": "1",
    "is_active": "1",
    "is_anonymous": "0",
    "login_channel": "password",
    "status": "1",
    "is_superuser": false,
    "create_time": 1700000000000,
    "create_date": "2024-01-01 00:00:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-15 10:30:00"
  }
}
```

---


## 10. 用户注册 (Register)

注册新用户。

- **URL**: `/register`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `nickname` | string | 是 | 昵称 |
| `email` | string | 是 | 邮箱 |
| `password` | string | 是 | 密码 (加密) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/register" \
     -H "Content-Type: application/json" \
     -d '{
           "nickname": "NewUser",
           "email": "new@example.com",
           "password": "encrypted_password_xxx"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "NewUser, welcome aboard!",
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "access_token": "f6g7h8i9j0k1l2m3n4o5p6a1b2c3d4e5",
    "nickname": "NewUser",
    "email": "new@example.com",
    "avatar": null,
    "language": "English",
    "color_schema": "Bright",
    "timezone": "UTC+8\tAsia/Shanghai",
    "last_login_time": "2024-01-15 10:30:00",
    "is_authenticated": "1",
    "is_active": "1",
    "is_anonymous": "0",
    "login_channel": "password",
    "status": "1",
    "is_superuser": false,
    "create_time": 1700000000000,
    "create_date": "2024-01-15 10:30:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-15 10:30:00"
  }
}
```

**失败响应 (注册已禁用):**
```json
{
  "code": 103,
  "message": "User registration is disabled!",
  "data": false
}
```

**失败响应 (邮箱格式无效):**
```json
{
  "code": 103,
  "message": "Invalid email address: invalid_email!",
  "data": false
}
```

**失败响应 (邮箱已注册):**
```json
{
  "code": 103,
  "message": "Email: new@example.com has already registered!",
  "data": false
}
```

**失败响应 (注册失败):**
```json
{
  "code": 500,
  "message": "User registration failure, error: ...",
  "data": false
}
```

---


## 11. 获取租户信息 (Tenant Info)

获取当前用户的租户信息。

- **URL**: `/tenant_info`
- **Method**: `GET`
- **Authentication**: Required

### 请求参数

无参数。

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/user/tenant_info" \
     -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "tenant_id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "name": "User's Kingdom",
    "llm_id": "deepseek-chat@DeepSeek",
    "embd_id": "BAAI/bge-large-zh-v1.5@Xinference",
    "rerank_id": "BAAI/bge-reranker-v2-m3@Xinference",
    "asr_id": "whisper-1@OpenAI",
    "img2txt_id": "gpt-4o@OpenAI",
    "tts_id": null,
    "parser_ids": "naive,qa,resume,manual,table,paper,book,laws,presentation,one,knowledge_graph,email,picture,tag",
    "role": "owner"
  }
}
```

**失败响应 (租户不存在):**
```json
{
  "code": 101,
  "message": "Tenant not found!",
  "data": null
}
```

---


## 12. 设置租户信息 (Set Tenant Info)

更新租户的模型配置。

- **URL**: `/set_tenant_info`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: Required

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tenant_id` | string | 是 | 租户 ID |
| `llm_id` | string | 是 | LLM ID |
| `embd_id` | string | 是 | Embedding Model ID |
| `asr_id` | string | 是 | ASR Model ID |
| `img2txt_id` | string | 是 | Image2Text Model ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/set_tenant_info" \
     -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "tenant_id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
           "llm_id": "gpt-4@OpenAI",
           "embd_id": "text-embedding-3-small@OpenAI",
           "asr_id": "whisper-1@OpenAI",
           "img2txt_id": "gpt-4o@OpenAI"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": true
}
```

**失败响应:**
```json
{
  "code": 500,
  "message": "Exception error message...",
  "data": null
}
```

---


## 13. 获取验证码 (Forget Password - Captcha)

获取重置密码用的图片验证码。

- **URL**: `/forget/captcha`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 用户邮箱 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/user/forget/captcha?email=user@example.com"
```

### 响应示例

**成功响应:**
Returns binary image data (JPEG, Content-Type: image/JPEG).

**失败响应 (缺少邮箱):**
```json
{
  "code": 102,
  "message": "email is required",
  "data": false
}
```

**失败响应 (邮箱无效):**
```json
{
  "code": 101,
  "message": "invalid email",
  "data": false
}
```

---


## 14. 发送 OTP (Forget Password - Send OTP)

验证图片验证码并发送邮件 OTP。

- **URL**: `/forget/otp`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 用户邮箱 |
| `captcha` | string | 是 | 图片验证码内容 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/forget/otp" \
     -H "Content-Type: application/json" \
     -d '{
           "email": "user@example.com",
           "captcha": "AB12CD"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "verification passed, email sent",
  "data": true
}
```

**失败响应 (缺少参数):**
```json
{
  "code": 102,
  "message": "email and captcha required",
  "data": false
}
```

**失败响应 (邮箱无效):**
```json
{
  "code": 101,
  "message": "invalid email",
  "data": false
}
```

**失败响应 (验证码无效或过期):**
```json
{
  "code": 104,
  "message": "invalid or expired captcha",
  "data": false
}
```

**失败响应 (验证码错误):**
```json
{
  "code": 109,
  "message": "invalid or expired captcha",
  "data": false
}
```

**失败响应 (冷却时间):**
```json
{
  "code": 104,
  "message": "you still have to wait 45 seconds",
  "data": false
}
```

**失败响应 (发送失败):**
```json
{
  "code": 100,
  "message": "failed to send email",
  "data": false
}
```

---


## 15. 验证 OTP (Forget Password - Verify OTP)

验证邮件 OTP。

- **URL**: `/forget/verify-otp`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 用户邮箱 |
| `otp` | string | 是 | 邮件 OTP |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/forget/verify-otp" \
     -H "Content-Type: application/json" \
     -d '{
           "email": "user@example.com",
           "otp": "ABCDEF"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "otp verified",
  "data": true
}
```

**失败响应 (缺少参数):**
```json
{
  "code": 102,
  "message": "email and otp are required",
  "data": false
}
```

**失败响应 (邮箱无效):**
```json
{
  "code": 101,
  "message": "invalid email",
  "data": false
}
```

**失败响应 (尝试次数过多):**
```json
{
  "code": 104,
  "message": "too many attempts, try later",
  "data": false
}
```

**失败响应 (OTP 过期):**
```json
{
  "code": 104,
  "message": "expired otp",
  "data": false
}
```

**失败响应 (OTP 错误):**
```json
{
  "code": 109,
  "message": "expired otp",
  "data": false
}
```

**失败响应 (存储错误):**
```json
{
  "code": 500,
  "message": "otp storage corrupted",
  "data": false
}
```

---


## 16. 重置密码 (Forget Password - Reset Password)

验证 OTP 通过后重置密码。

- **URL**: `/forget/reset-password`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 用户邮箱 |
| `new_password` | string | 是 | 新密码 (加密) |
| `confirm_new_password` | string | 是 | 确认新密码 (加密) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/forget/reset-password" \
     -H "Content-Type: application/json" \
     -d '{
           "email": "user@example.com",
           "new_password": "encrypted_new_pwd",
           "confirm_new_password": "encrypted_new_pwd"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "Password reset successful. Logged in.",
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "access_token": "f6g7h8i9j0k1l2m3n4o5p6a1b2c3d4e5",
    "nickname": "User Nickname",
    "email": "user@example.com",
    "avatar": "base64_encoded_avatar_string...",
    "language": "English",
    "color_schema": "Bright",
    "timezone": "UTC+8\tAsia/Shanghai",
    "last_login_time": "2024-01-15 10:30:00",
    "is_authenticated": "1",
    "is_active": "1",
    "is_anonymous": "0",
    "login_channel": "password",
    "status": "1",
    "is_superuser": false,
    "create_time": 1700000000000,
    "create_date": "2024-01-01 00:00:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-15 10:30:00"
  }
}
```

**失败响应 (邮箱未验证):**
```json
{
  "code": 109,
  "message": "email not verified",
  "data": false
}
```

**失败响应 (缺少参数):**
```json
{
  "code": 102,
  "message": "email and passwords are required",
  "data": false
}
```

**失败响应 (密码不匹配):**
```json
{
  "code": 102,
  "message": "passwords do not match",
  "data": false
}
```

**失败响应 (邮箱无效):**
```json
{
  "code": 101,
  "message": "invalid email",
  "data": false
}
```

**失败响应 (重置失败):**
```json
{
  "code": 500,
  "message": "failed to reset password",
  "data": false
}
```


