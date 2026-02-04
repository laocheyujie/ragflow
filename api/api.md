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
      "dsl": { ... },
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
             "components": { ... },
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
        "data": { ... }
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
    "id": "kb_123456",
    "name": "My Knowledge Base",
    "avatar": "",
    "description": "",
    "permission": "me",
    "embd_id": "BAAI/bge-large-zh-v1.5",
    "parser_id": "naive",
    "parser_config": { ... },
    "create_time": 1700000000,
    "create_date": "2024-01-01 12:00:00"
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

### 响应示例
```json
{
  "code": 0,
  "data": {
    "success_count": 2,
    "errors": []
  },
  "message": "Successfully deleted 2 datasets, 0 failed. Details: ..."
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
    "id": "kb_123",
    "name": "My Knowledge Base",
    "description": "Updated description",
    ...
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
      "id": "kb_1",
      "name": "Dataset 1",
      "create_time": 1700000000
    },
    ...
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
      "nodes": [...],
      "edges": [...]
    },
    "mind_map": { ... }
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
    "graphrag_task_id": "task_abc123"
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
    "id": "task_abc123",
    "progress": 0.5,
    "status": "running"
  },
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
    "raptor_task_id": "task_xyz789"
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
    "id": "task_xyz789",
    "progress": 1.0,
    "status": "success"
  },
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
```json
{
  "records": [
    {
      "content": "RAGFlow is an open-source RAG engine...",
      "score": 0.89,
      "title": "RAGFlow Introduction",
      "metadata": {
        "doc_id": "doc_1",
        "author": "admin",
        "source": "manual"
      }
    }
  ]
}
```

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
      "chunk_count": 0,
      "token_count": 0,
      "dataset_id": "dataset_123",
      "chunk_method": "naive",
      "run": "UNSTART"
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
      "run": "DONE",
      "status": "1"
      // ... 其他文档字段
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
(文件流)

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
        "chunk_count": 50,
        "token_count": 5000,
        "run": "DONE",
        "create_time": "2024-01-01 12:00:00"
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
       // 元数据统计信息
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
        "important_keywords": ["keyword1"],
        "dataset_id": "dataset_123"
      }
    ],
    "doc": {
        "id": "doc_1",
        "name": "doc.pdf"
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
           "important_keywords": ["new", "chunk"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "chunk": {
      "id": "generated_chunk_id",
      "content": "New chunk content",
      // ...
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
| `chunk_ids` | list[string] | 否 | 要删除的 Chunk ID 列表 (若空则根据 API 逻辑可能删除全部或报错，具体视实现而定，建议明确指定) |

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
| `available` | boolean | 否 | 是否启用 (1/0 or true/false) |
| `positions` | list[string] | 否 | 位置信息 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/datasets/dataset_123/documents/doc_1/chunks/chunk_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "content": "Updated content"
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
| `similarity_threshold` | number | 否 | 相似度阈值 (默认: 0.2) |
| `vector_similarity_weight` | number | 否 | 向量相似度权重 (默认: 0.3) |
| `top_k` | integer | 否 | 返回数量 (默认: 1024) |
| `highlight` | boolean | 否 | 是否高亮匹配内容 |
| `rerank_id` | string | 否 | 重排模型 ID |
| `keyword` | boolean | 否 | 是否进行关键词增强 |
| `cross_languages` | list[string] | 否 | 跨语言搜索配置 |
| `use_kg` | boolean | 否 | 是否使用知识图谱 |
| `metadata_condition` | object | 否 | 元数据过滤条件 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/retrieval" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "dataset_ids": ["dataset_123"],
           "question": "what is ragflow?",
           "top_k": 5
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "chunks": [
      {
        "id": "chunk_1",
        "content": "RAGFlow is ...",
        "similarity": 0.95,
        "document_id": "doc_1",
        "dataset_id": "dataset_123"
      }
    ]
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
      "name": "document.pdf",
      "size": 1024,
      "type": "pdf",
      "location": "document.pdf",
      "created_by": "tenant_id",
      "create_time": "2024-01-01 12:00:00"
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
    "name": "New Folder",
    "type": "FOLDER",
    "size": 0,
    "location": ""
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
        "name": "doc.pdf",
        "type": "pdf",
        "size": 2048,
        "create_time": "2024-01-01 10:00:00"
      }
    ],
    "parent_folder": {
      "id": "folder_id",
      "name": "root"
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
      "name": "root",
      "type": "FOLDER"
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
      "name": "Parent Name"
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
        "id": "root_id",
        "name": "root"
      },
      {
        "id": "folder_level_1",
        "name": "Project A"
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
(返回二进制文件流)

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
(返回二进制文件流)

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
      "document_id": "doc_1"
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
    "id": "session_1",
    "chat_id": "chat_123",
    "name": "My Chat Session",
    "create_time": "2024-01-01 12:00:00",
    "messages": [
      {
        "role": "assistant",
        "content": "Hello! How can I help you?"
      }
    ]
  },
  "message": "success"
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
    "id": "session_agent_1",
    "agent_id": "agent_123",
    "user_id": "user_abc",
    "messages": [{"role": "assistant", "content": "..."}],
    "source": "agent",
    "dsl": {...}
  },
  "message": "success"
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
  "code": 0,
  "data": null,
  "message": "success"
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
data:{"code": 0, "message": "", "data": {"answer": "RAG stands for...", "reference": [...]}}

data:{"code": 0, "message": "", "data": true}
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
      "answer": "RAG stands for...",
      "reference": [...]
  },
  "message": "success"
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

### 响应示例
(符合 OpenAI Chat Completion Chunk 格式)

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
           "stream": true
         }'
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
      "name": "New session",
      "create_time": "..."
    }
  ],
  "message": "success"
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
| `ids` | list[string] | 否 | 要删除的会话 ID 列表 (若为空则可能删除全部，具体视实现而定) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/chats/chat_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["session_1"]
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
| `ids` | list[string] | 否 | 要删除的会话 ID 列表 |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/agents/agent_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["session_agent_1"]
         }'
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

### 响应示例
(Stream 格式返回答案)

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
    "Neural Networks",
    "Backpropagation",
    "CNN vs RNN"
  ],
  "message": "success"
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

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chatbots/dialog_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Hello"
         }'
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
    "title": "Bot Name",
    "avatar": "...",
    "prologue": "Welcome!"
  },
  "message": "success"
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
| `...` | any | 否 | Agent 输入参数 |

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
    "title": "Agent Name",
    "inputs": {...},
    "prologue": "..."
  },
  "message": "success"
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
| `kb_id` | list[string] | 是 | 知识库 ID 列表 |
| `top_k` | integer | 否 | 返回数量 |
| `similarity_threshold` | number | 否 | 相似度阈值 |

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

---

## 21. 获取搜索机器人详情 (Searchbot Detail)

获取搜索机器人的详细配置。

- **URL**: `/searchbots/detail`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `search_id` | string | 是 | 搜索应用 ID |

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
    "token": "generated_token_xxx",
    "dialog_id": "dialog_123",
    "create_time": 1700000000,
    "create_date": "2024-01-01 12:00:00"
  },
  "message": "success"
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
      "token": "token_1",
      "dialog_id": "dialog_123"
    }
  ],
  "message": "success"
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
  "data": true,
  "message": "success"
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
    "pv": [["2024-01-01 00:00:00", 10]],
    "uv": [["2024-01-01 00:00:00", 5]],
    "speed": [["2024-01-01 00:00:00", 15.5]],
    "tokens": [["2024-01-01 00:00:00", 1.2]],
    "round": [["2024-01-01 00:00:00", 20]],
    "thumb_up": [["2024-01-01 00:00:00", 2]]
  },
  "message": "success"
}
```

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
      "title": "Translation Agent",
      "description": "A template for translation tasks.",
      "dsl": "..."
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
           "dsl": {...}
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "generated_canvas_id",
    "title": "My New Agent",
    "dsl": {...},
    "user_id": "user_1"
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
    "title": "My Agent",
    "dsl": {...},
    "create_time": "..."
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
    "title": "My Agent",
    "dsl": {...}
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
```
data: {"content": "Thinking...", "node_id": "step_1"}

data: {"content": "Hello! How can I help you?", "node_id": "step_2"}
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
| `id` | string | 是 | Canvas ID |
| `dsl` | object | 是 | 画布 DSL |
| `component_id` | string | 是 | 需要重跑的组件 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/rerun" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "component_id": "component_abc",
           "dsl": {...}
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
  "data": {...}, // 重置后的 DSL
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
    "file_id": "file_123",
    "name": "file.pdf"
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
  "data": {
    "form": [
      {"name": "field1", "type": "text"}
    ]
  },
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
| `params` | object | 是 | 调试参数 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/debug" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "component_id": "llm_component",
           "params": {"prompt": "Hello"}
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "content": "Result from LLM",
    "usage": {...}
  },
  "message": "success"
}
```

---

## 13. 测试数据库连接 (Test DB Connect)

测试各种数据库连接 (MySQL, Postgres, MSSQL, Trino, etc.)。

- **URL**: `/test_db_connect`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `db_type` | string | 是 | 数据库类型 (mysql, postgres, mssql, trino 等) |
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
    {"id": "v1", "title": "ver_1", "update_time": ...},
    {"id": "v2", "title": "ver_2", "update_time": ...}
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
    "dsl": {...},
    "create_time": ...
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
| `canvas_category` | string | 否 | 类别筛选 |
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
    "canvas": [...],
    "total": 100
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
| `permission` | string | 是 | 权限设置 |
| `description` | string | 否 | 描述 |
| `avatar` | string | 否 | 头像 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/setting" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "title": "New Title",
           "permission": "public"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": 1, // 更新行数
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
  "data": { ... }, // 详细日志结构
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
    "sessions": [...]
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
    "task_analysis": "...",
    "plan_generation": "..."
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
| `id` | string | 是 | 文件 ID |
| `created_by` | string | 是 | 创建者 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/download?id=file_1&created_by=user_1" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
(二进制文件流)

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
      "name": "example.pdf"
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
    "docnm_kwd": "example.pdf",
    "available_int": 1
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

### 响应示例
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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

### 响应示例
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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

### 响应示例
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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

### 响应示例
```json
{
  "code": 0,
  "data": {
    "chunk_id": "generated_chunk_id_xxx"
  },
  "message": "success"
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

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 10,
    "chunks": [
      {
        "chunk_id": "chunk_abc",
        "content_with_weight": "RAG stands for...",
        "similarity": 0.95
      }
    ],
    "labels": []
  },
  "message": "success"
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

### 响应示例
```json
{
  "code": 0,
  "data": {
    "graph": {},
    "mind_map": {
      "id": "root",
      "children": []
    }
  },
  "message": "success"
}
```
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
    "id": "connector_123",
    "name": "My Drive Connector",
    "source": "google_drive",
    "status": "1",
    "config": {"folder_id": "xxx", "api_key": "xxx"},
    "refresh_freq": 60,
    "prune_freq": 720,
    "timeout_secs": 1740,
    "tenant_id": "tenant_1"
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
      "id": "connector_123",
      "name": "My Drive Connector",
      "source": "google_drive",
      "status": "1"
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
    "id": "connector_123",
    "name": "My Drive Connector",
    "source": "google_drive",
    "config": { ... }
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
        "id": "log_1",
        "connector_id": "connector_123",
        "status": "success",
        "start_time": "2024-01-01 12:00:00"
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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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
  "data": true,
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "flow_id": "uuid_flow_id",
    "authorization_url": "https://accounts.google.com/o/oauth2/v2/auth?...",
    "expires_in": 900
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "credentials": { "token": "...", "refresh_token": "..." }
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "flow_id": "uuid_flow_id",
    "authorization_url": "https://account.box.com/api/oauth2/authorize?...",
    "expires_in": 900
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "credentials": {
      "user_id": "...",
      "client_id": "...",
      "access_token": "...",
      "refresh_token": "..."
    }
  },
  "message": "success"
}
```

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
    "message": [{"role": "assistant", "content": "Hello!"}],
    "user_id": "user_1",
    "reference": []
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
    "name": "My Chat",
    "message": [...],
    "avatar": "base64_string_or_url"
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
    "name": "Assistant",
    "avatar": "..."
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
      "name": "Chat 1",
      "create_time": "..."
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
| `messages` | list[dict] | 是 | 消息历史列表 (`[{"role": "user", "content": "..."}]`) |
| `llm_id` | string | 否 | 指定使用的 LLM 模型 ID |
| `stream` | boolean | 否 | 是否流式返回 (默认 true) |
| `temperature` | float | 否 | 模型温度 |
| `top_p` | float | 否 | Top P |
| `max_tokens` | int | 否 | 最大 Token 数 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/completion" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_id": "conv_123",
           "messages": [{"role": "user", "content": "Hello"}],
           "stream": true
         }'
```

### 响应示例 (流式)
```text
data: {"code": 0, "message": "", "data": {"answer": "Hi", "reference": []}}

data: {"code": 0, "message": "", "data": true}
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
| `file` | file | 是 | 音频文件 (wav, mp3, m4a, etc.) |
| `stream` | boolean | 否 | 是否流式返回 (默认 false) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/sequence2txt" \
     -F "file=@/path/to/audio.mp3" \
     -F "stream=false"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "text": "Transcribed text content."
  },
  "message": "success"
}
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

### 响应示例
返回音频流 (`audio/mpeg`)。

---

## 9. 删除消息 (Delete Message)

删除会话中的指定消息。

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
  "data": { ...updated conversation... },
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
           "thumbup": true
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": { ...updated conversation... },
  "message": "success"
}
```

---

## 11. 知识库问答 (Ask)

直接向知识库提问 (Ask about)。通常返回流式数据。

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
           "kb_ids": ["kb_1"]
         }'
```

### 响应示例 (流式)
```text
data: {"code": 0, "message": "", "data": {"answer": "RAG is...", "reference": [...]}}

data: {"code": 0, "message": "", "data": true}
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
           "question": "Project Overview",
           "kb_ids": ["kb_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "root": { "text": "Project Overview", "children": [...] }
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
           "question": "How to install?"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    "System requirements?",
    "Docker deployment steps?"
  ],
  "message": "success"
}
```

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
    "name": "My Assistant",
    "kb_ids": ["kb_123"],
    "llm_id": "chatgpt-3.5",
    "prompt_config": {
        "system": "You are a helpful assistant.",
        "parameters": []
    },
    "description": "A helpful dialog",
    "icon": "",
    "top_n": 6,
    "top_k": 1024,
    "rerank_id": "",
    "similarity_threshold": 0.1,
    "vector_similarity_weight": 0.3
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
    "name": "My Assistant",
    "kb_ids": ["kb_123"],
    "kb_names": ["Knowledge Base 1"],
    "llm_id": "chatgpt-3.5",
    "prompt_config": {
        "system": "You are a helpful assistant.",
        "parameters": []
    },
    "description": "A helpful dialog",
    "icon": "",
    "top_n": 6,
    "top_k": 1024,
    "rerank_id": "",
    "similarity_threshold": 0.1,
    "vector_similarity_weight": 0.3
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
      "name": "My Assistant",
      "kb_ids": ["kb_123"],
      "kb_names": ["Knowledge Base 1"]
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
        "name": "My Assistant",
        "kb_ids": ["kb_123"]
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
      "id": "doc_1",
      "name": "file.pdf",
      "size": 1024,
      "type": "pdf"
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
    "id": "doc_123",
    "name": "virtual_doc.txt"
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
        "id": "doc_1",
        "name": "file.pdf",
        "run_status": "1"
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
    "filter": {}
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
      "id": "doc_1",
      "name": "doc1.pdf",
      "size": 1000
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
    "id": "doc_1",
    "metadata": {"title": "My Doc"}
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
    "doc_1": "/v1/document/image/kb_1-thumb_1",
    "doc_2": "/v1/document/image/kb_1-thumb_2"
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
```json
{
  "code": 0,
  "data": {
    "doc_1": {"status": "1"},
    "doc_2": {"status": "1"}
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
  "data": ["doc_id_1", "doc_id_2"],
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
  "data": "Parsed text content...",
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
  "data": { ... },
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
    "dataset_id": "dataset_123"
  },
  "message": "success"
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
        "id": "dataset_123",
        "name": "My Evaluation Dataset",
        "kb_ids": ["kb_1"],
        "create_time": "2024-01-01 12:00:00"
      }
    ],
    "total": 1
  },
  "message": "success"
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
    "id": "dataset_123",
    "name": "My Evaluation Dataset",
    "description": "Dataset for testing RAG performance",
    "kb_ids": ["kb_1", "kb_2"],
    "create_time": "2024-01-01 12:00:00"
  },
  "message": "success"
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
    "dataset_id": "dataset_123"
  },
  "message": "success"
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
    "dataset_id": "dataset_123"
  },
  "message": "success"
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
    "case_id": "case_456"
  },
  "message": "success"
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
  },
  "message": "success"
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
        "id": "case_456",
        "question": "What is RAGFlow?",
        "reference_answer": "RAGFlow is an open-source RAG engine."
      }
    ],
    "total": 1
  },
  "message": "success"
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
    "case_id": "case_456"
  },
  "message": "success"
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
    "run_id": "run_001"
  },
  "message": "success"
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
    "id": "run_001",
    "status": "completed",
    "score": 0.85
  },
  "message": "success"
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
    "id": "run_001",
    "items": [
      {
        "case_id": "case_456",
        "question": "Q1",
        "answer": "A1",
        "score": 1.0
      }
    ]
  },
  "message": "success"
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
  },
  "message": "success"
}
```

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
    "run_id": "run_001"
  },
  "message": "success"
}
```

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
      "Increase top_k parameter",
      "Adjust prompt template"
    ]
  },
  "message": "success"
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
  },
  "message": "success"
}
```

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
  "data": { ... }
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
    "answer": "Generated answer...",
    "metrics": {
      "fidelity": 0.9,
      "relevance": 0.8
    },
    "retrieved_chunks": []
  },
  "message": "success"
}
```

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

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "uuid_xxx",
      "file_id": "file_123",
      "document_id": "doc_789",
      "create_time": 1700000000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": null,
      "update_date": null
    }
  ],
  "message": "success"
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
           "file_ids": ["file_123"]
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
      "type": "pdf",
      "name": "file2.pdf",
      "location": "file2.pdf",
      "size": 1024
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
    "name": "New Folder",
    "type": "folder",
    "parent_id": "root_folder_id",
    "create_time": 1700000000
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
        "name": "document.pdf",
        "type": "pdf"
      }
    ],
    "parent_folder": {
      "id": "folder_123",
      "name": "Parent Name"
    }
  },
  "message": "success"
}
```

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
      "name": "root",
      "type": "folder"
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
      "name": "My Folder"
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
      { "id": "root", "name": "root" },
      { "id": "folder_1", "name": "Docs" }
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
    "kb_id": "kb_123456"
  },
  "message": "success"
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
           "kb_id": "kb_123456",
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
    "id": "kb_123456",
    "name": "Updated Name",
    "description": "Updated Description",
    "permission": "me",
    "embd_id": "BAAI/bge-large-zh-v1.5",
    "parser_id": "naive",
    "language": "English",
    "pagerank": 0
  },
  "message": "success"
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
           "kb_id": "kb_123456",
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
    "id": "kb_123456",
    "parser_config": {
      "metadata": {
        "field1": "value1"
      }
    }
  },
  "message": "success"
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
curl -X GET "http://localhost:9380/v1/kb/detail?kb_id=kb_123456" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "kb_123456",
    "name": "My KB",
    "description": "...",
    "size": 1024,
    "doc_num": 10
  },
  "message": "success"
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
        "id": "kb_1",
        "name": "KB 1",
        "create_time": 1700000000
      }
    ]
  },
  "message": "success"
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
           "kb_id": "kb_123456"
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

## 7. 获取标签 (List Tags)

- **URL**: `/{kb_id}/tags`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/kb_123456/tags" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": ["tag1", "tag2"],
  "message": "success"
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
  "data": ["tag1", "tag3"],
  "message": "success"
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
curl -X POST "http://localhost:9380/v1/kb/kb_123456/rm_tags" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "tags": ["tag1"]
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
curl -X POST "http://localhost:9380/v1/kb/kb_123456/rename_tag" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "from_tag": "tag1",
           "to_tag": "tag_new"
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

## 11. 获取知识图谱 (Get Knowledge Graph)

- **URL**: `/{kb_id}/knowledge_graph`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/kb/kb_123456/knowledge_graph" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "graph": {
      "nodes": [],
      "edges": []
    },
    "mind_map": {}
  },
  "message": "success"
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
curl -X DELETE "http://localhost:9380/v1/kb/kb_123456/knowledge_graph" \
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
    "kb_1": { "meta_field": "value" }
  },
  "message": "success"
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
curl -X GET "http://localhost:9380/v1/kb/basic_info?kb_id=kb_123456" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "kb_123456",
    "name": "My KB",
    "doc_num": 5
  },
  "message": "success"
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
curl -X POST "http://localhost:9380/v1/kb/list_pipeline_logs?kb_id=kb_123456&page=1" \
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
    "logs": []
  },
  "message": "success"
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
curl -X POST "http://localhost:9380/v1/kb/list_pipeline_dataset_logs?kb_id=kb_123456&page=1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{}'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 50,
    "logs": []
  },
  "message": "success"
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
curl -X POST "http://localhost:9380/v1/kb/delete_pipeline_logs?kb_id=kb_123456" \
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
  "data": true,
  "message": "success"
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
curl -X GET "http://localhost:9380/v1/kb/pipeline_log_detail?log_id=log_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "log_123",
    "content": "..."
  },
  "message": "success"
}
```

---

## 19. 运行 GraphRAG 任务 (Run GraphRAG)

同接口 7。

- **URL**: `/run_graphrag`
- **Method**: `POST`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 响应示例
```json
{
  "code": 0,
  "data": { "graphrag_task_id": "task_id" },
  "message": "success"
}
```

---

## 20. 追踪 GraphRAG 任务 (Trace GraphRAG)

同接口 8。

- **URL**: `/trace_graphrag`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 响应示例
```json
{
  "code": 0,
  "data": { "progress": 0.8 },
  "message": "success"
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
           "kb_id": "kb_123456"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": { "raptor_task_id": "task_raptor_1" },
  "message": "success"
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
curl -X GET "http://localhost:9380/v1/kb/trace_raptor?kb_id=kb_123456" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": { "progress": 0.5 },
  "message": "success"
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
           "kb_id": "kb_123456"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": { "mindmap_task_id": "task_mm_1" },
  "message": "success"
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
curl -X GET "http://localhost:9380/v1/kb/trace_mindmap?kb_id=kb_123456" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": { "progress": 1.0 },
  "message": "success"
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
curl -X DELETE "http://localhost:9380/v1/kb/unbind_task?kb_id=kb_123456&pipeline_task_type=graphrag" \
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

## 26. 检查 Embedding (Check Embedding)

同接口 9。

- **URL**: `/check_embedding`
- **Method**: `POST`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |
| `embd_id` | string | 是 | 目标 Embedding 模型 ID |
| `check_num` | int | 否 | 采样数量 |

### 响应示例
```json
{
  "code": 0,
  "data": { "summary": {}, "results": [] },
  "message": "success"
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
      "tags": "LLM, Text Embedding",
      "model_types": ["chat", "embedding"]
    },
    {
      "name": "VolcEngine",
      "logo": "base64_string...",
      "tags": "LLM",
      "model_types": ["chat", "embedding"]
    }
  ],
  "message": "success"
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
  "data": true,
  "message": "success"
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
  "data": true,
  "message": "success"
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
  "data": true,
  "message": "success"
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
  "data": true,
  "message": "success"
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
  "data": true,
  "message": "success"
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

### 响应示例
```json
{
  "code": 0,
  "data": {
    "OpenAI": {
      "tags": "LLM, Text Embedding",
      "llm": [
        {
          "type": "chat",
          "name": "gpt-3.5-turbo",
          "used_token": 1000,
          "api_base": "",
          "max_tokens": 8192,
          "status": "1"
        }
      ]
    }
  },
  "message": "success"
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
        "llm_name": "gpt-3.5-turbo",
        "model_type": "chat",
        "fid": "OpenAI",
        "available": true,
        "status": "1"
      }
    ]
  },
  "message": "success"
}
```

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
        "id": "mcp_1",
        "name": "My MCP Server",
        "url": "http://example.com/sse",
        "server_type": "sse",
        "create_time": 1700000000
      }
    ],
    "total": 1
  },
  "message": "success"
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
    "id": "mcp_1",
    "name": "My MCP Server",
    "url": "http://example.com/sse",
    "server_type": "sse",
    "variables": {},
    "headers": {}
  },
  "message": "success"
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
    "id": "generated_uuid",
    "name": "Weather MCP",
    "url": "http://weather-mcp.example.com/sse",
    "server_type": "sse"
  },
  "message": "success"
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
    "id": "mcp_1",
    "name": "Updated Weather MCP",
    "url": "http://weather-mcp.example.com/sse"
  },
  "message": "success"
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
  "data": true,
  "message": "success"
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
        "id": "new_uuid",
        "new_name": "my-server"
      }
    ]
  },
  "message": "success"
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
        "tools": {}
      }
    }
  },
  "message": "success"
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
    "mcp_1": [
      {
        "name": "get_weather",
        "description": "Get weather info",
        "inputSchema": {},
        "enabled": true
      }
    ]
  },
  "message": "success"
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
        "text": "Weather in Beijing is Sunny"
      }
    ]
  },
  "message": "success"
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
    "get_weather": {"name": "get_weather", "enabled": true}
  },
  "message": "success"
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
      "description": "Get weather info",
      "enabled": true
    }
  ],
  "message": "success"
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
  "data": [
    {
      "name": "calculator",
      "displayName": "Calculator",
      "description": "Perform basic arithmetic operations.",
      "displayDescription": "计算器",
      "parameters": {
        "expression": {
          "type": "string",
          "description": "Mathematical expression to evaluate.",
          "displayDescription": "数学表达式",
          "required": true
        }
      }
    },
    {
      "name": "google_search",
      "displayName": "Google Search",
      "description": "Search for information on the internet.",
      "displayDescription": "谷歌搜索",
      "parameters": {
        "query": {
          "type": "string",
          "description": "The search query.",
          "displayDescription": "搜索关键词",
          "required": true
        }
      }
    }
  ],
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "search_id": "search_123456"
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "id": "search_123456",
    "name": "Updated Search App",
    "tenant_id": "tenant_1",
    "search_config": {
      "kb_ids": ["kb_1", "kb_2"],
      "similarity_threshold": 0.5,
      "vector_similarity_weight": 0.3,
      "top_k": 1024,
      "use_kg": false
    },
    "status": "1",
    "created_by": "user_1",
    "create_time": 1700000000,
    "update_time": 1700000000
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "id": "search_123456",
    "name": "My Search App",
    "tenant_id": "tenant_1",
    "search_config": {
      "kb_ids": ["kb_1"],
      "similarity_threshold": 0.2
    },
    "status": "1",
    "created_by": "user_1",
    "create_time": 1700000000
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "search_apps": [
      {
        "id": "search_123456",
        "name": "My Search App",
        "tenant_id": "tenant_1",
        "create_time": 1700000000
      }
    ],
    "total": 1
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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
```json
{
  "code": 0,
  "data": "0.1.0",
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "doc_engine": {
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
    "task_executor_heartbeats": {}
  },
  "message": "success"
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
```json
{
  "status": "ok"
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
```json
{
  "code": 0,
  "data": {
    "tenant_id": "tenant_1",
    "token": "ragflow-xxxxxxxx",
    "beta": "xxxxxxxx",
    "create_time": 1700000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": null,
    "update_date": null
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": [
    {
      "tenant_id": "tenant_1",
      "token": "ragflow-xxxxxxxx",
      "beta": "xxxxxxxx",
      "create_time": 1700000000,
      "create_date": "2024-01-01 12:00:00"
    }
  ],
  "message": "success"
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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "registerEnabled": true
  },
  "message": "success"
}
```

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
```json
{
  "code": 0,
  "data": [
    {
      "id": "user_1",
      "nickname": "Alice",
      "email": "alice@example.com",
      "role": "owner",
      "status": "1",
      "update_date": "2024-01-01 12:00:00",
      "delta_seconds": 120
    }
  ],
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "id": "user_2",
    "avatar": "base64_string...",
    "email": "bob@example.com",
    "nickname": "Bob"
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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
```json
{
  "code": 0,
  "data": [
    {
      "tenant_id": "tenant_1",
      "name": "My Team",
      "role": "owner",
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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

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
```json
{
  "code": 0,
  "data": {
    "id": "user_id_xxx",
    "email": "user@example.com",
    "nickname": "User Nickname",
    "access_token": "token_xxx",
    "create_time": 1700000000,
    "update_time": 1700000000
  },
  "message": "Welcome back!"
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
```json
{
  "code": 0,
  "data": [
    {
      "channel": "github",
      "display_name": "GitHub",
      "icon": "github_icon_path"
    }
  ],
  "message": "success"
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
Redirect to OAuth provider.

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
Redirect to frontend (e.g., `/?auth=user_id` or `/?error=xxx`).

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
Redirect to frontend.

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
Redirect to frontend.

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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

---

## 8. 更新设置 (Update Settings)

更新用户信息 (昵称, 邮箱, 密码等)。

- **URL**: `/setting`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: Required

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `nickname` | string | 否 | 新昵称 |
| `email` | string | 否 | 新邮箱 |
| `password` | string | 否 | 当前密码 (若修改密码则必填, 加密) |
| `new_password` | string | 否 | 新密码 (加密) |
| `avatar` | string | 否 | 头像 URL |

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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "id": "user_id_xxx",
    "nickname": "User Nickname",
    "email": "user@example.com"
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "id": "new_user_id",
    "email": "new@example.com",
    "nickname": "NewUser"
  },
  "message": "NewUser, welcome aboard!"
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
```json
{
  "code": 0,
  "data": {
    "tenant_id": "user_id",
    "name": "User's Kingdom",
    "llm_id": "gpt-3.5",
    "embd_id": "embedding-model"
  },
  "message": "success"
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
           "tenant_id": "tenant_1",
           "llm_id": "gpt-4",
           "embd_id": "bge-large-zh",
           "asr_id": "whisper-1",
           "img2txt_id": "gpt-4-vision"
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
Returns binary image data (JPEG).

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
           "captcha": "AB12"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true,
  "message": "verification passed, email sent"
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
```json
{
  "code": 0,
  "data": true,
  "message": "otp verified"
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
           "new_password": "encrypted_pwd",
           "confirm_new_password": "encrypted_pwd"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "user_id",
    "email": "user@example.com"
  },
  "message": "Password reset successful. Logged in."
}
```

