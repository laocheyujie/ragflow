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
    "parser_config": { "..." },
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
    "mind_map": { "..." }
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

