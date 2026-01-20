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
