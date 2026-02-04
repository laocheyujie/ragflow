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
