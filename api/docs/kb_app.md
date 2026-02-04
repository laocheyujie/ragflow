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
