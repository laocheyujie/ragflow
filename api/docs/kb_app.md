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
    "processing": 1,
    "finished": 0,
    "failed": 0,
    "cancelled": 0,
    "downloaded": 0,
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

