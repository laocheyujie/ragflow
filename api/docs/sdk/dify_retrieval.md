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

