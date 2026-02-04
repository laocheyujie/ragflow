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

