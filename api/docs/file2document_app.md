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

