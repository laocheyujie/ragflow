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

