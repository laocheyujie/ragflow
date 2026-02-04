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
  "data": { "..." },
  "message": "success"
}
```

---
