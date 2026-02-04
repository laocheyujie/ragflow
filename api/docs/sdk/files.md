# File Management API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 上传文件 (Upload File)

上传文件到系统。

- **URL**: `/file/upload`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (FormData)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file` | file | 是 | 要上传的文件 |
| `parent_id` | string | 否 | 父文件夹 ID (若不传则上传到根目录) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/upload" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -F "file=@/path/to/document.pdf" \
     -F "parent_id=folder_123"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "file_uuid",
      "parent_id": "folder_123",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "document.pdf",
      "location": "document.pdf",
      "size": 1024,
      "type": "pdf",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 12:00:00"
    }
  ],
  "message": "success"
}
```

---

## 2. 创建文件/文件夹 (Create File/Folder)

创建一个新的文件夹或虚拟文件。

- **URL**: `/file/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 文件/文件夹名称 |
| `type` | string | 否 | 类型: `FOLDER` 或 `VIRTUAL` (默认 `VIRTUAL`) |
| `parent_id` | string | 否 | 父文件夹 ID (默认根目录) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "New Folder",
           "type": "FOLDER",
           "parent_id": "root_id"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "folder_uuid",
    "parent_id": "root_id",
    "tenant_id": "tenant_id",
    "created_by": "tenant_id",
    "name": "New Folder",
    "location": "",
    "size": 0,
    "type": "folder",
    "source_type": "",
    "create_time": 1704067200000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1704067200000,
    "update_date": "2024-01-01 12:00:00"
  },
  "message": "success"
}
```

---

## 3. 获取文件列表 (List Files)

列出指定文件夹下的文件。

- **URL**: `/file/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `parent_id` | string | 否 | 文件夹 ID (默认根目录) |
| `keywords` | string | 否 | 搜索关键字 |
| `page` | integer | 否 | 页码 (默认 1) |
| `page_size` | integer | 否 | 每页数量 (默认 15) |
| `orderby` | string | 否 | 排序字段 (默认 `create_time`) |
| `desc` | boolean | 否 | 是否降序 (默认 `true`) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/list?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 50,
    "files": [
      {
        "id": "file_1",
        "parent_id": "folder_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "doc.pdf",
        "location": "doc.pdf",
        "size": 2048,
        "type": "pdf",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 10:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 10:00:00",
        "kbs_info": [
          {
            "kb_id": "kb_id_1",
            "kb_name": "My Dataset",
            "document_id": "doc_id_1"
          }
        ]
      },
      {
        "id": "folder_2",
        "parent_id": "folder_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "subfolder",
        "location": "",
        "size": 4096,
        "type": "folder",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 09:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 09:00:00",
        "kbs_info": [],
        "has_child_folder": true
      }
    ],
    "parent_folder": {
      "id": "folder_id",
      "parent_id": "root_id",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "root",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 08:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 08:00:00"
    }
  },
  "message": "success"
}
```

---

## 4. 获取根目录 (Get Root Folder)

获取用户的根文件夹信息。

- **URL**: `/file/root_folder`
- **Method**: `GET`

### 请求参数
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/root_folder" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "root_folder": {
      "id": "root_id",
      "parent_id": "root_id",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "/",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 00:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 00:00:00"
    }
  },
  "message": "success"
}
```

---

## 5. 获取父文件夹 (Get Parent Folder)

获取指定文件的父文件夹信息。

- **URL**: `/file/parent_folder`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 目标文件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/parent_folder?file_id=file_xxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "parent_folder": {
      "id": "parent_id",
      "parent_id": "root_id",
      "tenant_id": "tenant_id",
      "created_by": "tenant_id",
      "name": "Parent Folder",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 00:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 00:00:00"
    }
  },
  "message": "success"
}
```

---

## 6. 获取所有父文件夹 (Get All Parent Folders)

获取文件的所有上级目录（路径）。

- **URL**: `/file/all_parent_folder`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 目标文件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/all_parent_folder?file_id=file_xxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "parent_folders": [
      {
        "id": "file_xxx",
        "parent_id": "folder_level_1",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "current_file.pdf",
        "location": "current_file.pdf",
        "size": 1024,
        "type": "pdf",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 12:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 12:00:00"
      },
      {
        "id": "folder_level_1",
        "parent_id": "root_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "Project A",
        "location": "",
        "size": 0,
        "type": "folder",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 10:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 10:00:00"
      },
      {
        "id": "root_id",
        "parent_id": "root_id",
        "tenant_id": "tenant_id",
        "created_by": "tenant_id",
        "name": "/",
        "location": "",
        "size": 0,
        "type": "folder",
        "source_type": "",
        "create_time": 1704067200000,
        "create_date": "2024-01-01 00:00:00",
        "update_time": 1704067200000,
        "update_date": "2024-01-01 00:00:00"
      }
    ]
  },
  "message": "success"
}
```

---

## 7. 删除文件 (Remove Files)

删除一个或多个文件/文件夹。如果删除文件夹，其中的文件也会被删除。

- **URL**: `/file/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_ids` | list[string] | 是 | 要删除的文件 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_ids": ["file_1", "file_2"]
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

## 8. 重命名文件 (Rename File)

重命名文件。

- **URL**: `/file/rename`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 目标文件 ID |
| `name` | string | 是 | 新名称 (扩展名需保持一致) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/rename" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_id": "file_xxx",
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

## 9. 下载文件 (Download File)

下载文件内容。

- **URL**: `/file/get/<file_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 文件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/get/file_uuid_xxx" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     --output my_file.pdf
```

### 响应示例
(返回二进制文件流，响应头包含 `Content-Type` 字段，如 `application/pdf` 或 `image/png`)

---

## 10. 下载附件 (Download Attachment)

下载系统生成的附件。

- **URL**: `/file/download/<attachment_id>`
- **Method**: `GET`

### 请求参数

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `attachment_id` | string | 是 | 附件 ID (URL Path) |
| `ext` | string | 否 | 扩展名/格式 (Query, 默认 `markdown`) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/file/download/att_uuid?ext=pdf" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
(返回二进制文件流，响应头包含 `Content-Type` 字段，如 `application/pdf` 或 `text/markdown`)

---

## 11. 移动文件 (Move Files)

移动一个或多个文件到另一个文件夹。

- **URL**: `/file/mv`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `src_file_ids` | list[string] | 是 | 源文件 ID 列表 |
| `dest_file_id` | string | 是 | 目标文件夹 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/mv" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "src_file_ids": ["file_1", "file_2"],
           "dest_file_id": "folder_target"
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

## 12. 转换文件 (Convert File)

将文件解析并添加到知识库。

- **URL**: `/file/convert`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_ids` | list[string] | 是 | 目标知识库 ID 列表 |
| `file_ids` | list[string] | 是 | 要转换的文件 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/file/convert" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_ids": ["kb_1"],
           "file_ids": ["file_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "file2doc_id",
      "file_id": "file_1",
      "document_id": "doc_1",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 12:00:00"
    }
  ],
  "message": "success"
}
```
