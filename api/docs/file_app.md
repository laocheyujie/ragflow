# File Management API 文档

**Base URL**: `http://localhost:9380/v1/file`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 上传文件 (Upload)

上传一个或多个文件到指定文件夹。

- **URL**: `/upload`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Form Data)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file` | file | 是 | 要上传的文件 (支持多文件上传) |
| `parent_id` | string | 否 | 父文件夹 ID (默认上传到根目录) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file/upload" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -F "file=@/path/to/file1.txt" \
     -F "file=@/path/to/file2.pdf" \
     -F "parent_id=folder_123"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "file_uuid_1",
      "parent_id": "folder_123",
      "tenant_id": "tenant_1",
      "created_by": "user_1",
      "name": "file2.pdf",
      "location": "file2.pdf",
      "size": 1024,
      "type": "pdf",
      "source_type": "",
      "create_time": 1738656000000,
      "create_date": "2025-02-04 12:00:00",
      "update_time": 1738656000000,
      "update_date": "2025-02-04 12:00:00"
    }
  ],
  "message": "success"
}
```

---

## 2. 创建文件夹 (Create Folder)

创建一个新的文件夹或虚拟文件。

- **URL**: `/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 文件夹名称 |
| `parent_id` | string | 否 | 父文件夹 ID (默认根目录) |
| `type` | string | 否 | 类型 ("folder" 或 "virtual") |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "New Folder",
           "parent_id": "root_folder_id",
           "type": "folder"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "folder_uuid",
    "parent_id": "root_folder_id",
    "tenant_id": "tenant_1",
    "created_by": "user_1",
    "name": "New Folder",
    "location": "",
    "size": 0,
    "type": "folder",
    "source_type": "",
    "create_time": 1738656000000,
    "create_date": "2025-02-04 12:00:00",
    "update_time": 1738656000000,
    "update_date": "2025-02-04 12:00:00"
  },
  "message": "success"
}
```

---

## 3. 获取文件列表 (List Files)

分页获取指定文件夹下的文件列表。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `parent_id` | string | 否 | 父文件夹 ID (默认根目录) |
| `keywords` | string | 否 | 搜索关键字 |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 15) |
| `orderby` | string | 否 | 排序字段 (默认 "create_time") |
| `desc` | boolean | 否 | 是否倒序 (默认 True) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/file/list?parent_id=folder_123&page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 5,
    "files": [
      {
        "id": "file_1",
        "parent_id": "folder_123",
        "tenant_id": "tenant_1",
        "created_by": "user_1",
        "name": "document.pdf",
        "location": "document.pdf",
        "size": 2048,
        "type": "pdf",
        "source_type": "",
        "create_time": 1738656000000,
        "create_date": "2025-02-04 12:00:00",
        "update_time": 1738656000000,
        "update_date": "2025-02-04 12:00:00",
        "kbs_info": [
          {
            "kb_id": "kb_1",
            "kb_name": "My Knowledge Base",
            "document_id": "doc_1"
          }
        ]
      },
      {
        "id": "folder_456",
        "parent_id": "folder_123",
        "tenant_id": "tenant_1",
        "created_by": "user_1",
        "name": "Sub Folder",
        "location": "",
        "size": 4096,
        "type": "folder",
        "source_type": "",
        "create_time": 1738656000000,
        "create_date": "2025-02-04 12:00:00",
        "update_time": 1738656000000,
        "update_date": "2025-02-04 12:00:00",
        "kbs_info": [],
        "has_child_folder": true
      }
    ],
    "parent_folder": {
      "id": "folder_123",
      "parent_id": "root_id",
      "tenant_id": "tenant_1",
      "created_by": "user_1",
      "name": "Parent Name",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1738656000000,
      "create_date": "2025-02-04 12:00:00",
      "update_time": 1738656000000,
      "update_date": "2025-02-04 12:00:00"
    }
  },
  "message": "success"
}
```

**说明**:
- 对于文件类型，`kbs_info` 返回关联的知识库信息列表
- 对于文件夹类型，`kbs_info` 为空数组，`has_child_folder` 表示是否包含子文件夹，`size` 为文件夹内所有文件的总大小

---

## 4. 获取根文件夹 (Root Folder)

获取当前用户的根文件夹信息。

- **URL**: `/root_folder`
- **Method**: `GET`

### 请求参数
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/file/root_folder" \
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
      "tenant_id": "tenant_1",
      "created_by": "tenant_1",
      "name": "/",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1738656000000,
      "create_date": "2025-02-04 12:00:00",
      "update_time": 1738656000000,
      "update_date": "2025-02-04 12:00:00"
    }
  },
  "message": "success"
}
```

---

## 5. 获取父文件夹 (Parent Folder)

获取指定文件的直接父文件夹信息。

- **URL**: `/parent_folder`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 当前文件或文件夹 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/file/parent_folder?file_id=file_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "parent_folder": {
      "id": "folder_123",
      "parent_id": "root_id",
      "tenant_id": "tenant_1",
      "created_by": "user_1",
      "name": "My Folder",
      "location": "",
      "size": 0,
      "type": "folder",
      "source_type": "",
      "create_time": 1738656000000,
      "create_date": "2025-02-04 12:00:00",
      "update_time": 1738656000000,
      "update_date": "2025-02-04 12:00:00"
    }
  },
  "message": "success"
}
```

---

## 6. 获取所有父文件夹路径 (All Parent Folders)

获取指定文件的所有上级目录（路径）。

- **URL**: `/all_parent_folder`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 当前文件或文件夹 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/file/all_parent_folder?file_id=file_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "parent_folders": [
      {
        "id": "file_123",
        "parent_id": "folder_1",
        "tenant_id": "tenant_1",
        "created_by": "user_1",
        "name": "document.pdf",
        "location": "document.pdf",
        "size": 2048,
        "type": "pdf",
        "source_type": "",
        "create_time": 1738656000000,
        "create_date": "2025-02-04 12:00:00",
        "update_time": 1738656000000,
        "update_date": "2025-02-04 12:00:00"
      },
      {
        "id": "folder_1",
        "parent_id": "root_id",
        "tenant_id": "tenant_1",
        "created_by": "user_1",
        "name": "Docs",
        "location": "",
        "size": 0,
        "type": "folder",
        "source_type": "",
        "create_time": 1738656000000,
        "create_date": "2025-02-04 12:00:00",
        "update_time": 1738656000000,
        "update_date": "2025-02-04 12:00:00"
      },
      {
        "id": "root_id",
        "parent_id": "root_id",
        "tenant_id": "tenant_1",
        "created_by": "tenant_1",
        "name": "/",
        "location": "",
        "size": 0,
        "type": "folder",
        "source_type": "",
        "create_time": 1738656000000,
        "create_date": "2025-02-04 12:00:00",
        "update_time": 1738656000000,
        "update_date": "2025-02-04 12:00:00"
      }
    ]
  },
  "message": "success"
}
```

---

## 7. 删除文件/文件夹 (Remove)

删除指定的文件或文件夹。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_ids` | list[string] | 是 | 要删除的文件/文件夹 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_ids": ["file_1", "folder_2"]
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

## 8. 重命名 (Rename)

重命名文件或文件夹。

- **URL**: `/rename`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 文件 ID |
| `name` | string | 是 | 新名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file/rename" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "file_id": "file_123",
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

## 9. 下载/获取文件内容 (Get Content)

获取文件内容或下载文件。

- **URL**: `/get/<file_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file_id` | string | 是 | 文件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/file/get/file_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
(二进制文件流)

---

## 10. 移动文件 (Move)

移动文件或文件夹到另一个文件夹。

- **URL**: `/mv`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `src_file_ids` | list[string] | 是 | 源文件 ID 列表 |
| `dest_file_id` | string | 是 | 目标文件夹 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/file/mv" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "src_file_ids": ["file_1", "file_2"],
           "dest_file_id": "folder_destination"
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

