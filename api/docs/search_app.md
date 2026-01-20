# Search API 文档

**Base URL**: `http://localhost:9380/v1/search`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建搜索应用 (Create Search App)

创建一个新的搜索应用。

- **URL**: `/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 搜索应用名称 (不超过 255 字节) |
| `description` | string | 否 | 描述信息 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/search/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Search App",
           "description": "A search app for internal docs"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "search_id": "search_123456"
  },
  "message": "success"
}
```

---

## 2. 更新搜索应用 (Update Search App)

更新搜索应用的配置、名称等信息。

- **URL**: `/update`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `search_id` | string | 是 | 搜索应用 ID |
| `name` | string | 是 | 搜索应用名称 |
| `search_config` | object | 是 | 搜索配置 (包含 kb_ids, similarity_threshold 等) |
| `tenant_id` | string | 是 | 租户 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/search/update" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "search_id": "search_123456",
           "name": "Updated Search App",
           "tenant_id": "tenant_1",
           "search_config": {
             "kb_ids": ["kb_1", "kb_2"],
             "similarity_threshold": 0.5,
             "vector_similarity_weight": 0.3,
             "top_k": 1024
           }
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "search_123456",
    "name": "Updated Search App",
    "tenant_id": "tenant_1",
    "search_config": {
      "kb_ids": ["kb_1", "kb_2"],
      "similarity_threshold": 0.5,
      "vector_similarity_weight": 0.3,
      "top_k": 1024,
      "use_kg": false
    },
    "status": "1",
    "created_by": "user_1",
    "create_time": 1700000000,
    "update_time": 1700000000
  },
  "message": "success"
}
```

---

## 3. 获取搜索应用详情 (Get Search App Detail)

获取指定搜索应用的详细信息。

- **URL**: `/detail`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `search_id` | string | 是 | 搜索应用 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/search/detail?search_id=search_123456" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "search_123456",
    "name": "My Search App",
    "tenant_id": "tenant_1",
    "search_config": {
      "kb_ids": ["kb_1"],
      "similarity_threshold": 0.2
    },
    "status": "1",
    "created_by": "user_1",
    "create_time": 1700000000
  },
  "message": "success"
}
```

---

## 4. 获取搜索应用列表 (List Search Apps)

获取搜索应用列表，支持分页和关键词搜索。

- **URL**: `/list`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `keywords` | string | 否 | 搜索关键词 |
| `page` | integer | 否 | 页码 (默认 0, 表示不分页或第一页) |
| `page_size` | integer | 否 | 每页数量 (默认 0) |
| `orderby` | string | 否 | 排序字段 (默认 "create_time") |
| `desc` | boolean | 否 | 是否降序 (默认 true) |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `owner_ids` | list[string] | 否 | 指定 Tenant ID 列表进行筛选 (若不传则查询当前用户权限下的) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/search/list?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "owner_ids": []
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "search_apps": [
      {
        "id": "search_123456",
        "name": "My Search App",
        "tenant_id": "tenant_1",
        "create_time": 1700000000
      }
    ],
    "total": 1
  },
  "message": "success"
}
```

---

## 5. 删除搜索应用 (Delete Search App)

删除指定的搜索应用。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `search_id` | string | 是 | 要删除的搜索应用 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/search/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "search_id": "search_123456"
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

