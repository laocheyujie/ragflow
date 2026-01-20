# Dialog API 文档

**Base URL**: `http://localhost:9380/v1/dialog`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 设置对话 (Set Dialog)

创建或更新一个对话 (Assistant)。如果不提供 `dialog_id`，则创建一个新的对话。

- **URL**: `/set`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 否 | Dialog ID (若未提供则创建新 Dialog) |
| `name` | string | 否 | Dialog 名称 (默认为 "New Dialog") |
| `description` | string | 否 | Dialog 描述 |
| `icon` | string | 否 | Dialog 图标 |
| `kb_ids` | list[string] | 否 | 关联的知识库 ID 列表 |
| `llm_id` | string | 否 | LLM 模型 ID (如 "deepseek-chat") |
| `llm_setting` | object | 否 | LLM 参数设置 (如 temperature) |
| `prompt_config` | object | 是 | Prompt 配置 (包含 system, parameters 等) |
| `top_n` | int | 否 | 引用片段数量 (默认 6) |
| `top_k` | int | 否 | 搜索候选项数量 (默认 1024) |
| `rerank_id` | string | 否 | Rerank 模型 ID |
| `similarity_threshold` | float | 否 | 相似度阈值 (默认 0.1) |
| `vector_similarity_weight` | float | 否 | 向量搜索权重 (1.0为纯向量，0为纯关键词，默认 0.3) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/dialog/set" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Assistant",
           "kb_ids": ["kb_123"],
           "llm_id": "chatgpt-3.5",
           "prompt_config": {
               "system": "You are a helpful assistant.",
               "parameters": []
           }
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "dialog_xyz",
    "name": "My Assistant",
    "kb_ids": ["kb_123"],
    "llm_id": "chatgpt-3.5",
    "prompt_config": {
        "system": "You are a helpful assistant.",
        "parameters": []
    },
    "description": "A helpful dialog",
    "icon": "",
    "top_n": 6,
    "top_k": 1024,
    "rerank_id": "",
    "similarity_threshold": 0.1,
    "vector_similarity_weight": 0.3
  },
  "message": "success"
}
```

---

## 2. 获取对话详情 (Get Dialog)

获取指定对话的详细配置。

- **URL**: `/get`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | Dialog ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/dialog/get?dialog_id=dialog_xyz" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "dialog_xyz",
    "name": "My Assistant",
    "kb_ids": ["kb_123"],
    "kb_names": ["Knowledge Base 1"],
    "llm_id": "chatgpt-3.5",
    "prompt_config": {
        "system": "You are a helpful assistant.",
        "parameters": []
    },
    "description": "A helpful dialog",
    "icon": "",
    "top_n": 6,
    "top_k": 1024,
    "rerank_id": "",
    "similarity_threshold": 0.1,
    "vector_similarity_weight": 0.3
  },
  "message": "success"
}
```

---

## 3. 获取对话列表 (List Dialogs)

获取当前用户的所有对话列表。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/dialog/list" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "dialog_xyz",
      "name": "My Assistant",
      "kb_ids": ["kb_123"],
      "kb_names": ["Knowledge Base 1"]
    }
  ],
  "message": "success"
}
```

---

## 4. 获取对话列表 (分页) (List Dialogs - Pagination)

支持分页、搜索和排序的对话列表查询。

- **URL**: `/next`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | int | 否 | 页码 (默认 0，表示不分页或第一页) |
| `page_size` | int | 否 | 每页数量 |
| `keywords` | string | 否 | 搜索关键词 (匹配名称) |
| `orderby` | string | 否 | 排序字段 (默认 "create_time") |
| `desc` | string/bool | 否 | 是否降序 (默认 true) |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `owner_ids` | list[string] | 否 | 租户 ID 列表 (用于管理员查询，普通用户忽略) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/dialog/next?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{}'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "dialogs": [
      { 
        "id": "dialog_xyz", 
        "name": "My Assistant",
        "kb_ids": ["kb_123"]
      }
    ],
    "total": 1
  },
  "message": "success"
}
```

---

## 5. 删除对话 (Remove Dialog)

删除一个或多个对话。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_ids` | list[string] | 是 | 要删除的 Dialog ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/dialog/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "dialog_ids": ["dialog_xyz"]
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

