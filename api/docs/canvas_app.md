# Canvas API 文档

**Base URL**: `http://localhost:9380/v1/canvas`

**Authentication**:
绝大多数接口需要认证。请在 Header 中携带 API Key 或登录后的 Authorization Token：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取画布模板 (Templates)

获取系统提供的画布模板列表。

- **URL**: `/templates`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/templates" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "template_1",
      "title": "Translation Agent",
      "description": "A template for translation tasks.",
      "dsl": "..."
    }
  ],
  "message": "success"
}
```

---

## 2. 删除画布 (Remove Canvas)

删除一个或多个画布。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_ids` | list[string] | 是 | 要删除的 Canvas ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/rm" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "canvas_ids": ["canvas_1", "canvas_2"]
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

## 3. 保存/创建画布 (Save/Set Canvas)

创建新的画布或更新现有画布。

- **URL**: `/set`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dsl` | string/object | 是 | 画布的 DSL (JSON 结构) |
| `title` | string | 是 | 画布标题 |
| `id` | string | 否 | Canvas ID (更新时必填，创建时不填) |
| `canvas_category` | string | 否 | 画布类别 (默认 "Agent") |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/set" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "title": "My New Agent",
           "dsl": {...}
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "generated_canvas_id",
    "title": "My New Agent",
    "dsl": {...},
    "user_id": "user_1"
  },
  "message": "success"
}
```

---

## 4. 获取画布详情 (Get Canvas)

获取指定 ID 的画布详情。

- **URL**: `/get/<canvas_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/get/canvas_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "canvas_123",
    "title": "My Agent",
    "dsl": {...},
    "create_time": "..."
  },
  "message": "success"
}
```

---

## 5. 获取画布详情 (Get Canvas SSE)

通过 API Key 获取画布详情 (主要用于 SSE 场景下的鉴权)。

- **URL**: `/getsse/<canvas_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/getsse/canvas_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "canvas_123",
    "title": "My Agent",
    "dsl": {...}
  },
  "message": "success"
}
```

---

## 6. 运行画布 (Completion)

运行画布 (Agent 或 DataFlow)。返回 SSE 流。

- **URL**: `/completion`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Canvas ID |
| `query` | string | 否 | 用户输入的问题 |
| `files` | list | 否 | 上传的文件列表 |
| `inputs` | object | 否 | 其他输入参数 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/completion" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "query": "Hello world"
         }'
```

### 响应示例 (SSE Stream)
```
data: {"content": "Thinking...", "node_id": "step_1"}

data: {"content": "Hello! How can I help you?", "node_id": "step_2"}
```

---

## 7. 重跑任务 (Rerun)

重跑某个任务或组件。

- **URL**: `/rerun`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Canvas ID |
| `dsl` | object | 是 | 画布 DSL |
| `component_id` | string | 是 | 需要重跑的组件 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/rerun" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "component_id": "component_abc",
           "dsl": {...}
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

## 8. 取消任务 (Cancel Task)

取消正在运行的任务。

- **URL**: `/cancel/<task_id>`
- **Method**: `PUT`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `task_id` | string | 是 | 任务 ID |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/canvas/cancel/task_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
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

## 9. 重置画布 (Reset Canvas)

重置画布状态。

- **URL**: `/reset`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Canvas ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/reset" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {...}, // 重置后的 DSL
  "message": "success"
}
```

---

## 10. 上传文件 (Upload File)

上传文件到指定画布。

- **URL**: `/upload/<canvas_id>`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Path/Query/Form)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID (Path) |
| `file` | file | 否 | 文件内容 (Form Data) |
| `url` | string | 否 | 文件 URL (Query, 若无 file 则使用 url) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/upload/canvas_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -F "file=@/path/to/file.pdf"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "file_id": "file_123",
    "name": "file.pdf"
  },
  "message": "success"
}
```

---

## 11. 获取组件输入表单 (Input Form)

获取画布中组件的输入表单结构。

- **URL**: `/input_form`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Canvas ID |
| `component_id` | string | 是 | 组件 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/input_form?id=canvas_123&component_id=comp_1" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "form": [
      {"name": "field1", "type": "text"}
    ]
  },
  "message": "success"
}
```

---

## 12. 调试组件 (Debug Component)

调试运行单个组件。

- **URL**: `/debug`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Canvas ID |
| `component_id` | string | 是 | 组件 ID |
| `params` | object | 是 | 调试参数 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/debug" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "component_id": "llm_component",
           "params": {"prompt": "Hello"}
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "content": "Result from LLM",
    "usage": {...}
  },
  "message": "success"
}
```

---

## 13. 测试数据库连接 (Test DB Connect)

测试各种数据库连接 (MySQL, Postgres, MSSQL, Trino, etc.)。

- **URL**: `/test_db_connect`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `db_type` | string | 是 | 数据库类型 (mysql, postgres, mssql, trino 等) |
| `database` | string | 是 | 数据库名 |
| `username` | string | 是 | 用户名 |
| `host` | string | 是 | 主机地址 |
| `port` | int | 是 | 端口 |
| `password` | string | 是 | 密码 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/test_db_connect" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "db_type": "mysql",
           "host": "localhost",
           "port": 3306,
           "username": "root",
           "password": "password",
           "database": "test_db"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": "Database Connection Successful!",
  "message": "success"
}
```

---

## 14. 获取版本列表 (Get Version List)

获取画布的历史版本列表。

- **URL**: `/getlistversion/<canvas_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/getlistversion/canvas_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {"id": "v1", "title": "ver_1", "update_time": ...},
    {"id": "v2", "title": "ver_2", "update_time": ...}
  ],
  "message": "success"
}
```

---

## 15. 获取版本详情 (Get Version)

获取指定版本的画布 DSL。

- **URL**: `/getversion/<version_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `version_id` | string | 是 | 版本 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/getversion/ver_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "ver_123",
    "dsl": {...},
    "create_time": ...
  },
  "message": "success"
}
```

---

## 16. 画布列表 (List Canvas)

获取用户的画布列表，支持分页和搜索。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `keywords` | string | 否 | 搜索关键词 |
| `page` | int | 否 | 页码 (默认 0) |
| `page_size` | int | 否 | 每页数量 (默认 0 表示全部) |
| `orderby` | string | 否 | 排序字段 (默认 create_time) |
| `desc` | boolean | 否 | 是否倒序 (默认 true) |
| `canvas_category` | string | 否 | 类别筛选 |
| `owner_ids` | string | 否 | 逗号分隔的 User ID 列表 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/list?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "canvas": [...],
    "total": 100
  },
  "message": "success"
}
```

---

## 17. 设置画布 (Setting)

更新画布的元数据 (标题、描述、权限、头像等)。

- **URL**: `/setting`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | Canvas ID |
| `title` | string | 是 | 标题 |
| `permission` | string | 是 | 权限设置 |
| `description` | string | 否 | 描述 |
| `avatar` | string | 否 | 头像 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/setting" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "title": "New Title",
           "permission": "public"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": 1, // 更新行数
  "message": "success"
}
```

---

## 18. 追踪日志 (Trace)

获取画布运行的详细追踪日志。

- **URL**: `/trace`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID |
| `message_id` | string | 是 | 消息 ID (运行 ID) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/trace?canvas_id=c1&message_id=m1" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": { "..." }, // 详细日志结构
  "message": "success"
}
```

---

## 19. 获取会话列表 (Sessions)

获取画布的对话历史会话。

- **URL**: `/<canvas_id>/sessions`
- **Method**: `GET`

### 请求参数 (Path/Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `canvas_id` | string | 是 | Canvas ID (Path) |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认 30) |
| `user_id` | string | 否 | 用户 ID 筛选 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/canvas_123/sessions" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 50,
    "sessions": [...]
  },
  "message": "success"
}
```

---

## 20. 获取 Prompt 模板 (Prompts)

获取系统内置的 Prompt 模板。

- **URL**: `/prompts`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/prompts" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "task_analysis": "...",
    "plan_generation": "..."
  },
  "message": "success"
}
```

---

## 21. 下载文件 (Download)

下载画布相关的文件。

- **URL**: `/download`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 是 | 文件 ID |
| `created_by` | string | 是 | 创建者 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/download?id=file_1&created_by=user_1" \
     -H "Authorization: Bearer <YOUR_TOKEN>"
```

### 响应示例
(二进制文件流)

