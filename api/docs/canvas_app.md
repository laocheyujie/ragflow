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
      "avatar": null,
      "title": {"en": "Translation Agent", "zh": "翻译代理"},
      "description": {"en": "A template for translation tasks.", "zh": "用于翻译任务的模板。"},
      "canvas_type": "chatbot",
      "canvas_category": "agent_canvas",
      "dsl": {},
      "create_time": 1704067200000,
      "create_date": "2024-01-01 00:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 00:00:00"
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
           "dsl": {}
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "title": "My New Agent",
    "dsl": {},
    "user_id": "user_123456"
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
    "avatar": null,
    "title": "My Agent",
    "dsl": {},
    "description": "A sample agent",
    "permission": "me",
    "update_time": 1704067200000,
    "user_id": "user_123456",
    "create_time": 1704067200000,
    "create_date": "2024-01-01 00:00:00",
    "update_date": "2024-01-01 00:00:00",
    "canvas_category": "agent_canvas",
    "nickname": "John Doe",
    "tenant_avatar": null
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
    "avatar": null,
    "user_id": "user_123456",
    "title": "My Agent",
    "permission": "me",
    "description": "A sample agent",
    "canvas_type": "chatbot",
    "canvas_category": "agent_canvas",
    "dsl": {},
    "create_time": 1704067200000,
    "create_date": "2024-01-01 00:00:00",
    "update_time": 1704067200000,
    "update_date": "2024-01-01 00:00:00"
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
| `user_id` | string | 否 | 用户 ID |

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

**Agent 模式**:
```
data: {"event": "message", "data": {"content": "Thinking...", "node_id": "step_1"}}

data: {"event": "message", "data": {"content": "Hello! How can I help you?", "node_id": "step_2"}}

data: {"event": "message_end", "data": {"reference": {}}}
```

**DataFlow 模式**:
```json
{
  "code": 0,
  "data": {
    "message_id": "task_uuid_12345678"
  },
  "message": "success"
}
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
| `id` | string | 是 | Pipeline Operation Log ID |
| `dsl` | object | 是 | 画布 DSL |
| `component_id` | string | 是 | 需要重跑的组件 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/rerun" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "log_123",
           "component_id": "component_abc",
           "dsl": {}
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
  "data": {
    "components": {},
    "history": [],
    "messages": [],
    "path": [],
    "answer": []
  },
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
    "id": "a1b2c3d4-uuid-location",
    "name": "file.pdf",
    "size": 102400,
    "extension": "pdf",
    "mime_type": "application/pdf",
    "created_by": "user_123456",
    "created_at": 1704067200.123,
    "preview_url": null
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
  "data": [
    {
      "key": "query",
      "name": "User Query",
      "type": "string",
      "optional": false
    },
    {
      "key": "temperature",
      "name": "Temperature",
      "type": "number",
      "optional": true
    }
  ],
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
| `params` | object | 是 | 调试参数 (key: {value: ...}) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/debug" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "component_id": "llm_component",
           "params": {
             "prompt": {"value": "Hello"}
           }
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "content": "This is the result from the LLM component.",
    "usage": {
      "prompt_tokens": 10,
      "completion_tokens": 50,
      "total_tokens": 60
    }
  },
  "message": "success"
}
```

---

## 13. 测试数据库连接 (Test DB Connect)

测试各种数据库连接 (MySQL, Postgres, MSSQL, Trino, IBM DB2 等)。

- **URL**: `/test_db_connect`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `db_type` | string | 是 | 数据库类型 (mysql, mariadb, postgres, mssql, trino, IBM DB2) |
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
    {
      "id": "version_1",
      "title": "My Agent_2024_01_15_10_30_00",
      "user_canvas_id": "canvas_123",
      "create_time": 1705312200000,
      "create_date": "2024-01-15 10:30:00",
      "update_time": 1705312200000,
      "update_date": "2024-01-15 10:30:00"
    },
    {
      "id": "version_2",
      "title": "My Agent_2024_01_14_09_00_00",
      "user_canvas_id": "canvas_123",
      "create_time": 1705220400000,
      "create_date": "2024-01-14 09:00:00",
      "update_time": 1705220400000,
      "update_date": "2024-01-14 09:00:00"
    }
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
    "user_canvas_id": "canvas_123",
    "title": "My Agent_2024_01_15_10_30_00",
    "description": null,
    "dsl": {
      "components": {},
      "history": [],
      "messages": [],
      "path": [],
      "answer": []
    },
    "create_time": 1705312200000,
    "create_date": "2024-01-15 10:30:00",
    "update_time": 1705312200000,
    "update_date": "2024-01-15 10:30:00"
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
| `canvas_category` | string | 否 | 类别筛选 (agent_canvas / dataflow_canvas) |
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
    "canvas": [
      {
        "id": "canvas_123",
        "avatar": null,
        "title": "My Agent",
        "dsl": {},
        "description": "A sample agent",
        "permission": "me",
        "tenant_id": "user_123456",
        "nickname": "John Doe",
        "tenant_avatar": null,
        "update_time": 1704067200000,
        "canvas_category": "agent_canvas"
      }
    ],
    "total": 1
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
| `permission` | string | 是 | 权限设置 (me / team) |
| `description` | string | 否 | 描述 |
| `avatar` | string | 否 | 头像 (base64 字符串) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/canvas/setting" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "id": "canvas_123",
           "title": "New Title",
           "permission": "team"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": 1,
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
  "data": {
    "component_1": {
      "start_time": 1704067200.123,
      "end_time": 1704067201.456,
      "inputs": {},
      "outputs": {},
      "status": "success"
    },
    "component_2": {
      "start_time": 1704067201.456,
      "end_time": 1704067202.789,
      "inputs": {},
      "outputs": {},
      "status": "success"
    }
  },
  "message": "success"
}
```

如果没有找到日志:
```json
{
  "code": 0,
  "data": {},
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
| `keywords` | string | 否 | 搜索关键词 |
| `from_date` | string | 否 | 起始日期 |
| `to_date` | string | 否 | 结束日期 |
| `orderby` | string | 否 | 排序字段 (默认 update_time) |
| `desc` | boolean | 否 | 是否倒序 (默认 true) |
| `dsl` | boolean | 否 | 是否包含 DSL (默认 true) |

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
    "sessions": [
      {
        "id": "session_abc123",
        "dialog_id": "canvas_123",
        "user_id": "external_user_1",
        "message": [
          {"role": "user", "content": "Hello", "id": "msg_1"},
          {"role": "assistant", "content": "Hi! How can I help?", "id": "msg_1", "created_at": 1704067200.123}
        ],
        "reference": [],
        "tokens": 150,
        "source": "agent",
        "dsl": {},
        "duration": 2.5,
        "round": 1,
        "thumb_up": 0,
        "errors": null,
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
    "task_analysis": "You are an intelligent assistant...\n\nPlease analyze the following task...",
    "plan_generation": "Based on the analysis, generate a step-by-step plan...",
    "reflection": "Review the previous response and identify...",
    "citation_guidelines": "When citing sources, use the following format..."
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
| `id` | string | 是 | 文件 ID (location) |
| `created_by` | string | 是 | 创建者 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/canvas/download?id=file_location_uuid&created_by=user_123" \
     -H "Authorization: Bearer <YOUR_TOKEN>" \
     -o downloaded_file.pdf
```

### 响应示例
(二进制文件流)
