# Connector API 文档

**Base URL**: `http://localhost:9380/v1/connector`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 设置 Connector (Set Connector)

创建或更新 Connector 配置。如果不传 `id` 则为创建，传 `id` 则为更新。

- **URL**: `/set`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `id` | string | 否 | Connector ID。若提供则为更新操作，否则为创建。 |
| `name` | string | 是 (创建时) | Connector 名称。 |
| `source` | string | 是 (创建时) | 数据源类型 (e.g., "google_drive", "gmail")。 |
| `config` | object | 是 | 数据源配置信息 (JSON Object)。 |
| `refresh_freq` | int | 否 | 刷新频率 (秒)，默认 30。 |
| `prune_freq` | int | 否 | 清理频率 (秒)，默认 720。 |
| `timeout_secs` | int | 否 | 超时时间 (秒)，默认 1740。 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/set" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Drive Connector",
           "source": "google_drive",
           "config": {"folder_id": "xxx", "api_key": "xxx"},
           "refresh_freq": 60
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "tenant_id": "tenant_abc123",
    "name": "My Drive Connector",
    "source": "google_drive",
    "input_type": "poll",
    "config": {"folder_id": "xxx", "credentials": {}},
    "refresh_freq": 60,
    "prune_freq": 720,
    "timeout_secs": 1740,
    "indexing_start": null,
    "status": "schedule",
    "create_time": 1706150400000,
    "create_date": "2024-01-25 08:00:00",
    "update_time": 1706150400000,
    "update_date": "2024-01-25 08:00:00"
  },
  "message": "success"
}
```

---

## 2. 获取 Connector 列表 (List Connectors)

获取当前用户的 Connector 列表。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/connector/list" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "a1b2c3d4e5f6789012345678",
      "name": "My Drive Connector",
      "source": "google_drive",
      "status": "schedule"
    },
    {
      "id": "b2c3d4e5f67890123456789a",
      "name": "Gmail Connector",
      "source": "gmail",
      "status": "running"
    }
  ],
  "message": "success"
}
```

---

## 3. 获取 Connector 详情 (Get Connector)

根据 ID 获取 Connector 详情。

- **URL**: `/<connector_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `connector_id` | string | 是 | Connector ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/connector/connector_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6789012345678",
    "tenant_id": "tenant_abc123",
    "name": "My Drive Connector",
    "source": "google_drive",
    "input_type": "poll",
    "config": {"folder_id": "xxx", "credentials": {}},
    "refresh_freq": 60,
    "prune_freq": 720,
    "timeout_secs": 1740,
    "indexing_start": null,
    "status": "schedule",
    "create_time": 1706150400000,
    "create_date": "2024-01-25 08:00:00",
    "update_time": 1706150400000,
    "update_date": "2024-01-25 08:00:00"
  },
  "message": "success"
}
```

---

## 4. 获取同步日志 (List Logs)

获取 Connector 的同步任务日志。

- **URL**: `/<connector_id>/logs`
- **Method**: `GET`

### 请求参数 (Path & Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `connector_id` | string | 是 | Connector ID (Path Param) |
| `page` | int | 否 | 页码，默认 1 |
| `page_size` | int | 否 | 每页条数，默认 15 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/connector/connector_123/logs?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 100,
    "logs": [
      {
        "id": "log_a1b2c3d4e5f6789012345678",
        "connector_id": "a1b2c3d4e5f6789012345678",
        "kb_id": "kb_abc123def456",
        "update_date": "2024-01-25 12:00:00",
        "poll_range_start": "2024-01-01T00:00:00+00:00",
        "poll_range_end": "2024-01-25T12:00:00+00:00",
        "new_docs_indexed": 10,
        "total_docs_indexed": 150,
        "error_msg": "",
        "full_exception_trace": "",
        "error_count": 0,
        "name": "My Drive Connector",
        "source": "google_drive",
        "tenant_id": "tenant_abc123",
        "timeout_secs": 1740,
        "kb_name": "My Knowledge Base",
        "kb_avatar": null,
        "auto_parse": "1",
        "reindex": "0",
        "status": "done",
        "update_time": 1706184000000
      }
    ]
  },
  "message": "success"
}
```

---

## 5. 暂停/恢复/取消 Connector (Resume/Pause)

修改 Connector 的运行状态。

- **URL**: `/<connector_id>/resume`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `resume` | boolean | 否 | `true` 为恢复/启动 (SCHEDULE)，`false` 为取消/停止 (CANCEL)。默认为 `false`。 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/connector/connector_123/resume" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "resume": true
         }'
```

### 响应示例

**成功:**
```json
{
  "code": 0,
  "data": true
}
```

---

## 6. 重建索引 (Rebuild)

触发 Connector 对应知识库的索引重建。

- **URL**: `/<connector_id>/rebuild`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `kb_id` | string | 是 | 知识库 ID |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/connector/connector_123/rebuild" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "kb_id": "kb_456"
         }'
```

### 响应示例

**成功:**
```json
{
  "code": 0,
  "data": true
}
```

**失败:**
```json
{
  "code": 100,
  "data": false,
  "message": "Error message describing the failure"
}
```

---

## 7. 删除 Connector (Remove)

删除指定的 Connector。

- **URL**: `/<connector_id>/rm`
- **Method**: `POST`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `connector_id` | string | 是 | Connector ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/connector_123/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

---

## 8. 启动 Google OAuth (Start Google OAuth)

发起 Google Drive 或 Gmail 的 OAuth 授权流程 (Web 端)。

- **URL**: `/google/oauth/web/start`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query & Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `type` | string | 否 | OAuth 类型，可选 `google-drive` (默认) 或 `gmail` (Query Param)。 |
| `credentials` | object/string | 是 | Google OAuth 客户端凭证 (Body Param)。 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/google/oauth/web/start?type=google-drive" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "credentials": { "web": { "client_id": "...", "client_secret": "..." } }
         }'
```

### 响应示例

**成功:**
```json
{
  "code": 0,
  "data": {
    "flow_id": "550e8400-e29b-41d4-a716-446655440000",
    "authorization_url": "https://accounts.google.com/o/oauth2/v2/auth?response_type=code&client_id=xxx.apps.googleusercontent.com&redirect_uri=https%3A%2F%2Fexample.com%2Fcallback&scope=...&state=550e8400-e29b-41d4-a716-446655440000&access_type=offline&include_granted_scopes=true&prompt=consent",
    "expires_in": 900
  }
}
```

**错误 (凭证已包含 refresh_token):**
```json
{
  "code": 102,
  "message": "Uploaded credentials already include a refresh token."
}
```

**错误 (缺少 web 配置):**
```json
{
  "code": 102,
  "message": "Google OAuth JSON must include a 'web' client configuration to use browser-based authorization."
}
```

---

## 9. Google Gmail OAuth 回调 (Gmail Callback)

Google OAuth 授权完成后的回调接口 (通常由浏览器重定向调用)。

- **URL**: `/gmail/oauth/web/callback`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `state` | string | 是 | OAuth State ID (Flow ID)。 |
| `code` | string | 是 | 授权码。 |
| `error` | string | 否 | 错误信息。 |

### 响应
返回 HTML 页面，提示授权成功或失败，并自动关闭窗口。

**成功示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Google Gmail Authorization</title></head>
<body>
  <h1>Authorization complete</h1>
  <p>Authorization completed successfully.</p>
  <script>
    window.opener.postMessage({
      "type": "ragflow-gmail-oauth",
      "status": "success",
      "flowId": "550e8400-e29b-41d4-a716-446655440000",
      "message": "Authorization completed successfully."
    }, "*");
    window.close();
  </script>
</body>
</html>
```

**失败示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Google Gmail Authorization</title></head>
<body>
  <h1>Authorization failed</h1>
  <p>Authorization session expired. Please restart from the main window.</p>
</body>
</html>
```

---

## 10. Google Drive OAuth 回调 (Drive Callback)

Google OAuth 授权完成后的回调接口 (通常由浏览器重定向调用)。

- **URL**: `/google-drive/oauth/web/callback`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `state` | string | 是 | OAuth State ID (Flow ID)。 |
| `code` | string | 是 | 授权码。 |
| `error` | string | 否 | 错误信息。 |

### 响应
返回 HTML 页面，提示授权成功或失败，并自动关闭窗口。

**成功示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Google Drive Authorization</title></head>
<body>
  <h1>Authorization complete</h1>
  <p>Authorization completed successfully.</p>
  <script>
    window.opener.postMessage({
      "type": "ragflow-google-drive-oauth",
      "status": "success",
      "flowId": "550e8400-e29b-41d4-a716-446655440000",
      "message": "Authorization completed successfully."
    }, "*");
    window.close();
  </script>
</body>
</html>
```

**失败示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Google Drive Authorization</title></head>
<body>
  <h1>Authorization failed</h1>
  <p>Missing authorization code from Google.</p>
</body>
</html>
```

---

## 11. 轮询 Google OAuth 结果 (Poll Google Result)

前端轮询获取 Google OAuth 的授权结果 (凭证)。

- **URL**: `/google/oauth/web/result`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query & Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `type` | string | 否 | OAuth 类型，可选 `google-drive` 或 `gmail` (Query Param)。 |
| `flow_id` | string | 是 | 授权流程 ID (Body Param)。 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/google/oauth/web/result?type=google-drive" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "flow_id": "uuid_flow_id"
         }'
```

### 响应示例

**成功 (授权已完成):**
```json
{
  "code": 0,
  "data": {
    "credentials": "{\"token\": \"ya29.xxx\", \"refresh_token\": \"1//xxx\", \"token_uri\": \"https://oauth2.googleapis.com/token\", \"client_id\": \"xxx.apps.googleusercontent.com\", \"client_secret\": \"xxx\", \"scopes\": [\"https://www.googleapis.com/auth/drive.readonly\"]}"
  }
}
```

**等待中 (授权尚未完成):**
```json
{
  "code": 110,
  "message": "Authorization is still pending."
}
```

**权限错误:**
```json
{
  "code": 109,
  "message": "You are not allowed to access this authorization result."
}
```

---

## 12. 启动 Box OAuth (Start Box OAuth)

发起 Box 的 OAuth 授权流程。

- **URL**: `/box/oauth/web/start`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `client_id` | string | 是 | Box Client ID。 |
| `client_secret` | string | 是 | Box Client Secret。 |
| `redirect_uri` | string | 否 | 重定向 URI。 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/box/oauth/web/start" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "client_id": "box_client_id",
           "client_secret": "box_client_secret"
         }'
```

### 响应示例

**成功:**
```json
{
  "code": 0,
  "data": {
    "flow_id": "550e8400-e29b-41d4-a716-446655440000",
    "authorization_url": "https://account.box.com/api/oauth2/authorize?response_type=code&client_id=xxx&redirect_uri=https%3A%2F%2Fexample.com%2Fcallback&state=550e8400-e29b-41d4-a716-446655440000",
    "expires_in": 900
  }
}
```

**错误 (缺少必要参数):**
```json
{
  "code": 102,
  "message": "Box client_id and client_secret are required."
}
```

---

## 13. Box OAuth 回调 (Box Callback)

Box OAuth 授权完成后的回调接口。

- **URL**: `/box/oauth/web/callback`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `state` | string | 是 | OAuth State ID (Flow ID)。 |
| `code` | string | 是 | 授权码。 |
| `error` | string | 否 | 错误信息。 |

### 响应
返回 HTML 页面，提示授权成功或失败，并自动关闭窗口。

**成功示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Box Authorization</title></head>
<body>
  <h1>Authorization complete</h1>
  <p>Authorization completed successfully.</p>
  <script>
    window.opener.postMessage({
      "type": "ragflow-box-oauth",
      "status": "success",
      "flowId": "550e8400-e29b-41d4-a716-446655440000",
      "message": "Authorization completed successfully."
    }, "*");
    window.close();
  </script>
</body>
</html>
```

**失败示例 (HTML):**
```html
<!DOCTYPE html>
<html>
<head><title>Box Authorization</title></head>
<body>
  <h1>Authorization failed</h1>
  <p>Missing authorization code from Box.</p>
</body>
</html>
```

---

## 14. 轮询 Box OAuth 结果 (Poll Box Result)

前端轮询获取 Box OAuth 的授权结果 (凭证)。

- **URL**: `/box/oauth/web/result`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `flow_id` | string | 是 | 授权流程 ID。 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/connector/box/oauth/web/result" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "flow_id": "uuid_flow_id"
         }'
```

### 响应示例

**成功 (授权已完成):**
```json
{
  "code": 0,
  "data": {
    "credentials": {
      "user_id": "user_abc123def456",
      "client_id": "box_client_id_xxx",
      "client_secret": "box_client_secret_xxx",
      "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
      "refresh_token": "abc123def456ghi789..."
    }
  }
}
```

**等待中 (授权尚未完成):**
```json
{
  "code": 110,
  "message": "Authorization is still pending."
}
```

**权限错误:**
```json
{
  "code": 109,
  "message": "You are not allowed to access this authorization result."
}
```

