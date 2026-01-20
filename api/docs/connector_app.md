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
    "id": "connector_123",
    "name": "My Drive Connector",
    "source": "google_drive",
    "status": "1",
    "config": {"folder_id": "xxx", "api_key": "xxx"},
    "refresh_freq": 60,
    "prune_freq": 720,
    "timeout_secs": 1740,
    "tenant_id": "tenant_1"
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
      "id": "connector_123",
      "name": "My Drive Connector",
      "source": "google_drive",
      "status": "1"
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
    "id": "connector_123",
    "name": "My Drive Connector",
    "source": "google_drive",
    "config": { ... }
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
        "id": "log_1",
        "connector_id": "connector_123",
        "status": "success",
        "start_time": "2024-01-01 12:00:00"
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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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
  "data": true,
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "flow_id": "uuid_flow_id",
    "authorization_url": "https://accounts.google.com/o/oauth2/v2/auth?...",
    "expires_in": 900
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "credentials": { "token": "...", "refresh_token": "..." }
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "flow_id": "uuid_flow_id",
    "authorization_url": "https://account.box.com/api/oauth2/authorize?...",
    "expires_in": 900
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "credentials": {
      "user_id": "...",
      "client_id": "...",
      "access_token": "...",
      "refresh_token": "..."
    }
  },
  "message": "success"
}
```

