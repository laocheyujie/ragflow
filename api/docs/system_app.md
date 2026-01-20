# System API 文档

**Base URL**: `http://localhost:9380/v1/system`

**Authentication**:
大部分接口需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取版本信息 (Version)

获取当前应用程序的版本号。

- **URL**: `/version`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/version" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": "0.1.0",
  "message": "success"
}
```

---

## 2. 获取系统状态 (Status)

获取系统各个组件 (ES, Storage, Database, Redis) 的运行状态。

- **URL**: `/status`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/status" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "doc_engine": {
      "status": "green",
      "elapsed": "10.5"
    },
    "storage": {
      "storage": "minio",
      "status": "green",
      "elapsed": "5.2"
    },
    "database": {
      "database": "mysql",
      "status": "green",
      "elapsed": "2.1"
    },
    "redis": {
      "status": "green",
      "elapsed": "1.0"
    },
    "task_executor_heartbeats": {}
  },
  "message": "success"
}
```

---

## 3. 健康检查 (Healthz)

用于容器或负载均衡器的健康检查。

- **URL**: `/healthz`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/healthz"
```

### 响应示例
```json
{
  "status": "ok"
}
```

---

## 4. Ping

简单的连通性测试。

- **URL**: `/ping`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/ping"
```

### 响应示例
```text
pong
```

---

## 5. 创建新 Token (New Token)

生成一个新的 API Token。

- **URL**: `/new_token`
- **Method**: `POST`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | Token 名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/system/new_token?name=my_token" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "tenant_id": "tenant_1",
    "token": "ragflow-xxxxxxxx",
    "beta": "xxxxxxxx",
    "create_time": 1700000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": null,
    "update_date": null
  },
  "message": "success"
}
```

---

## 6. 获取 Token 列表 (Token List)

列出当前用户的所有 API Token。

- **URL**: `/token_list`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/token_list" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "tenant_id": "tenant_1",
      "token": "ragflow-xxxxxxxx",
      "beta": "xxxxxxxx",
      "create_time": 1700000000,
      "create_date": "2024-01-01 12:00:00"
    }
  ],
  "message": "success"
}
```

---

## 7. 删除 Token (Remove Token)

删除指定的 API Token。

- **URL**: `/token/<token>`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `token` | string | 是 | 要删除的 API Token |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/system/token/ragflow-xxxxxxxx" \
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

## 8. 获取系统配置 (Config)

获取系统配置信息（如是否开启注册）。

- **URL**: `/config`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/system/config"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "registerEnabled": true
  },
  "message": "success"
}
```

