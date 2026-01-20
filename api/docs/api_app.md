# API Token API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建 API Token (New Token)

为对话 (Dialog) 或 Agent Canvas 创建一个新的 API Token。

- **URL**: `/new_token`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 否 | Dialog ID (若未提供 canvas_id 则必填) |
| `canvas_id` | string | 否 | Canvas ID (若提供，则是 Agent 模式) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/new_token" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "dialog_id": "dialog_123"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "tenant_id": "tenant_1",
    "token": "generated_token_xxx",
    "dialog_id": "dialog_123",
    "create_time": 1700000000,
    "create_date": "2024-01-01 12:00:00"
  },
  "message": "success"
}
```

---

## 2. 获取 Token 列表 (Token List)

获取指定 Dialog 或 Canvas 的 Token 列表。

- **URL**: `/token_list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 否 | Dialog ID (或 Canvas ID) |
| `canvas_id` | string | 否 | Canvas ID (同 dialog_id) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/token_list?dialog_id=dialog_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "tenant_id": "tenant_1",
      "token": "token_1",
      "dialog_id": "dialog_123"
    }
  ],
  "message": "success"
}
```

---

## 3. 删除 Token (Remove Token)

删除一个或多个 API Token。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tokens` | list[string] | 是 | 要删除的 Token 列表 |
| `tenant_id` | string | 是 | Tenant ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "tokens": ["token_1", "token_2"],
           "tenant_id": "tenant_1"
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

## 4. 获取统计信息 (Stats)

获取对话或 Agent 的统计信息 (PV, UV, Speed, Tokens 等)。

- **URL**: `/stats`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `from_date` | string | 否 | 开始日期 (yyyy-MM-dd HH:mm:ss, 默认7天前) |
| `to_date` | string | 否 | 结束日期 (yyyy-MM-dd HH:mm:ss, 默认现在) |
| `canvas_id` | string | 否 | Canvas ID (若存在则查询 Agent 统计) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/stats?canvas_id=canvas_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "pv": [["2024-01-01 00:00:00", 10]],
    "uv": [["2024-01-01 00:00:00", 5]],
    "speed": [["2024-01-01 00:00:00", 15.5]],
    "tokens": [["2024-01-01 00:00:00", 1.2]],
    "round": [["2024-01-01 00:00:00", 20]],
    "thumb_up": [["2024-01-01 00:00:00", 2]]
  },
  "message": "success"
}
```

---
