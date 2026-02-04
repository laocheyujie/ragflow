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
    "token": "ragflow-xxxxx",
    "dialog_id": "dialog_123",
    "source": null,
    "beta": null,
    "create_time": 1700000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": null,
    "update_date": null
  }
}
```

若传入 `canvas_id`，则 `source` 为 `"agent"`：

```json
{
  "code": 0,
  "data": {
    "tenant_id": "tenant_1",
    "token": "ragflow-xxxxx",
    "dialog_id": "canvas_123",
    "source": "agent",
    "beta": null,
    "create_time": 1700000000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": null,
    "update_date": null
  }
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
      "token": "ragflow-xxxxx",
      "dialog_id": "dialog_123",
      "source": null,
      "beta": null,
      "create_time": 1700000000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1700001000,
      "update_date": "2024-01-01 12:16:40"
    }
  ]
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
  "data": true
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
    "pv": [["2024-01-01", 10], ["2024-01-02", 15]],
    "uv": [["2024-01-01", 5], ["2024-01-02", 8]],
    "speed": [["2024-01-01", 15.5], ["2024-01-02", 18.2]],
    "tokens": [["2024-01-01", 1.2], ["2024-01-02", 2.5]],
    "round": [["2024-01-01", 3.5], ["2024-01-02", 4.2]],
    "thumb_up": [["2024-01-01", 2], ["2024-01-02", 5]]
  }
}
```

### 响应字段说明

| 字段名 | 类型 | 描述 |
| :--- | :--- | :--- |
| `pv` | list | 页面访问量，格式为 `[[日期, 数量], ...]` |
| `uv` | list | 独立访客数，格式为 `[[日期, 数量], ...]` |
| `speed` | list | 平均响应速度 (tokens/秒)，格式为 `[[日期, 速度], ...]` |
| `tokens` | list | Token 消耗量 (千)，格式为 `[[日期, 数量], ...]` |
| `round` | list | 平均对话轮数，格式为 `[[日期, 轮数], ...]` |
| `thumb_up` | list | 点赞数，格式为 `[[日期, 数量], ...]` |

---
