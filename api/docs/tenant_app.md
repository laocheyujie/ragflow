# Tenant API 文档

**Base URL**: `http://localhost:9380/v1/tenant`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取租户用户列表 (User List)

获取指定租户下的用户列表。

- **URL**: `/<tenant_id>/user/list`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tenant_id` | string | 是 | Tenant ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/tenant/tenant_1/user/list" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "data": [
    {
      "id": "5c99a15c4e6011efb0c80242ac120006",
      "user_id": "a1b2c3d4e5f6",
      "status": "1",
      "role": "normal",
      "nickname": "Alice",
      "email": "alice@example.com",
      "avatar": "base64_string...",
      "is_authenticated": "1",
      "is_active": "1",
      "is_anonymous": "0",
      "update_date": "2024-01-01 12:00:00",
      "is_superuser": false,
      "delta_seconds": 120
    }
  ],
  "message": "success"
}
```

**失败响应 (无权限):**
```json
{
  "code": 109,
  "data": false,
  "message": "No authorization."
}
```

---

## 2. 邀请用户 (Invite User)

邀请用户加入租户。

- **URL**: `/<tenant_id>/user`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tenant_id` | string | 是 | Tenant ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 被邀请人的邮箱 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/tenant/tenant_1/user" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "email": "bob@example.com"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "data": {
    "id": "b2c3d4e5f6a7",
    "avatar": "base64_string...",
    "email": "bob@example.com",
    "nickname": "Bob"
  },
  "message": "success"
}
```

**失败响应 (无权限):**
```json
{
  "code": 109,
  "data": false,
  "message": "No authorization."
}
```

**失败响应 (用户不存在):**
```json
{
  "code": 102,
  "data": false,
  "message": "User not found."
}
```

**失败响应 (用户已在团队中):**
```json
{
  "code": 102,
  "data": false,
  "message": "bob@example.com is already in the team."
}
```

**失败响应 (邀请邮件发送失败):**
```json
{
  "code": 100,
  "data": false,
  "message": "Failed to send invite email."
}
```

---

## 3. 移除用户 (Remove User)

移除租户下的用户。

- **URL**: `/<tenant_id>/user/<user_id>`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tenant_id` | string | 是 | Tenant ID |
| `user_id` | string | 是 | User ID |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/tenant/tenant_1/user/user_2" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

**失败响应 (无权限):**
```json
{
  "code": 109,
  "data": false,
  "message": "No authorization."
}
```

---

## 4. 获取租户列表 (Tenant List)

获取当前用户所属的租户列表。

- **URL**: `/list`
- **Method**: `GET`

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/tenant/list" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "data": [
    {
      "tenant_id": "a1b2c3d4e5f6",
      "role": "normal",
      "nickname": "Team Owner",
      "email": "owner@example.com",
      "avatar": "base64_string...",
      "update_date": "2024-01-01 12:00:00",
      "delta_seconds": 3600
    }
  ],
  "message": "success"
}
```

---

## 5. 同意加入 (Agree Join)

同意加入租户。

- **URL**: `/agree/<tenant_id>`
- **Method**: `PUT`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tenant_id` | string | 是 | Tenant ID |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/tenant/agree/tenant_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```
