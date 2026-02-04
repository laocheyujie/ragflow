# User API 文档

**Base URL**: `http://localhost:9380/v1/user`

**Authentication**:
部分接口需要认证 (See `login_required` in code)。请在 Header 中携带 API Key 或 Session Token：
`Authorization: Bearer <YOUR_ACCESS_TOKEN>`

## 1. 用户登录 (Login)

用户登录接口。

- **URL**: `/login`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 用户邮箱 |
| `password` | string | 是 | 用户密码 (加密后) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/login" \
     -H "Content-Type: application/json" \
     -d '{
           "email": "user@example.com",
           "password": "encrypted_password_xxx"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "Welcome back!",
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "access_token": "f6g7h8i9j0k1l2m3n4o5p6a1b2c3d4e5",
    "nickname": "User Nickname",
    "email": "user@example.com",
    "avatar": "base64_encoded_avatar_string...",
    "language": "English",
    "color_schema": "Bright",
    "timezone": "UTC+8\tAsia/Shanghai",
    "last_login_time": "2024-01-15 10:30:00",
    "is_authenticated": "1",
    "is_active": "1",
    "is_anonymous": "0",
    "login_channel": "password",
    "status": "1",
    "is_superuser": false,
    "create_time": 1700000000000,
    "create_date": "2024-01-01 00:00:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-15 10:30:00"
  }
}
```

**失败响应 (用户未注册):**
```json
{
  "code": 109,
  "message": "Email: user@example.com is not registered!",
  "data": false
}
```

**失败响应 (密码错误):**
```json
{
  "code": 109,
  "message": "Email and password do not match!",
  "data": false
}
```

**失败响应 (账号被禁用):**
```json
{
  "code": 110,
  "message": "This account has been disabled, please contact the administrator!",
  "data": false
}
```

---

## 2. 获取登录渠道 (Login Channels)

获取所有支持的认证渠道。

- **URL**: `/login/channels`
- **Method**: `GET`

### 请求参数 (Query)

无参数。

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/user/login/channels"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": [
    {
      "channel": "github",
      "display_name": "GitHub",
      "icon": "github"
    },
    {
      "channel": "feishu",
      "display_name": "Feishu",
      "icon": "sso"
    }
  ]
}
```

**失败响应:**
```json
{
  "code": 500,
  "message": "Load channels failure, error: ...",
  "data": []
}
```

---

## 3. OAuth 登录 (OAuth Login)

重定向到指定渠道的 OAuth 登录页面。

- **URL**: `/login/<channel>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `channel` | string | 是 | 登录渠道 (e.g., github, feishu) |

### 请求示例
```bash
# 在浏览器中访问
http://localhost:9380/v1/user/login/github
```

### 响应示例
Redirect to OAuth provider authorization URL.

---

## 4. OAuth 回调 (OAuth Callback)

处理 OAuth/OIDC 回调。

- **URL**: `/oauth/callback/<channel>`
- **Method**: `GET`

### 请求参数 (Path/Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `channel` | string | 是 | 登录渠道 (Path 参数) |
| `code` | string | 是 | OAuth 授权码 (Query 参数) |
| `state` | string | 是 | OAuth State (Query 参数) |

### 请求示例
```bash
# 回调 URL 示例
http://localhost:9380/v1/user/oauth/callback/github?code=xyz&state=abc
```

### 响应示例
Redirect to frontend:
- 成功: `/?auth=<user_auth_token>`
- 失败: `/?error=<error_message>`

---

## 5. GitHub 回调 (GitHub Callback - Deprecated)

**Deprecated**: 请使用 `/oauth/callback/<channel>`。

- **URL**: `/github_callback`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `code` | string | 是 | GitHub 授权码 |

### 请求示例
```bash
http://localhost:9380/v1/user/github_callback?code=xyz
```

### 响应示例
Redirect to frontend:
- 成功: `/?auth=<user_auth_token>`
- 失败: `/?error=<error_message>`

---

## 6. 飞书回调 (Feishu Callback)

飞书 OAuth 回调。

- **URL**: `/feishu_callback`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `code` | string | 是 | 飞书授权码 |

### 请求示例
```bash
http://localhost:9380/v1/user/feishu_callback?code=xyz
```

### 响应示例
Redirect to frontend:
- 成功: `/?auth=<user_auth_token>`
- 失败: `/?error=<error_message>`

---

## 7. 登出 (Logout)

用户登出。

- **URL**: `/logout`
- **Method**: `GET`
- **Authentication**: Required

### 请求参数

无参数。

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/user/logout" \
     -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": true
}
```

---

## 8. 更新设置 (Update Settings)

更新用户信息 (昵称, 密码等)。

- **URL**: `/setting`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: Required

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `nickname` | string | 否 | 新昵称 |
| `avatar` | string | 否 | 头像 (base64 编码) |
| `language` | string | 否 | 语言设置 (English/Chinese) |
| `color_schema` | string | 否 | 颜色主题 (Bright/Dark) |
| `timezone` | string | 否 | 时区设置 |
| `password` | string | 否 | 当前密码 (若修改密码则必填, 加密) |
| `new_password` | string | 否 | 新密码 (加密) |

**注意**: 以下字段不可修改: `email`, `status`, `is_superuser`, `login_channel`, `is_anonymous`, `is_active`, `is_authenticated`, `last_login_time`

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/setting" \
     -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "nickname": "New Name"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": true
}
```

**失败响应 (密码错误):**
```json
{
  "code": 109,
  "message": "Password error!",
  "data": false
}
```

**失败响应 (更新失败):**
```json
{
  "code": 500,
  "message": "Update failure!",
  "data": false
}
```

---

## 9. 获取用户信息 (User Profile)

获取当前用户信息。

- **URL**: `/info`
- **Method**: `GET`
- **Authentication**: Required

### 请求参数

无参数。

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/user/info" \
     -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "access_token": "f6g7h8i9j0k1l2m3n4o5p6a1b2c3d4e5",
    "nickname": "User Nickname",
    "email": "user@example.com",
    "avatar": "base64_encoded_avatar_string...",
    "language": "English",
    "color_schema": "Bright",
    "timezone": "UTC+8\tAsia/Shanghai",
    "last_login_time": "2024-01-15 10:30:00",
    "is_authenticated": "1",
    "is_active": "1",
    "is_anonymous": "0",
    "login_channel": "password",
    "status": "1",
    "is_superuser": false,
    "create_time": 1700000000000,
    "create_date": "2024-01-01 00:00:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-15 10:30:00"
  }
}
```

---

## 10. 用户注册 (Register)

注册新用户。

- **URL**: `/register`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `nickname` | string | 是 | 昵称 |
| `email` | string | 是 | 邮箱 |
| `password` | string | 是 | 密码 (加密) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/register" \
     -H "Content-Type: application/json" \
     -d '{
           "nickname": "NewUser",
           "email": "new@example.com",
           "password": "encrypted_password_xxx"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "NewUser, welcome aboard!",
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "access_token": "f6g7h8i9j0k1l2m3n4o5p6a1b2c3d4e5",
    "nickname": "NewUser",
    "email": "new@example.com",
    "avatar": null,
    "language": "English",
    "color_schema": "Bright",
    "timezone": "UTC+8\tAsia/Shanghai",
    "last_login_time": "2024-01-15 10:30:00",
    "is_authenticated": "1",
    "is_active": "1",
    "is_anonymous": "0",
    "login_channel": "password",
    "status": "1",
    "is_superuser": false,
    "create_time": 1700000000000,
    "create_date": "2024-01-15 10:30:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-15 10:30:00"
  }
}
```

**失败响应 (注册已禁用):**
```json
{
  "code": 103,
  "message": "User registration is disabled!",
  "data": false
}
```

**失败响应 (邮箱格式无效):**
```json
{
  "code": 103,
  "message": "Invalid email address: invalid_email!",
  "data": false
}
```

**失败响应 (邮箱已注册):**
```json
{
  "code": 103,
  "message": "Email: new@example.com has already registered!",
  "data": false
}
```

**失败响应 (注册失败):**
```json
{
  "code": 500,
  "message": "User registration failure, error: ...",
  "data": false
}
```

---

## 11. 获取租户信息 (Tenant Info)

获取当前用户的租户信息。

- **URL**: `/tenant_info`
- **Method**: `GET`
- **Authentication**: Required

### 请求参数

无参数。

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/user/tenant_info" \
     -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>"
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "tenant_id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "name": "User's Kingdom",
    "llm_id": "deepseek-chat@DeepSeek",
    "embd_id": "BAAI/bge-large-zh-v1.5@Xinference",
    "rerank_id": "BAAI/bge-reranker-v2-m3@Xinference",
    "asr_id": "whisper-1@OpenAI",
    "img2txt_id": "gpt-4o@OpenAI",
    "tts_id": null,
    "parser_ids": "naive,qa,resume,manual,table,paper,book,laws,presentation,one,knowledge_graph,email,picture,tag",
    "role": "owner"
  }
}
```

**失败响应 (租户不存在):**
```json
{
  "code": 101,
  "message": "Tenant not found!",
  "data": null
}
```

---

## 12. 设置租户信息 (Set Tenant Info)

更新租户的模型配置。

- **URL**: `/set_tenant_info`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: Required

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `tenant_id` | string | 是 | 租户 ID |
| `llm_id` | string | 是 | LLM ID |
| `embd_id` | string | 是 | Embedding Model ID |
| `asr_id` | string | 是 | ASR Model ID |
| `img2txt_id` | string | 是 | Image2Text Model ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/set_tenant_info" \
     -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
           "tenant_id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
           "llm_id": "gpt-4@OpenAI",
           "embd_id": "text-embedding-3-small@OpenAI",
           "asr_id": "whisper-1@OpenAI",
           "img2txt_id": "gpt-4o@OpenAI"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "success",
  "data": true
}
```

**失败响应:**
```json
{
  "code": 500,
  "message": "Exception error message...",
  "data": null
}
```

---

## 13. 获取验证码 (Forget Password - Captcha)

获取重置密码用的图片验证码。

- **URL**: `/forget/captcha`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 用户邮箱 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/user/forget/captcha?email=user@example.com"
```

### 响应示例

**成功响应:**
Returns binary image data (JPEG, Content-Type: image/JPEG).

**失败响应 (缺少邮箱):**
```json
{
  "code": 102,
  "message": "email is required",
  "data": false
}
```

**失败响应 (邮箱无效):**
```json
{
  "code": 101,
  "message": "invalid email",
  "data": false
}
```

---

## 14. 发送 OTP (Forget Password - Send OTP)

验证图片验证码并发送邮件 OTP。

- **URL**: `/forget/otp`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 用户邮箱 |
| `captcha` | string | 是 | 图片验证码内容 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/forget/otp" \
     -H "Content-Type: application/json" \
     -d '{
           "email": "user@example.com",
           "captcha": "AB12CD"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "verification passed, email sent",
  "data": true
}
```

**失败响应 (缺少参数):**
```json
{
  "code": 102,
  "message": "email and captcha required",
  "data": false
}
```

**失败响应 (邮箱无效):**
```json
{
  "code": 101,
  "message": "invalid email",
  "data": false
}
```

**失败响应 (验证码无效或过期):**
```json
{
  "code": 104,
  "message": "invalid or expired captcha",
  "data": false
}
```

**失败响应 (验证码错误):**
```json
{
  "code": 109,
  "message": "invalid or expired captcha",
  "data": false
}
```

**失败响应 (冷却时间):**
```json
{
  "code": 104,
  "message": "you still have to wait 45 seconds",
  "data": false
}
```

**失败响应 (发送失败):**
```json
{
  "code": 100,
  "message": "failed to send email",
  "data": false
}
```

---

## 15. 验证 OTP (Forget Password - Verify OTP)

验证邮件 OTP。

- **URL**: `/forget/verify-otp`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 用户邮箱 |
| `otp` | string | 是 | 邮件 OTP |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/forget/verify-otp" \
     -H "Content-Type: application/json" \
     -d '{
           "email": "user@example.com",
           "otp": "ABCDEF"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "otp verified",
  "data": true
}
```

**失败响应 (缺少参数):**
```json
{
  "code": 102,
  "message": "email and otp are required",
  "data": false
}
```

**失败响应 (邮箱无效):**
```json
{
  "code": 101,
  "message": "invalid email",
  "data": false
}
```

**失败响应 (尝试次数过多):**
```json
{
  "code": 104,
  "message": "too many attempts, try later",
  "data": false
}
```

**失败响应 (OTP 过期):**
```json
{
  "code": 104,
  "message": "expired otp",
  "data": false
}
```

**失败响应 (OTP 错误):**
```json
{
  "code": 109,
  "message": "expired otp",
  "data": false
}
```

**失败响应 (存储错误):**
```json
{
  "code": 500,
  "message": "otp storage corrupted",
  "data": false
}
```

---

## 16. 重置密码 (Forget Password - Reset Password)

验证 OTP 通过后重置密码。

- **URL**: `/forget/reset-password`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `email` | string | 是 | 用户邮箱 |
| `new_password` | string | 是 | 新密码 (加密) |
| `confirm_new_password` | string | 是 | 确认新密码 (加密) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/user/forget/reset-password" \
     -H "Content-Type: application/json" \
     -d '{
           "email": "user@example.com",
           "new_password": "encrypted_new_pwd",
           "confirm_new_password": "encrypted_new_pwd"
         }'
```

### 响应示例

**成功响应:**
```json
{
  "code": 0,
  "message": "Password reset successful. Logged in.",
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "access_token": "f6g7h8i9j0k1l2m3n4o5p6a1b2c3d4e5",
    "nickname": "User Nickname",
    "email": "user@example.com",
    "avatar": "base64_encoded_avatar_string...",
    "language": "English",
    "color_schema": "Bright",
    "timezone": "UTC+8\tAsia/Shanghai",
    "last_login_time": "2024-01-15 10:30:00",
    "is_authenticated": "1",
    "is_active": "1",
    "is_anonymous": "0",
    "login_channel": "password",
    "status": "1",
    "is_superuser": false,
    "create_time": 1700000000000,
    "create_date": "2024-01-01 00:00:00",
    "update_time": 1700000000000,
    "update_date": "2024-01-15 10:30:00"
  }
}
```

**失败响应 (邮箱未验证):**
```json
{
  "code": 109,
  "message": "email not verified",
  "data": false
}
```

**失败响应 (缺少参数):**
```json
{
  "code": 102,
  "message": "email and passwords are required",
  "data": false
}
```

**失败响应 (密码不匹配):**
```json
{
  "code": 102,
  "message": "passwords do not match",
  "data": false
}
```

**失败响应 (邮箱无效):**
```json
{
  "code": 101,
  "message": "invalid email",
  "data": false
}
```

**失败响应 (重置失败):**
```json
{
  "code": 500,
  "message": "failed to reset password",
  "data": false
}
```
