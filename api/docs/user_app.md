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
```json
{
  "code": 0,
  "data": {
    "id": "user_id_xxx",
    "email": "user@example.com",
    "nickname": "User Nickname",
    "access_token": "token_xxx",
    "create_time": 1700000000,
    "update_time": 1700000000
  },
  "message": "Welcome back!"
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
```json
{
  "code": 0,
  "data": [
    {
      "channel": "github",
      "display_name": "GitHub",
      "icon": "github_icon_path"
    }
  ],
  "message": "success"
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
Redirect to OAuth provider.

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
Redirect to frontend (e.g., `/?auth=user_id` or `/?error=xxx`).

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
Redirect to frontend.

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
Redirect to frontend.

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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
}
```

---

## 8. 更新设置 (Update Settings)

更新用户信息 (昵称, 邮箱, 密码等)。

- **URL**: `/setting`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: Required

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `nickname` | string | 否 | 新昵称 |
| `email` | string | 否 | 新邮箱 |
| `password` | string | 否 | 当前密码 (若修改密码则必填, 加密) |
| `new_password` | string | 否 | 新密码 (加密) |
| `avatar` | string | 否 | 头像 URL |

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
```json
{
  "code": 0,
  "data": true,
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "id": "user_id_xxx",
    "nickname": "User Nickname",
    "email": "user@example.com"
  },
  "message": "success"
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
```json
{
  "code": 0,
  "data": {
    "id": "new_user_id",
    "email": "new@example.com",
    "nickname": "NewUser"
  },
  "message": "NewUser, welcome aboard!"
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
```json
{
  "code": 0,
  "data": {
    "tenant_id": "user_id",
    "name": "User's Kingdom",
    "llm_id": "gpt-3.5",
    "embd_id": "embedding-model"
  },
  "message": "success"
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
           "tenant_id": "tenant_1",
           "llm_id": "gpt-4",
           "embd_id": "bge-large-zh",
           "asr_id": "whisper-1",
           "img2txt_id": "gpt-4-vision"
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
Returns binary image data (JPEG).

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
           "captcha": "AB12"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true,
  "message": "verification passed, email sent"
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
```json
{
  "code": 0,
  "data": true,
  "message": "otp verified"
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
           "new_password": "encrypted_pwd",
           "confirm_new_password": "encrypted_pwd"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "user_id",
    "email": "user@example.com"
  },
  "message": "Password reset successful. Logged in."
}
```

