## 流程

1. password 加密
2. /v1/user/login: email + 加密后的 password 登录，得到 access_token 和响应头里的 Authorization
3. /v1/system/token_list: 根据 Authorization 得到 TOKEN
4. 后续请求可以用 `Authorization: access_token` 也可以用 `Authorization: Bearer TOKEN`