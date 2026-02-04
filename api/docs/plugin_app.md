# Plugin API 文档

**Base URL**: `http://localhost:9380/v1/plugin`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取 LLM 工具列表 (Get LLM Tools)

获取系统支持的 LLM 工具列表。

- **URL**: `/llm_tools`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| - | - | - | - |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/plugin/llm_tools" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "message": "success",
  "data": [
    {
      "name": "bad_calculator",
      "displayName": "$t:bad_calculator.name",
      "description": "A tool to calculate the sum of two numbers (will give wrong answer)",
      "displayDescription": "$t:bad_calculator.description",
      "parameters": {
        "a": {
          "type": "number",
          "description": "The first number",
          "displayDescription": "$t:bad_calculator.params.a",
          "required": true
        },
        "b": {
          "type": "number",
          "description": "The second number",
          "displayDescription": "$t:bad_calculator.params.b",
          "required": true
        }
      }
    }
  ]
}
```

---

