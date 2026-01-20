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
  "data": [
    {
      "name": "calculator",
      "displayName": "Calculator",
      "description": "Perform basic arithmetic operations.",
      "displayDescription": "计算器",
      "parameters": {
        "expression": {
          "type": "string",
          "description": "Mathematical expression to evaluate.",
          "displayDescription": "数学表达式",
          "required": true
        }
      }
    },
    {
      "name": "google_search",
      "displayName": "Google Search",
      "description": "Search for information on the internet.",
      "displayDescription": "谷歌搜索",
      "parameters": {
        "query": {
          "type": "string",
          "description": "The search query.",
          "displayDescription": "搜索关键词",
          "required": true
        }
      }
    }
  ],
  "message": "success"
}
```

---

