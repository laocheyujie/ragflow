# LLM Management API 文档

**Base URL**: `http://localhost:9380/v1/llm`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取 LLM 工厂列表 (List Factories)

获取系统支持的 LLM 工厂及其支持的模型类型列表。

- **URL**: `/factories`
- **Method**: `GET`

### 请求参数 (Query)
无

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/llm/factories" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "name": "OpenAI",
      "logo": "base64_string...",
      "tags": "LLM, Text Embedding, Image2Text, TTS",
      "status": "1",
      "model_types": ["chat", "embedding", "image2text", "tts"]
    },
    {
      "name": "VolcEngine",
      "logo": "base64_string...",
      "tags": "LLM, Text Embedding, Rerank",
      "status": "1",
      "model_types": ["chat", "embedding", "rerank"]
    },
    {
      "name": "Ollama",
      "logo": "base64_string...",
      "tags": "LLM, Text Embedding, Image2Text",
      "status": "1",
      "model_types": ["chat", "embedding", "image2text", "speech2text", "rerank", "tts", "ocr"]
    }
  ]
}
```

---

## 2. 设置 API Key (Set API Key)

为特定的 LLM 工厂设置 API Key，并测试其可用性。通常建议使用 `/add_llm` 接口，因为它处理了不同工厂的字段组合逻辑。

- **URL**: `/set_api_key`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `llm_factory` | string | 是 | LLM 工厂名称 |
| `api_key` | string | 是 | 完整的 API Key (可能是 JSON 字符串) |
| `base_url` | string | 否 | Base URL |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/llm/set_api_key" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "llm_factory": "OpenAI",
           "api_key": "sk-xxxxxx"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

### 错误响应示例
```json
{
  "code": 102,
  "message": "\nFail to access embedding model(text-embedding-3-small) using this api key.Invalid API key provided."
}
```

---

## 3. 添加 LLM (Add LLM)

添加或配置一个新的 LLM 模型。根据不同的 `llm_factory`，可能需要提供不同的认证字段（这些字段会组合成 `api_key`）。

- **URL**: `/add_llm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `llm_factory` | string | 是 | LLM 工厂名称 (e.g., "OpenAI", "VolcEngine") |
| `llm_name` | string | 否 | 模型名称 (若不提供，部分工厂会自动生成) |
| `model_type` | string | 是 | 模型类型 (chat, embedding, rerank, etc.) |
| `api_key` | string | 否 | API Key (OpenAI 等通用工厂必填) |
| `api_base` | string | 否 | API Base URL |
| `max_tokens` | integer | 否 | 最大 Token 数 |
| `ark_api_key` | string | 否 | VolcEngine 专用 |
| `endpoint_id` | string | 否 | VolcEngine 专用 |
| `hunyuan_sid` | string | 否 | Tencent Hunyuan 专用 |
| `hunyuan_sk` | string | 否 | Tencent Hunyuan 专用 |
| `...` | ... | 否 | 其他工厂专用字段 |

> 注意：对于需要多个认证字段的工厂 (如 VolcEngine, Tencent)，请直接将这些字段放在 JSON 根节点中，后端会自动组合。

### 请求示例 (OpenAI)
```bash
curl -X POST "http://localhost:9380/v1/llm/add_llm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "llm_factory": "OpenAI",
           "llm_name": "gpt-3.5-turbo",
           "model_type": "chat",
           "api_key": "sk-xxxxxx"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": true
}
```

### 错误响应示例
```json
{
  "code": 102,
  "message": "LLM factory InvalidFactory is not allowed"
}
```

```json
{
  "code": 102,
  "message": "\nFail to access model(OpenAI/gpt-4o).Invalid API key provided."
}
```

---

## 4. 删除 LLM (Delete LLM)

删除已配置的 LLM 模型。

- **URL**: `/delete_llm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `llm_factory` | string | 是 | LLM 工厂名称 |
| `llm_name` | string | 是 | 模型名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/llm/delete_llm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "llm_factory": "OpenAI",
           "llm_name": "gpt-3.5-turbo"
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

## 5. 启用/禁用 LLM (Enable/Disable LLM)

切换 LLM 模型的启用状态。

- **URL**: `/enable_llm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `llm_factory` | string | 是 | LLM 工厂名称 |
| `llm_name` | string | 是 | 模型名称 |
| `status` | string | 否 | 状态 "1" (启用) 或 "0" (禁用)，默认为 "1" |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/llm/enable_llm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "llm_factory": "OpenAI",
           "llm_name": "gpt-3.5-turbo",
           "status": "1"
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

## 6. 删除工厂配置 (Delete Factory)

删除该租户下某个工厂的所有 LLM 配置。

- **URL**: `/delete_factory`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `llm_factory` | string | 是 | LLM 工厂名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/llm/delete_factory" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "llm_factory": "VolcEngine"
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

## 7. 获取我的 LLM 列表 (My LLMs)

获取当前用户（租户）配置的所有 LLM 模型。

- **URL**: `/my_llms`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `include_details` | string | 否 | 是否包含详细信息 (true/false)，默认为 false |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/llm/my_llms?include_details=true" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例 (include_details=false，默认)
```json
{
  "code": 0,
  "data": {
    "OpenAI": {
      "tags": "LLM, Text Embedding, Image2Text, TTS",
      "llm": [
        {
          "type": "chat",
          "name": "gpt-4o",
          "used_token": 15000,
          "status": "1"
        },
        {
          "type": "embedding",
          "name": "text-embedding-3-small",
          "used_token": 5000,
          "status": "1"
        }
      ]
    },
    "VolcEngine": {
      "tags": "LLM, Text Embedding, Rerank",
      "llm": [
        {
          "type": "chat",
          "name": "doubao-pro-32k",
          "used_token": 2000,
          "status": "1"
        }
      ]
    }
  }
}
```

### 响应示例 (include_details=true)
```json
{
  "code": 0,
  "data": {
    "OpenAI": {
      "tags": "LLM, Text Embedding, Image2Text, TTS",
      "llm": [
        {
          "type": "chat",
          "name": "gpt-4o",
          "used_token": 15000,
          "api_base": "https://api.openai.com/v1",
          "max_tokens": 128000,
          "status": "1"
        },
        {
          "type": "embedding",
          "name": "text-embedding-3-small",
          "used_token": 5000,
          "api_base": "",
          "max_tokens": 8191,
          "status": "1"
        }
      ]
    },
    "Ollama": {
      "tags": "LLM, Text Embedding, Image2Text",
      "llm": [
        {
          "type": "chat",
          "name": "llama3.1:8b",
          "used_token": 0,
          "api_base": "http://localhost:11434",
          "max_tokens": 8192,
          "status": "1"
        }
      ]
    }
  }
}
```

---

## 8. 获取可用 LLM 列表 (List LLMs)

获取所有可用的 LLM 模型，包括系统内置的和用户配置的。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `model_type` | string | 否 | 筛选模型类型 (e.g., "chat", "embedding") |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/llm/list?model_type=chat" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "OpenAI": [
      {
        "llm_name": "gpt-4o",
        "model_type": "chat",
        "fid": "OpenAI",
        "max_tokens": 128000,
        "tags": "LLM, 128k",
        "is_tools": true,
        "status": "1",
        "available": true
      },
      {
        "llm_name": "gpt-4o-mini",
        "model_type": "chat",
        "fid": "OpenAI",
        "max_tokens": 128000,
        "tags": "LLM, 128k",
        "is_tools": true,
        "status": "1",
        "available": true
      },
      {
        "llm_name": "text-embedding-3-small",
        "model_type": "embedding",
        "fid": "OpenAI",
        "max_tokens": 8191,
        "tags": "Text Embedding",
        "is_tools": false,
        "status": "1",
        "available": true
      }
    ],
    "Ollama": [
      {
        "llm_name": "llama3.1:8b",
        "model_type": "chat",
        "fid": "Ollama",
        "available": true,
        "status": "1"
      }
    ],
    "Builtin": [
      {
        "llm_name": "flag-embedding",
        "model_type": "embedding",
        "fid": "Builtin",
        "max_tokens": 8192,
        "tags": "Text Embedding",
        "is_tools": false,
        "status": "1",
        "available": true
      }
    ]
  }
}
```

### 响应示例 (筛选 model_type=embedding)
```json
{
  "code": 0,
  "data": {
    "OpenAI": [
      {
        "llm_name": "text-embedding-3-small",
        "model_type": "embedding",
        "fid": "OpenAI",
        "max_tokens": 8191,
        "tags": "Text Embedding",
        "is_tools": false,
        "status": "1",
        "available": true
      },
      {
        "llm_name": "text-embedding-3-large",
        "model_type": "embedding",
        "fid": "OpenAI",
        "max_tokens": 8191,
        "tags": "Text Embedding",
        "is_tools": false,
        "status": "1",
        "available": true
      }
    ],
    "Builtin": [
      {
        "llm_name": "flag-embedding",
        "model_type": "embedding",
        "fid": "Builtin",
        "max_tokens": 8192,
        "tags": "Text Embedding",
        "is_tools": false,
        "status": "1",
        "available": true
      }
    ]
  }
}
```

