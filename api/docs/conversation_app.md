# Conversation API 文档

**Base URL**: `http://localhost:9380/v1/conversation`

**Authentication**:
大部分接口均需要认证（登录态）。
特殊接口如 `/getsse` 需要在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 设置/创建会话 (Set Conversation)

创建新会话或更新现有会话信息。

- **URL**: `/set`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_id` | string | 是 | 会话 ID (客户端生成或现有 ID) |
| `is_new` | boolean | 是 | 是否为新会话 |
| `name` | string | 否 | 会话名称 (默认为 "New conversation") |
| `dialog_id` | string | 否 | Dialog ID (创建新会话时必填) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/set" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_id": "conv_123",
           "is_new": true,
           "name": "My Chat",
           "dialog_id": "dialog_456"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "conv_123",
    "dialog_id": "dialog_456",
    "name": "My Chat",
    "message": [{"role": "assistant", "content": "Hello!"}],
    "user_id": "user_1",
    "reference": []
  },
  "message": "success"
}
```

---

## 2. 获取会话详情 (Get Conversation)

获取指定会话的详细信息。

- **URL**: `/get`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_id` | string | 是 | 会话 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/conversation/get?conversation_id=conv_123"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "conv_123",
    "name": "My Chat",
    "message": [...],
    "avatar": "base64_string_or_url"
  },
  "message": "success"
}
```

---

## 3. 获取 SSE 会话信息 (Get SSE)

通过 API Token 获取会话的基本信息（通常用于外部集成）。

- **URL**: `/getsse/<dialog_id>`
- **Method**: `GET`
- **Authentication**: `Authorization: Bearer <API_TOKEN>`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | Dialog ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/conversation/getsse/dialog_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "dialog_123",
    "name": "Assistant",
    "avatar": "..."
  },
  "message": "success"
}
```

---

## 4. 删除会话 (Remove Conversation)

删除一个或多个会话。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_ids` | list[string] | 是 | 要删除的会话 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/rm" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_ids": ["conv_123", "conv_124"]
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

## 5. 获取会话列表 (List Conversations)

获取指定 Dialog 下的会话列表。

- **URL**: `/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | Dialog ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/conversation/list?dialog_id=dialog_123"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "conv_123",
      "name": "Chat 1",
      "create_time": "..."
    }
  ],
  "message": "success"
}
```

---

## 6. 对话补全 (Completion)

发送消息并获取 AI 回复。支持流式 (SSE) 和非流式响应。

- **URL**: `/completion`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_id` | string | 是 | 会话 ID |
| `messages` | list[dict] | 是 | 消息历史列表 (`[{"role": "user", "content": "..."}]`) |
| `llm_id` | string | 否 | 指定使用的 LLM 模型 ID |
| `stream` | boolean | 否 | 是否流式返回 (默认 true) |
| `temperature` | float | 否 | 模型温度 |
| `top_p` | float | 否 | Top P |
| `max_tokens` | int | 否 | 最大 Token 数 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/completion" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_id": "conv_123",
           "messages": [{"role": "user", "content": "Hello"}],
           "stream": true
         }'
```

### 响应示例 (流式)
```text
data: {"code": 0, "message": "", "data": {"answer": "Hi", "reference": []}}

data: {"code": 0, "message": "", "data": true}
```

---

## 7. 音频转文字 (Sequence to Text)

上传音频文件并转换为文字 (ASR)。

- **URL**: `/sequence2txt`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`

### 请求参数 (Form Data)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `file` | file | 是 | 音频文件 (wav, mp3, m4a, etc.) |
| `stream` | boolean | 否 | 是否流式返回 (默认 false) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/sequence2txt" \
     -F "file=@/path/to/audio.mp3" \
     -F "stream=false"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "text": "Transcribed text content."
  },
  "message": "success"
}
```

---

## 8. 文字转语音 (TTS)

将文本转换为语音流。

- **URL**: `/tts`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `text` | string | 是 | 要转换的文本 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/tts" \
     -H "Content-Type: application/json" \
     -d '{
           "text": "Hello world"
         }' \
     --output output.mp3
```

### 响应示例
返回音频流 (`audio/mpeg`)。

---

## 9. 删除消息 (Delete Message)

删除会话中的指定消息。

- **URL**: `/delete_msg`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_id` | string | 是 | 会话 ID |
| `message_id` | string | 是 | 消息 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/delete_msg" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_id": "conv_123",
           "message_id": "msg_456"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": { ...updated conversation... },
  "message": "success"
}
```

---

## 10. 消息点赞/点踩 (Thumb Up/Down)

对 AI 的回复进行点赞或点踩。

- **URL**: `/thumbup`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `conversation_id` | string | 是 | 会话 ID |
| `message_id` | string | 是 | 消息 ID |
| `thumbup` | boolean | 是 | true: 点赞, false: 点踩 |
| `feedback` | string | 否 | 反馈内容 (点踩时可选) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/thumbup" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_id": "conv_123",
           "message_id": "msg_456",
           "thumbup": true
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": { ...updated conversation... },
  "message": "success"
}
```

---

## 11. 知识库问答 (Ask)

直接向知识库提问 (Ask about)。通常返回流式数据。

- **URL**: `/ask`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题内容 |
| `kb_ids` | list[string] | 是 | 知识库 ID 列表 |
| `search_id` | string | 否 | 搜索配置 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/ask" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAG?",
           "kb_ids": ["kb_1"]
         }'
```

### 响应示例 (流式)
```text
data: {"code": 0, "message": "", "data": {"answer": "RAG is...", "reference": [...]}}

data: {"code": 0, "message": "", "data": true}
```

---

## 12. 生成思维导图 (Mindmap)

根据问题和知识库生成思维导图数据。

- **URL**: `/mindmap`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题/主题 |
| `kb_ids` | list[string] | 是 | 知识库 ID 列表 |
| `search_id` | string | 否 | 搜索配置 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/mindmap" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Project Overview",
           "kb_ids": ["kb_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "root": { "text": "Project Overview", "children": [...] }
  },
  "message": "success"
}
```

---

## 13. 相关问题建议 (Related Questions)

根据当前问题生成相关问题建议。

- **URL**: `/related_questions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 当前问题 |
| `search_id` | string | 否 | 搜索配置 ID (用于获取 LLM 设置) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/related_questions" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "How to install?"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    "System requirements?",
    "Docker deployment steps?"
  ],
  "message": "success"
}
```

