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
    "message": [
      {
        "role": "assistant",
        "content": "Hi! I'm your assistant. What can I do for you?"
      }
    ],
    "user_id": "user_abc123",
    "reference": [],
    "create_time": 1706841600000,
    "create_date": "2024-02-02 12:00:00",
    "update_time": 1706841600000,
    "update_date": "2024-02-02 12:00:00"
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
    "dialog_id": "dialog_456",
    "name": "My Chat",
    "message": [
      {
        "role": "assistant",
        "content": "Hi! I'm your assistant. What can I do for you?",
        "id": "msg_001"
      },
      {
        "role": "user",
        "content": "Hello",
        "id": "msg_002",
        "created_at": 1706841700.123
      },
      {
        "role": "assistant",
        "content": "Hello! How can I help you today?",
        "id": "msg_002",
        "created_at": 1706841702.456
      }
    ],
    "reference": [
      {
        "chunks": [
          {
            "id": "chunk_001",
            "content": "This is the chunk content...",
            "doc_id": "doc_001",
            "docnm_kwd": "document.pdf",
            "img_id": "",
            "positions": [[10, 20, 100, 50]]
          }
        ],
        "doc_aggs": [
          {
            "doc_id": "doc_001",
            "doc_name": "document.pdf",
            "count": 3
          }
        ],
        "total": 10
      }
    ],
    "user_id": "user_abc123",
    "avatar": "data:image/png;base64,...",
    "create_time": 1706841600000,
    "create_date": "2024-02-02 12:00:00",
    "update_time": 1706841800000,
    "update_date": "2024-02-02 12:03:20"
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
    "tenant_id": "tenant_abc",
    "name": "Customer Support Bot",
    "description": "A helpful assistant for customer inquiries",
    "avatar": "data:image/png;base64,...",
    "language": "English",
    "llm_id": "gpt-4",
    "llm_setting": {
      "temperature": 0.1,
      "top_p": 0.3,
      "frequency_penalty": 0.7,
      "presence_penalty": 0.4,
      "max_tokens": 512
    },
    "prompt_type": "simple",
    "prompt_config": {
      "system": "",
      "prologue": "Hi! I'm your assistant. What can I do for you?",
      "parameters": [],
      "empty_response": "Sorry! No relevant content was found in the knowledge base!"
    },
    "similarity_threshold": 0.2,
    "vector_similarity_weight": 0.3,
    "top_n": 6,
    "top_k": 1024,
    "do_refer": "1",
    "rerank_id": "",
    "kb_ids": ["kb_001", "kb_002"],
    "status": "1",
    "create_time": 1706841600000,
    "update_time": 1706841600000
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
      "dialog_id": "dialog_123",
      "name": "Chat Session 1",
      "message": [
        {
          "role": "assistant",
          "content": "Hi! I'm your assistant."
        }
      ],
      "reference": [],
      "user_id": "user_abc123",
      "create_time": 1706841600000,
      "create_date": "2024-02-02 12:00:00",
      "update_time": 1706841800000,
      "update_date": "2024-02-02 12:03:20"
    },
    {
      "id": "conv_124",
      "dialog_id": "dialog_123",
      "name": "Chat Session 2",
      "message": [
        {
          "role": "assistant",
          "content": "Hello! How can I help you?"
        }
      ],
      "reference": [],
      "user_id": "user_abc123",
      "create_time": 1706841500000,
      "create_date": "2024-02-02 11:58:20",
      "update_time": 1706841500000,
      "update_date": "2024-02-02 11:58:20"
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
| `messages` | list[dict] | 是 | 消息历史列表 (`[{"role": "user", "content": "...", "id": "..."}]`) |
| `llm_id` | string | 否 | 指定使用的 LLM 模型 ID |
| `stream` | boolean | 否 | 是否流式返回 (默认 true) |
| `temperature` | float | 否 | 模型温度 |
| `top_p` | float | 否 | Top P |
| `frequency_penalty` | float | 否 | 频率惩罚 |
| `presence_penalty` | float | 否 | 存在惩罚 |
| `max_tokens` | int | 否 | 最大 Token 数 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/completion" \
     -H "Content-Type: application/json" \
     -d '{
           "conversation_id": "conv_123",
           "messages": [
             {"role": "assistant", "content": "Hi! How can I help you?"},
             {"role": "user", "content": "What is RAG?", "id": "msg_001"}
           ],
           "stream": true
         }'
```

### 响应示例 (流式)
```text
data:{"code": 0, "message": "", "data": {"answer": "RAG stands for", "reference": {"chunks": [], "doc_aggs": []}, "id": "msg_001", "session_id": "conv_123"}}

data:{"code": 0, "message": "", "data": {"answer": "RAG stands for Retrieval-Augmented Generation", "reference": {"chunks": [], "doc_aggs": []}, "id": "msg_001", "session_id": "conv_123"}}

data:{"code": 0, "message": "", "data": {"answer": "RAG stands for Retrieval-Augmented Generation. It is a technique that combines...", "reference": {"chunks": [{"id": "chunk_001", "content": "RAG is a powerful technique...", "doc_id": "doc_001", "docnm_kwd": "rag_guide.pdf", "img_id": "", "positions": [[10, 20, 100, 50]]}], "doc_aggs": [{"doc_id": "doc_001", "doc_name": "rag_guide.pdf", "count": 2}], "total": 5}, "id": "msg_001", "session_id": "conv_123"}}

data:{"code": 0, "message": "", "data": true}
```

### 响应示例 (非流式)
```json
{
  "code": 0,
  "data": {
    "answer": "RAG stands for Retrieval-Augmented Generation. It is a technique that combines information retrieval with text generation to provide more accurate and contextual responses.",
    "reference": {
      "chunks": [
        {
          "id": "chunk_001",
          "content": "RAG is a powerful technique that enhances language models...",
          "doc_id": "doc_001",
          "docnm_kwd": "rag_guide.pdf",
          "img_id": "",
          "positions": [[10, 20, 100, 50]]
        }
      ],
      "doc_aggs": [
        {
          "doc_id": "doc_001",
          "doc_name": "rag_guide.pdf",
          "count": 2
        }
      ],
      "total": 5
    },
    "id": "msg_001",
    "session_id": "conv_123"
  },
  "message": "success"
}
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
| `file` | file | 是 | 音频文件 (wav, mp3, m4a, aac, flac, ogg, webm, opus, wma) |
| `stream` | string | 否 | 是否流式返回 ("true" 或 "false"，默认 "false") |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/conversation/sequence2txt" \
     -F "file=@/path/to/audio.mp3" \
     -F "stream=false"
```

### 响应示例 (非流式)
```json
{
  "code": 0,
  "data": {
    "text": "Hello, this is the transcribed text from the audio file."
  },
  "message": "success"
}
```

### 响应示例 (流式)
```text
data: {"event": "partial", "text": "Hello, this is"}

data: {"event": "partial", "text": "Hello, this is the transcribed"}

data: {"event": "final", "text": "Hello, this is the transcribed text from the audio file."}
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

### 响应
返回音频流 (`audio/mpeg`)，包含以下 HTTP 头：
- `Content-Type: audio/mpeg`
- `Cache-Control: no-cache`
- `Connection: keep-alive`
- `X-Accel-Buffering: no`

---

## 9. 删除消息 (Delete Message)

删除会话中的指定消息（包含用户问题和对应的助手回复）。

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
  "data": {
    "id": "conv_123",
    "dialog_id": "dialog_456",
    "name": "My Chat",
    "message": [
      {
        "role": "assistant",
        "content": "Hi! I'm your assistant. What can I do for you?"
      }
    ],
    "reference": [],
    "user_id": "user_abc123",
    "create_time": 1706841600000,
    "update_time": 1706842000000
  },
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
           "thumbup": false,
           "feedback": "The answer was not accurate"
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
    "message": [
      {
        "role": "assistant",
        "content": "Hi! I'm your assistant. What can I do for you?"
      },
      {
        "role": "user",
        "content": "What is RAG?",
        "id": "msg_456"
      },
      {
        "role": "assistant",
        "content": "RAG stands for Retrieval-Augmented Generation...",
        "id": "msg_456",
        "thumbup": false,
        "feedback": "The answer was not accurate"
      }
    ],
    "reference": [],
    "user_id": "user_abc123",
    "create_time": 1706841600000,
    "update_time": 1706842100000
  },
  "message": "success"
}
```

---

## 11. 知识库问答 (Ask)

直接向知识库提问 (Ask about)。返回流式数据。

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
           "kb_ids": ["kb_001"]
         }'
```

### 响应示例 (流式)
```text
data:{"code": 0, "message": "", "data": {"answer": "RAG stands for", "reference": {}}}

data:{"code": 0, "message": "", "data": {"answer": "RAG stands for Retrieval-Augmented Generation", "reference": {}}}

data:{"code": 0, "message": "", "data": {"answer": "RAG stands for Retrieval-Augmented Generation. It is a technique that combines retrieval and generation to provide more accurate responses. ##0$$", "reference": {"chunks": [{"id": "chunk_001", "content": "RAG (Retrieval-Augmented Generation) is a powerful technique...", "doc_id": "doc_001", "docnm_kwd": "rag_guide.pdf", "img_id": "", "positions": []}], "doc_aggs": [{"doc_id": "doc_001", "doc_name": "rag_guide.pdf", "count": 1}], "total": 3}}}

data:{"code": 0, "message": "", "data": true}
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
           "question": "Machine Learning Overview",
           "kb_ids": ["kb_001"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "root",
    "topic": "Machine Learning Overview",
    "children": [
      {
        "id": "node_1",
        "topic": "Supervised Learning",
        "children": [
          {
            "id": "node_1_1",
            "topic": "Classification"
          },
          {
            "id": "node_1_2",
            "topic": "Regression"
          }
        ]
      },
      {
        "id": "node_2",
        "topic": "Unsupervised Learning",
        "children": [
          {
            "id": "node_2_1",
            "topic": "Clustering"
          },
          {
            "id": "node_2_2",
            "topic": "Dimensionality Reduction"
          }
        ]
      },
      {
        "id": "node_3",
        "topic": "Reinforcement Learning"
      }
    ]
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
           "question": "How to install Docker?"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    "What are the system requirements for Docker?",
    "How to run a container in Docker?",
    "What is the difference between Docker and virtual machines?",
    "How to write a Dockerfile?",
    "How to use Docker Compose?"
  ],
  "message": "success"
}
```

---

## 错误响应

当发生错误时，API 会返回以下格式的响应：

### 数据错误
```json
{
  "code": 101,
  "message": "Conversation not found!"
}
```

### 权限错误
```json
{
  "code": 109,
  "message": "Only owner of conversation authorized for this operation."
}
```

### 服务器错误
```json
{
  "code": 500,
  "message": "Exception('Internal server error')"
}
```

### 流式错误响应
```text
data:{"code": 500, "message": "Error message here", "data": {"answer": "**ERROR**: Error message here", "reference": []}}
```
