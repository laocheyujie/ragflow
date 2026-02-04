# Session & Chat API 文档

**Base URL**: `http://localhost:9380/v1/api`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建会话 (Create Session)

为指定的助手 (Assistant/Chat) 创建一个新的会话。

- **URL**: `/chats/<chat_id>/sessions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID (Dialog ID) |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 会话名称 (默认: "New session") |
| `user_id` | string | 否 | 用户标识 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chats/chat_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Chat Session",
           "user_id": "user_abc"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "550e8400e29b41d4a716446655440000",
    "chat_id": "chat_123",
    "name": "My Chat Session",
    "user_id": "user_abc",
    "create_time": 1704067200000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1704067200000,
    "update_date": "2024-01-01 12:00:00",
    "messages": [
      {
        "role": "assistant",
        "content": "Hi! I'm your assistant. What can I do for you?"
      }
    ]
  }
}
```

---

## 2. 创建 Agent 会话 (Create Agent Session)

为指定的 Agent 创建一个新的会话。

- **URL**: `/agents/<agent_id>/sessions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `user_id` | string | 否 | 用户标识 (默认为 tenant_id) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agents/agent_123/sessions?user_id=user_abc" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "550e8400e29b41d4a716446655440001",
    "agent_id": "agent_123",
    "user_id": "user_abc",
    "message": [
      {
        "role": "assistant",
        "content": "Hello! How can I assist you today?"
      }
    ],
    "source": "agent",
    "dsl": {
      "components": {},
      "history": [],
      "path": [],
      "answer": []
    }
  }
}
```

---

## 3. 更新会话 (Update Session)

更新会话信息（如重命名）。

- **URL**: `/chats/<chat_id>/sessions/<session_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |
| `session_id` | string | 是 | 会话 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 新的会话名称 (不能为空) |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/api/chats/chat_123/sessions/session_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "Renamed Session"
         }'
```

### 响应示例
```json
{
  "code": 0
}
```

---

## 4. 对话补全 (Chat Completion)

与助手进行对话。

- **URL**: `/chats/<chat_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 否 | 用户提问内容 (若 session_id 未提供则为空字符串) |
| `session_id` | string | 否 | 会话 ID (若提供则基于历史上下文) |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `metadata_condition` | object | 否 | 元数据过滤条件 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chats/chat_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAG?",
           "session_id": "session_1",
           "stream": true
         }'
```

### 响应示例 (Stream)
```text
data:{"code": 0, "data": {"answer": "RAG stands for Retrieval-Augmented Generation...", "reference": {"total": 3, "chunks": [{"id": "chunk_1", "content": "...", "document_id": "doc_1", "document_name": "example.pdf", "dataset_id": "kb_1", "image_id": "", "positions": [[1, 100, 200, 300, 400]]}], "doc_aggs": [{"doc_id": "doc_1", "doc_name": "example.pdf", "count": 1}]}, "audio_binary": null, "id": "msg_123", "session_id": "session_1"}}

data:{"code": 0, "data": true}
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "answer": "RAG stands for Retrieval-Augmented Generation...",
    "reference": {
      "total": 3,
      "chunks": [
        {
          "id": "chunk_1",
          "content": "RAG is a technique that combines retrieval and generation...",
          "document_id": "doc_1",
          "document_name": "example.pdf",
          "dataset_id": "kb_1",
          "image_id": "",
          "positions": [[1, 100, 200, 300, 400]]
        }
      ],
      "doc_aggs": [
        {
          "doc_id": "doc_1",
          "doc_name": "example.pdf",
          "count": 1
        }
      ]
    },
    "audio_binary": null,
    "id": "msg_123",
    "session_id": "session_1",
    "prompt": "...",
    "created_at": 1704067200.123
  }
}
```

---

## 5. OpenAI 兼容对话 (Chat Completion OpenAI Compatible)

OpenAI 兼容的对话接口。

- **URL**: `/chats_openai/<chat_id>/chat/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `messages` | list[object] | 是 | 消息列表 (包含 role 和 content) |
| `model` | string | 是 | 模型名称 (占位符，实际由后端配置决定) |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `extra_body` | object | 否 | 额外参数 (如 `reference`: boolean, `metadata_condition`: object) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chats_openai/chat_123/chat/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "model": "gpt-3.5-turbo",
           "messages": [
             {"role": "user", "content": "Hello"}
           ],
           "stream": true
         }'
```

### 响应示例 (Stream)
```text
data:{"id": "chatcmpl-chat_123", "choices": [{"delta": {"content": "Hello", "role": "assistant", "function_call": null, "tool_calls": null, "reasoning_content": null}, "finish_reason": null, "index": 0, "logprobs": null}], "created": 1704067200, "model": "model", "object": "chat.completion.chunk", "system_fingerprint": "", "usage": null}

data:{"id": "chatcmpl-chat_123", "choices": [{"delta": {"content": null, "reasoning_content": null}, "finish_reason": "stop", "index": 0, "logprobs": null}], "created": 1704067200, "model": "model", "object": "chat.completion.chunk", "system_fingerprint": "", "usage": {"prompt_tokens": 5, "completion_tokens": 50, "total_tokens": 55}}

data:[DONE]
```

### 响应示例 (Non-Stream)
```json
{
  "id": "chatcmpl-chat_123",
  "object": "chat.completion",
  "created": 1704067200,
  "model": "gpt-3.5-turbo",
  "usage": {
    "prompt_tokens": 5,
    "completion_tokens": 50,
    "total_tokens": 55,
    "completion_tokens_details": {
      "reasoning_tokens": 100,
      "accepted_prediction_tokens": 50,
      "rejected_prediction_tokens": 0
    }
  },
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "Hello! How can I help you today?"
      },
      "logprobs": null,
      "finish_reason": "stop",
      "index": 0
    }
  ]
}
```

### 响应示例 (Non-Stream with Reference)
```json
{
  "id": "chatcmpl-chat_123",
  "object": "chat.completion",
  "created": 1704067200,
  "model": "gpt-3.5-turbo",
  "usage": {
    "prompt_tokens": 5,
    "completion_tokens": 50,
    "total_tokens": 55,
    "completion_tokens_details": {
      "reasoning_tokens": 100,
      "accepted_prediction_tokens": 50,
      "rejected_prediction_tokens": 0
    }
  },
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "Based on the documents...",
        "reference": [
          {
            "id": "chunk_1",
            "content": "...",
            "document_id": "doc_1",
            "document_name": "example.pdf",
            "dataset_id": "kb_1"
          }
        ]
      },
      "logprobs": null,
      "finish_reason": "stop",
      "index": 0
    }
  ]
}
```

---

## 6. OpenAI 兼容 Agent 对话 (Agent Completion OpenAI Compatible)

OpenAI 兼容的 Agent 对话接口。

- **URL**: `/agents_openai/<agent_id>/chat/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `messages` | list[object] | 是 | 消息列表 |
| `model` | string | 是 | 模型名称 |
| `stream` | boolean | 否 | 是否流式返回 (默认: false, 注意此接口默认值与其他不同) |
| `session_id` | string | 否 | 会话 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agents_openai/agent_123/chat/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "messages": [{"role": "user", "content": "Run analysis"}],
           "model": "agent-model"
         }'
```

### 响应示例 (Non-Stream)
```json
{
  "id": "agent_123",
  "object": "chat.completion",
  "created": 1704067200,
  "model": "agent-model",
  "usage": {
    "prompt_tokens": 10,
    "completion_tokens": 100,
    "total_tokens": 110
  },
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "The analysis results show..."
      },
      "logprobs": null,
      "finish_reason": "stop",
      "index": 0
    }
  ]
}
```

---

## 7. Agent 补全 (Agent Completion)

执行 Agent 对话/任务。

- **URL**: `/agents/<agent_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `return_trace` | boolean | 否 | 是否返回执行轨迹 (默认: false) |
| `...` | any | 否 | 其他传递给 Agent 的参数 (如 inputs, question 等) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agents/agent_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Analyze this data",
           "stream": true,
           "return_trace": true
         }'
```

### 响应示例 (Stream)
```text
data:{"event": "message", "data": {"content": "Analyzing...", "session_id": "session_1"}}

data:{"event": "node_finished", "data": {"component_id": "begin_0", "trace": [{"component_id": "begin_0", "...": "..."}]}}

data:{"event": "message_end", "data": {"content": "Analysis complete.", "reference": {}, "session_id": "session_1"}}

data:[DONE]
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "event": "message_end",
    "data": {
      "content": "The analysis shows that...",
      "reference": {
        "chunks": [
          {
            "id": "chunk_1",
            "content": "...",
            "document_id": "doc_1",
            "document_name": "data.csv",
            "dataset_id": "kb_1"
          }
        ],
        "doc_aggs": [
          {
            "doc_id": "doc_1",
            "doc_name": "data.csv",
            "count": 1
          }
        ]
      },
      "trace": [
        {
          "component_id": "begin_0",
          "trace": [{"component_id": "begin_0"}]
        },
        {
          "component_id": "generate_1",
          "trace": [{"component_id": "generate_1"}]
        }
      ]
    }
  }
}
```

---

## 8. 获取会话列表 (List Sessions)

获取助手的会话列表。

- **URL**: `/chats/<chat_id>/sessions`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `orderby` | string | 否 | 排序字段 (默认: create_time) |
| `desc` | boolean | 否 | 是否降序 (默认: true) |
| `id` | string | 否 | 按会话 ID 过滤 |
| `name` | string | 否 | 按会话名称过滤 |
| `user_id` | string | 否 | 按用户 ID 过滤 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/chats/chat_123/sessions?page=1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "session_1",
      "chat_id": "chat_123",
      "name": "New session",
      "user_id": "user_abc",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704067200000,
      "update_date": "2024-01-01 12:00:00",
      "messages": [
        {
          "role": "assistant",
          "content": "Hi! How can I help you?",
          "created_at": 1704067200.0
        },
        {
          "role": "user",
          "content": "What is RAG?",
          "id": "msg_user_1"
        },
        {
          "role": "assistant",
          "content": "RAG stands for...",
          "id": "msg_assistant_1",
          "created_at": 1704067210.0,
          "reference": [
            {
              "id": "chunk_1",
              "content": "...",
              "document_id": "doc_1",
              "document_name": "example.pdf",
              "dataset_id": "kb_1",
              "image_id": "",
              "positions": [[1, 100, 200, 300, 400]]
            }
          ]
        }
      ]
    }
  ]
}
```

---

## 9. 获取 Agent 会话列表 (List Agent Sessions)

获取 Agent 的会话列表。

- **URL**: `/agents/<agent_id>/sessions`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | integer | 否 | 页码 (默认: 1) |
| `page_size` | integer | 否 | 每页数量 (默认: 30) |
| `orderby` | string | 否 | 排序字段 (默认: update_time) |
| `desc` | boolean | 否 | 是否降序 (默认: true) |
| `dsl` | boolean | 否 | 是否包含 DSL (默认: true) |
| `id` | string | 否 | 按 ID 过滤 |
| `user_id` | string | 否 | 按用户 ID 过滤 |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/agents/agent_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "id": "session_agent_1",
      "agent_id": "agent_123",
      "user_id": "user_abc",
      "create_time": 1704067200000,
      "create_date": "2024-01-01 12:00:00",
      "update_time": 1704153600000,
      "update_date": "2024-01-02 12:00:00",
      "tokens": 1500,
      "source": "agent",
      "duration": 2.5,
      "round": 3,
      "thumb_up": 1,
      "messages": [
        {
          "role": "assistant",
          "content": "Hello! How can I assist you?",
          "created_at": 1704067200.0
        },
        {
          "role": "user",
          "content": "Analyze this data",
          "id": "msg_user_1"
        },
        {
          "role": "assistant",
          "content": "The analysis shows...",
          "id": "msg_assistant_1",
          "created_at": 1704067210.0,
          "reference": [
            {
              "id": "chunk_1",
              "content": "...",
              "document_id": "doc_1",
              "document_name": "data.csv",
              "dataset_id": "kb_1",
              "image_id": "",
              "positions": []
            }
          ]
        }
      ],
      "dsl": {
        "components": {},
        "history": [],
        "path": [],
        "answer": []
      }
    }
  ]
}
```

---

## 10. 删除会话 (Delete Sessions)

删除一个或多个会话。

- **URL**: `/chats/<chat_id>/sessions`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `chat_id` | string | 是 | 助手 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ids` | list[string] | 否 | 要删除的会话 ID 列表 (若为空则删除该 chat 下的全部会话) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/chats/chat_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["session_1"]
         }'
```

### 响应示例 (全部成功)
```json
{
  "code": 0
}
```

### 响应示例 (部分成功)
```json
{
  "code": 0,
  "message": "Partially deleted 2 sessions with 1 errors",
  "data": {
    "success_count": 2,
    "errors": [
      "The chat doesn't own the session session_not_exist"
    ]
  }
}
```

---

## 11. 删除 Agent 会话 (Delete Agent Sessions)

删除一个或多个 Agent 会话。

- **URL**: `/agents/<agent_id>/sessions`
- **Method**: `DELETE`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `ids` | list[string] | 否 | 要删除的会话 ID 列表 (若为空则删除该 agent 下的全部会话) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/agents/agent_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["session_agent_1"]
         }'
```

### 响应示例 (全部成功)
```json
{
  "code": 0
}
```

### 响应示例 (部分成功)
```json
{
  "code": 0,
  "message": "Partially deleted 2 sessions with 1 errors",
  "data": {
    "success_count": 2,
    "errors": [
      "The agent doesn't own the session session_not_exist"
    ]
  }
}
```

---

## 12. 知识库问答 (Ask KB)

直接针对知识库提问。

- **URL**: `/sessions/ask`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题内容 |
| `dataset_ids` | list[string] | 是 | 知识库 ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/sessions/ask" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is in the doc?",
           "dataset_ids": ["kb_1"]
         }'
```

### 响应示例 (Stream)
```text
data:{"code": 0, "message": "", "data": {"answer": "Based on the documents...", "reference": {}}}

data:{"code": 0, "message": "", "data": {"answer": "Based on the documents, the content includes...", "reference": {"total": 2, "chunks": [{"id": "chunk_1", "content": "...", "document_id": "doc_1", "document_name": "example.pdf", "dataset_id": "kb_1"}], "doc_aggs": [{"doc_id": "doc_1", "doc_name": "example.pdf", "count": 1}]}}}

data:{"code": 0, "message": "", "data": true}
```

---

## 13. 相关问题生成 (Related Questions)

根据问题生成相关搜索建议。

- **URL**: `/sessions/related_questions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 原始问题 |
| `industry` | string | 否 | 行业背景 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/sessions/related_questions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Deep learning"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    "What is deep learning?",
    "Deep learning vs machine learning",
    "Deep learning applications",
    "Neural network architectures",
    "How to get started with deep learning"
  ]
}
```

---

## 14. 聊天机器人补全 (Chatbot Completion)

用于嵌入式聊天机器人 (Iframe/External) 的对话接口。

- **URL**: `/chatbots/<dialog_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | 对话 ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 用户提问 |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `session_id` | string | 否 | 会话 ID |
| `quote` | boolean | 否 | 是否返回引用 (默认: false) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chatbots/dialog_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Hello"
         }'
```

### 响应示例 (Stream - 新会话)
```text
data:{"code": 0, "message": "", "data": {"answer": "Hi! I'm your assistant. What can I do for you?", "reference": {}, "audio_binary": null, "id": null, "session_id": "550e8400e29b41d4a716446655440000"}}

data:{"code": 0, "message": "", "data": true}
```

### 响应示例 (Stream - 已有会话)
```text
data:{"code": 0, "message": "", "data": {"answer": "Hello! How can I help you today?", "reference": {"chunks": [...], "doc_aggs": [...]}, "audio_binary": null, "id": "msg_123", "session_id": "session_1"}}

data:{"code": 0, "message": "", "data": true}
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "answer": "Hello! How can I help you today?",
    "reference": {
      "chunks": [],
      "doc_aggs": []
    },
    "audio_binary": null,
    "id": "msg_123",
    "session_id": "session_1",
    "prompt": "...",
    "created_at": 1704067200.123
  }
}
```

---

## 15. 获取聊天机器人信息 (Chatbot Info)

获取嵌入式聊天机器人的基本信息。

- **URL**: `/chatbots/<dialog_id>/info`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dialog_id` | string | 是 | 对话 ID |

### 响应示例
```json
{
  "code": 0,
  "data": {
    "title": "Customer Service Bot",
    "avatar": "data:image/png;base64,iVBORw0KGgo...",
    "prologue": "Hi! I'm your assistant. What can I do for you?"
  }
}
```

---

## 16. Agent 机器人补全 (Agentbot Completion)

用于嵌入式 Agent 机器人 (Iframe/External) 的执行接口。

- **URL**: `/agentbots/<agent_id>/completions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `stream` | boolean | 否 | 是否流式返回 (默认: true) |
| `question` | string | 否 | 用户问题 |
| `session_id` | string | 否 | 会话 ID |
| `...` | any | 否 | Agent 输入参数 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/agentbots/agent_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Process this request",
           "stream": true
         }'
```

### 响应示例 (Stream)
```text
data:{"event": "message", "data": {"content": "Processing your request...", "session_id": "session_1"}}

data:{"event": "message", "data": {"content": "Processing your request... Done!", "session_id": "session_1"}}

data:{"event": "message_end", "data": {"content": "Processing your request... Done!", "reference": {}, "session_id": "session_1"}}

data:[DONE]
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
    "event": "message_end",
    "data": {
      "content": "Request processed successfully.",
      "reference": {},
      "session_id": "session_1"
    }
  }
}
```

---

## 17. 获取 Agent 机器人输入项 (Agentbot Inputs)

获取 Agent 机器人的初始输入表单配置。

- **URL**: `/agentbots/<agent_id>/inputs`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `agent_id` | string | 是 | Agent ID |

### 响应示例
```json
{
  "code": 0,
  "data": {
    "title": "Data Analysis Agent",
    "avatar": "data:image/png;base64,iVBORw0KGgo...",
    "inputs": [
      {
        "key": "file",
        "type": "file",
        "name": "Upload File",
        "required": true
      },
      {
        "key": "query",
        "type": "text",
        "name": "Analysis Query",
        "required": false
      }
    ],
    "prologue": "Welcome! Please upload your data file to begin analysis.",
    "mode": "chat"
  }
}
```

---

## 18. 搜索机器人问答 (Searchbot Ask)

用于搜索机器人 (Searchbot) 的问答接口。

- **URL**: `/searchbots/ask`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `kb_ids` | list[string] | 是 | 知识库 ID 列表 |
| `search_id` | string | 否 | 搜索应用 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/ask" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is machine learning?",
           "kb_ids": ["kb_1", "kb_2"]
         }'
```

### 响应示例 (Stream)
```text
data:{"code": 0, "message": "", "data": {"answer": "Machine learning is...", "reference": {}}}

data:{"code": 0, "message": "", "data": {"answer": "Machine learning is a subset of artificial intelligence...", "reference": {"total": 5, "chunks": [{"id": "chunk_1", "content": "...", "document_id": "doc_1", "document_name": "ml_guide.pdf", "dataset_id": "kb_1"}], "doc_aggs": [{"doc_id": "doc_1", "doc_name": "ml_guide.pdf", "count": 2}]}}}

data:{"code": 0, "message": "", "data": true}
```

---

## 19. 搜索机器人检索测试 (Searchbot Retrieval Test)

搜索机器人的检索测试接口。

- **URL**: `/searchbots/retrieval_test`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `kb_id` | string 或 list[string] | 是 | 知识库 ID (列表) |
| `top_k` | integer | 否 | 返回数量 (默认: 1024) |
| `similarity_threshold` | number | 否 | 相似度阈值 (默认: 0.0) |
| `vector_similarity_weight` | number | 否 | 向量相似度权重 (默认: 0.3) |
| `doc_ids` | list[string] | 否 | 文档 ID 过滤列表 |
| `page` | integer | 否 | 页码 (默认: 1) |
| `size` | integer | 否 | 每页数量 (默认: 30) |
| `rerank_id` | string | 否 | Rerank 模型 ID |
| `use_kg` | boolean | 否 | 是否使用知识图谱 (默认: false) |
| `highlight` | boolean | 否 | 是否高亮显示 |
| `keyword` | boolean | 否 | 是否启用关键词提取 (默认: false) |
| `cross_languages` | list[string] | 否 | 跨语言搜索列表 |
| `search_id` | string | 否 | 搜索应用 ID |
| `meta_data_filter` | object | 否 | 元数据过滤配置 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/retrieval_test" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAG?",
           "kb_id": ["kb_1"],
           "top_k": 10,
           "similarity_threshold": 0.2
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "total": 25,
    "chunks": [
      {
        "chunk_id": "chunk_001",
        "content_with_weight": "RAG (Retrieval-Augmented Generation) is a technique...",
        "content_ltks": "rag retrieval augmented generation technique",
        "doc_id": "doc_1",
        "docnm_kwd": "rag_guide.pdf",
        "kb_id": "kb_1",
        "similarity": 0.89,
        "vector_similarity": 0.85,
        "term_similarity": 0.92,
        "positions": [[1, 50, 100, 200, 150]],
        "image_id": ""
      },
      {
        "chunk_id": "chunk_002",
        "content_with_weight": "RAG combines the power of retrieval...",
        "content_ltks": "rag combines power retrieval",
        "doc_id": "doc_1",
        "docnm_kwd": "rag_guide.pdf",
        "kb_id": "kb_1",
        "similarity": 0.82,
        "vector_similarity": 0.80,
        "term_similarity": 0.84,
        "positions": [[2, 60, 110, 210, 160]],
        "image_id": ""
      }
    ],
    "doc_aggs": [
      {
        "doc_id": "doc_1",
        "doc_name": "rag_guide.pdf",
        "count": 5
      }
    ],
    "labels": ["technology", "ai"]
  }
}
```

---

## 20. 搜索机器人相关问题 (Searchbot Related Questions)

生成搜索机器人的相关推荐问题。

- **URL**: `/searchbots/related_questions`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `search_id` | string | 否 | 搜索应用 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/related_questions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAG?"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    "How does RAG work?",
    "RAG vs fine-tuning comparison",
    "Best practices for RAG implementation",
    "RAG architecture overview",
    "Common RAG use cases"
  ]
}
```

---

## 21. 获取搜索机器人详情 (Searchbot Detail)

获取搜索机器人的详细配置。

- **URL**: `/searchbots/detail`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `search_id` | string | 是 | 搜索应用 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/api/searchbots/detail?search_id=search_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "search_123",
    "name": "Knowledge Search",
    "avatar": "data:image/png;base64,iVBORw0KGgo...",
    "description": "A search application for internal knowledge base",
    "tenant_id": "tenant_1",
    "created_by": "user_1",
    "create_time": 1704067200000,
    "create_date": "2024-01-01 12:00:00",
    "update_time": 1704153600000,
    "update_date": "2024-01-02 12:00:00",
    "status": "1",
    "search_config": {
      "kb_ids": ["kb_1", "kb_2"],
      "doc_ids": [],
      "similarity_threshold": 0.2,
      "vector_similarity_weight": 0.3,
      "use_kg": false,
      "rerank_id": "",
      "top_k": 1024,
      "summary": true,
      "chat_id": "llm_model_1",
      "llm_setting": {
        "temperature": 0.1,
        "top_p": 0.3
      },
      "cross_languages": [],
      "highlight": true,
      "keyword": false,
      "web_search": false,
      "related_search": true,
      "query_mindmap": false
    }
  }
}
```

---

## 22. 搜索机器人思维导图 (Searchbot Mindmap)

生成搜索结果的思维导图。

- **URL**: `/searchbots/mindmap`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 问题 |
| `kb_ids` | list[string] | 是 | 知识库 ID 列表 |
| `search_id` | string | 否 | 搜索应用 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/searchbots/mindmap" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Explain machine learning concepts",
           "kb_ids": ["kb_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "name": "Machine Learning Concepts",
    "children": [
      {
        "name": "Supervised Learning",
        "children": [
          {"name": "Classification"},
          {"name": "Regression"}
        ]
      },
      {
        "name": "Unsupervised Learning",
        "children": [
          {"name": "Clustering"},
          {"name": "Dimensionality Reduction"}
        ]
      },
      {
        "name": "Reinforcement Learning",
        "children": [
          {"name": "Q-Learning"},
          {"name": "Policy Gradient"}
        ]
      }
    ]
  }
}
```
