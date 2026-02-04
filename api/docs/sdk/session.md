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
    "id": "session_1",
    "chat_id": "chat_123",
    "name": "My Chat Session",
    "create_time": "2024-01-01 12:00:00",
    "messages": [
      {
        "role": "assistant",
        "content": "Hello! How can I help you?"
      }
    ]
  },
  "message": "success"
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
    "id": "session_agent_1",
    "agent_id": "agent_123",
    "user_id": "user_abc",
    "messages": [{"role": "assistant", "content": "..."}],
    "source": "agent",
    "dsl": {...}
  },
  "message": "success"
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
  "code": 0,
  "data": null,
  "message": "success"
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
data:{"code": 0, "message": "", "data": {"answer": "RAG stands for...", "reference": [...]}}

data:{"code": 0, "message": "", "data": true}
```

### 响应示例 (Non-Stream)
```json
{
  "code": 0,
  "data": {
      "answer": "RAG stands for...",
      "reference": [...]
  },
  "message": "success"
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

### 响应示例
(符合 OpenAI Chat Completion Chunk 格式)

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
           "stream": true
         }'
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
      "name": "New session",
      "create_time": "..."
    }
  ],
  "message": "success"
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
| `ids` | list[string] | 否 | 要删除的会话 ID 列表 (若为空则可能删除全部，具体视实现而定) |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/chats/chat_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["session_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "message": "success"
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
| `ids` | list[string] | 否 | 要删除的会话 ID 列表 |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/api/agents/agent_123/sessions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "ids": ["session_agent_1"]
         }'
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

### 响应示例
(Stream 格式返回答案)

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
    "Neural Networks",
    "Backpropagation",
    "CNN vs RNN"
  ],
  "message": "success"
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

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/api/chatbots/dialog_123/completions" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "Hello"
         }'
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
    "title": "Bot Name",
    "avatar": "...",
    "prologue": "Welcome!"
  },
  "message": "success"
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
| `...` | any | 否 | Agent 输入参数 |

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
    "title": "Agent Name",
    "inputs": {...},
    "prologue": "..."
  },
  "message": "success"
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
| `kb_id` | list[string] | 是 | 知识库 ID 列表 |
| `top_k` | integer | 否 | 返回数量 |
| `similarity_threshold` | number | 否 | 相似度阈值 |

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

---

## 21. 获取搜索机器人详情 (Searchbot Detail)

获取搜索机器人的详细配置。

- **URL**: `/searchbots/detail`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `search_id` | string | 是 | 搜索应用 ID |

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
