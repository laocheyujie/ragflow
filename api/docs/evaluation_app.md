# Evaluation API 文档

**Base URL**: `http://localhost:9380/v1/evaluation`

**Authentication**:
所有接口均需要认证。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 创建数据集 (Create Dataset)

创建一个新的评估数据集。

- **URL**: `/dataset/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | 数据集名称 |
| `kb_ids` | list[string] | 是 | 关联的知识库 ID 列表 |
| `description` | string | 否 | 数据集描述 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/dataset/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "My Evaluation Dataset",
           "description": "Dataset for testing RAG performance",
           "kb_ids": ["kb_1", "kb_2"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f"
  }
}
```

---

## 2. 获取数据集列表 (List Datasets)

获取当前租户下的评估数据集列表。

- **URL**: `/dataset/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | int | 否 | 页码 (默认: 1) |
| `page_size` | int | 否 | 每页数量 (默认: 20) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/dataset/list?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "datasets": [
      {
        "id": "5a6b7c8d9e0f1a2b3c4d5e6f",
        "tenant_id": "tenant_abc123",
        "name": "My Evaluation Dataset",
        "description": "Dataset for testing RAG performance",
        "kb_ids": ["kb_1", "kb_2"],
        "created_by": "user_xyz789",
        "create_time": 1704067200000,
        "update_time": 1704067200000,
        "status": 1
      }
    ],
    "total": 1
  }
}
```

---

## 3. 获取数据集详情 (Get Dataset)

根据 ID 获取数据集详情。

- **URL**: `/dataset/<dataset_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/dataset/dataset_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "5a6b7c8d9e0f1a2b3c4d5e6f",
    "tenant_id": "tenant_abc123",
    "name": "My Evaluation Dataset",
    "description": "Dataset for testing RAG performance",
    "kb_ids": ["kb_1", "kb_2"],
    "created_by": "user_xyz789",
    "create_time": 1704067200000,
    "update_time": 1704067200000,
    "status": 1
  }
}
```

---

## 4. 更新数据集 (Update Dataset)

更新数据集信息。

- **URL**: `/dataset/<dataset_id>`
- **Method**: `PUT`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 否 | 新的数据集名称 |
| `description` | string | 否 | 新的数据集描述 |
| `kb_ids` | list[string] | 否 | 新的关联知识库 ID 列表 |

### 请求示例
```bash
curl -X PUT "http://localhost:9380/v1/evaluation/dataset/dataset_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "Updated Name"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f"
  }
}
```

---

## 5. 删除数据集 (Delete Dataset)

删除指定的数据集 (软删除)。

- **URL**: `/dataset/<dataset_id>`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/evaluation/dataset/dataset_123" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f"
  }
}
```

---

## 6. 添加测试用例 (Add Test Case)

向数据集添加单个测试用例。

- **URL**: `/dataset/<dataset_id>/case/add`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 测试问题 |
| `reference_answer` | string | 否 | 参考答案 (Ground Truth) |
| `relevant_doc_ids` | list[string] | 否 | 相关文档 ID 列表 |
| `relevant_chunk_ids` | list[string] | 否 | 相关切片 ID 列表 |
| `metadata` | object | 否 | 元数据 (Key-Value) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/dataset/dataset_123/case/add" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "What is RAGFlow?",
           "reference_answer": "RAGFlow is an open-source RAG engine."
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "case_id": "a1b2c3d4e5f6a7b8c9d0e1f2"
  }
}
```

---

## 7. 批量导入测试用例 (Import Test Cases)

批量导入测试用例到数据集。

- **URL**: `/dataset/<dataset_id>/case/import`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `cases` | list[object] | 是 | 测试用例列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/dataset/dataset_123/case/import" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "cases": [
             {
               "question": "Question 1",
               "reference_answer": "Answer 1"
             },
             {
               "question": "Question 2",
               "reference_answer": "Answer 2"
             }
           ]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "success_count": 2,
    "failure_count": 0,
    "total": 2
  }
}
```

---

## 8. 获取测试用例列表 (Get Test Cases)

获取指定数据集下的所有测试用例。

- **URL**: `/dataset/<dataset_id>/cases`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/dataset/dataset_123/cases" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "cases": [
      {
        "id": "a1b2c3d4e5f6a7b8c9d0e1f2",
        "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f",
        "question": "What is RAGFlow?",
        "reference_answer": "RAGFlow is an open-source RAG engine.",
        "relevant_doc_ids": ["doc_001", "doc_002"],
        "relevant_chunk_ids": ["chunk_001", "chunk_002"],
        "metadata": {"category": "general"},
        "create_time": 1704067200000
      }
    ],
    "total": 1
  }
}
```

---

## 9. 删除测试用例 (Delete Test Case)

删除指定的测试用例。

- **URL**: `/case/<case_id>`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `case_id` | string | 是 | 测试用例 ID |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/evaluation/case/case_456" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "case_id": "a1b2c3d4e5f6a7b8c9d0e1f2"
  }
}
```

---

## 10. 开始评估 (Start Evaluation)

启动一次评估任务。

- **URL**: `/run/start`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 是 | 数据集 ID |
| `dialog_id` | string | 是 | 对话 (Agent) ID |
| `name` | string | 否 | 评估任务名称 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/run/start" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "dataset_id": "dataset_123",
           "dialog_id": "dialog_789",
           "name": "Run 1"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "run_id": "run1a2b3c4d5e6f7a8b9c0d1e"
  }
}
```

---

## 11. 获取评估任务详情 (Get Evaluation Run)

获取评估任务的基本信息。

- **URL**: `/run/<run_id>`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_id` | string | 是 | 评估任务 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/run/run_001" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "run": {
      "id": "run1a2b3c4d5e6f7a8b9c0d1e",
      "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f",
      "dialog_id": "dialog_abc123",
      "name": "Evaluation Run 2024-01-01 12:00:00",
      "config_snapshot": {},
      "metrics_summary": {
        "total_cases": 10,
        "avg_execution_time": 2.5,
        "avg_precision": 0.85,
        "avg_recall": 0.78,
        "avg_f1_score": 0.81
      },
      "status": "COMPLETED",
      "created_by": "user_xyz789",
      "create_time": 1704067200000,
      "complete_time": 1704070800000
    },
    "results": []
  }
}
```

---

## 12. 获取评估结果详情 (Get Run Results)

获取评估任务的详细结果。

- **URL**: `/run/<run_id>/results`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_id` | string | 是 | 评估任务 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/run/run_001/results" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "run": {
      "id": "run1a2b3c4d5e6f7a8b9c0d1e",
      "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f",
      "dialog_id": "dialog_abc123",
      "name": "Evaluation Run 2024-01-01 12:00:00",
      "config_snapshot": {},
      "metrics_summary": {
        "total_cases": 10,
        "avg_execution_time": 2.5,
        "avg_precision": 0.85,
        "avg_recall": 0.78,
        "avg_f1_score": 0.81
      },
      "status": "COMPLETED",
      "created_by": "user_xyz789",
      "create_time": 1704067200000,
      "complete_time": 1704070800000
    },
    "results": [
      {
        "id": "result_abc123",
        "run_id": "run1a2b3c4d5e6f7a8b9c0d1e",
        "case_id": "a1b2c3d4e5f6a7b8c9d0e1f2",
        "generated_answer": "RAGFlow is an open-source RAG engine based on deep document understanding.",
        "retrieved_chunks": [
          {
            "chunk_id": "chunk_001",
            "content": "RAGFlow is an open-source RAG engine...",
            "similarity": 0.95
          }
        ],
        "metrics": {
          "precision": 0.9,
          "recall": 0.85,
          "f1_score": 0.87,
          "hit_rate": 1.0,
          "mrr": 1.0,
          "answer_length": 78,
          "has_answer": 1.0
        },
        "execution_time": 2.35,
        "token_usage": null,
        "create_time": 1704067250000
      }
    ]
  }
}
```

---

## 13. 获取评估任务列表 (List Evaluation Runs)

列出评估任务。

- **URL**: `/run/list`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `dataset_id` | string | 否 | 按数据集筛选 |
| `dialog_id` | string | 否 | 按对话筛选 |
| `page` | int | 否 | 页码 (默认: 1) |
| `page_size` | int | 否 | 每页数量 (默认: 20) |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/run/list?page=1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "runs": [],
    "total": 0
  }
}
```

> **注意**: 此接口尚未完全实现。

---

## 14. 删除评估任务 (Delete Evaluation Run)

删除评估任务。

- **URL**: `/run/<run_id>`
- **Method**: `DELETE`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_id` | string | 是 | 评估任务 ID |

### 请求示例
```bash
curl -X DELETE "http://localhost:9380/v1/evaluation/run/run_001" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "run_id": "run1a2b3c4d5e6f7a8b9c0d1e"
  }
}
```

> **注意**: 此接口尚未完全实现。

---

## 15. 获取优化建议 (Get Recommendations)

根据评估结果获取配置优化建议。

- **URL**: `/run/<run_id>/recommendations`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_id` | string | 是 | 评估任务 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/run/run_001/recommendations" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "recommendations": [
      {
        "issue": "Low Precision",
        "severity": "high",
        "description": "System is retrieving many irrelevant chunks",
        "suggestions": [
          "Increase similarity_threshold to filter out less relevant chunks",
          "Enable reranking to improve chunk ordering",
          "Reduce top_k to return fewer chunks"
        ]
      },
      {
        "issue": "Slow Response Time",
        "severity": "medium",
        "description": "Average response time is 5.50s",
        "suggestions": [
          "Reduce top_k to retrieve fewer chunks",
          "Optimize embedding model selection",
          "Consider caching frequently asked questions"
        ]
      }
    ]
  }
}
```

---

## 16. 对比评估任务 (Compare Runs)

对比多个评估任务的结果。

- **URL**: `/compare`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_ids` | list[string] | 是 | 待对比的评估任务 ID 列表 (至少2个) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/compare" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "run_ids": ["run_001", "run_002"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "comparison": {}
  }
}
```

> **注意**: 此接口尚未完全实现。

---

## 17. 导出结果 (Export Results)

导出评估结果 (JSON/CSV)。

- **URL**: `/run/<run_id>/export`
- **Method**: `GET`

### 请求参数 (Path)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `run_id` | string | 是 | 评估任务 ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/evaluation/run/run_001/export" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "run": {
      "id": "run1a2b3c4d5e6f7a8b9c0d1e",
      "dataset_id": "5a6b7c8d9e0f1a2b3c4d5e6f",
      "dialog_id": "dialog_abc123",
      "name": "Evaluation Run 2024-01-01 12:00:00",
      "config_snapshot": {},
      "metrics_summary": {
        "total_cases": 10,
        "avg_execution_time": 2.5,
        "avg_precision": 0.85,
        "avg_recall": 0.78,
        "avg_f1_score": 0.81
      },
      "status": "COMPLETED",
      "created_by": "user_xyz789",
      "create_time": 1704067200000,
      "complete_time": 1704070800000
    },
    "results": [
      {
        "id": "result_abc123",
        "run_id": "run1a2b3c4d5e6f7a8b9c0d1e",
        "case_id": "a1b2c3d4e5f6a7b8c9d0e1f2",
        "generated_answer": "RAGFlow is an open-source RAG engine.",
        "retrieved_chunks": [],
        "metrics": {
          "answer_length": 40,
          "has_answer": 1.0
        },
        "execution_time": 2.35,
        "token_usage": null,
        "create_time": 1704067250000
      }
    ]
  }
}
```

---

## 18. 单次评估 (Evaluate Single)

实时评估单个问答对。

- **URL**: `/evaluate_single`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `question` | string | 是 | 测试问题 |
| `dialog_id` | string | 是 | 对话 (Agent) ID |
| `reference_answer` | string | 否 | 参考答案 |
| `relevant_chunk_ids` | list[string] | 否 | 指定切片 ID |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/evaluation/evaluate_single" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "question": "test question",
           "dialog_id": "dialog_789"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "answer": "",
    "metrics": {},
    "retrieved_chunks": []
  }
}
```

> **注意**: 此接口尚未完全实现，返回值为占位符。

