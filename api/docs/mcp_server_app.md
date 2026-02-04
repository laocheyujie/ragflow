# MCP Server API 文档

**Base URL**: `http://localhost:9380/v1/mcp_server`

**Authentication**:
所有接口均需要认证（除 `/test_mcp` 外，但通常也建议携带）。请在 Header 中携带 API Key：
`Authorization: Bearer <YOUR_API_KEY>`

## 1. 获取 MCP Server 列表 (List MCP Servers)

获取当前用户的 MCP Server 列表。

- **URL**: `/list`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `page` | int | 否 | 页码 (默认 1) |
| `page_size` | int | 否 | 每页数量 (默认不限制) |
| `orderby` | string | 否 | 排序字段 (默认 `create_time`) |
| `desc` | boolean | 否 | 是否降序 (默认 `true`) |
| `keywords` | string | 否 | 搜索关键字 |

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_ids` | list[string] | 否 | 指定 MCP ID 列表进行筛选 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/list?page=1&page_size=10" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_ids": []
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "mcp_servers": [
      {
        "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
        "name": "My MCP Server",
        "server_type": "sse",
        "url": "http://example.com/sse",
        "description": "A sample MCP server",
        "variables": {
          "tools": {
            "get_weather": {
              "name": "get_weather",
              "description": "Get weather info",
              "enabled": true
            }
          }
        },
        "create_date": "2024-01-15 10:30:00",
        "update_date": "2024-01-15 10:30:00"
      }
    ],
    "total": 1
  }
}
```

---

## 2. 获取 MCP Server 详情 (Get MCP Server Detail)

获取指定 MCP Server 的详细信息。

- **URL**: `/detail`
- **Method**: `GET`

### 请求参数 (Query)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_id` | string | 是 | MCP Server ID |

### 请求示例
```bash
curl -X GET "http://localhost:9380/v1/mcp_server/detail?mcp_id=mcp_1" \
     -H "Authorization: Bearer <YOUR_API_KEY>"
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "name": "My MCP Server",
    "tenant_id": "tenant_abc123",
    "url": "http://example.com/sse",
    "server_type": "sse",
    "description": null,
    "variables": {
      "tools": {
        "get_weather": {
          "name": "get_weather",
          "description": "Get weather info",
          "enabled": true
        }
      }
    },
    "headers": {},
    "create_time": 1705312200000,
    "create_date": "2024-01-15 10:30:00",
    "update_time": 1705312200000,
    "update_date": "2024-01-15 10:30:00"
  }
}
```

---

## 3. 创建 MCP Server (Create MCP Server)

创建一个新的 MCP Server。

- **URL**: `/create`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `name` | string | 是 | MCP Server 名称 (最大 255 字节) |
| `url` | string | 是 | MCP Server 地址 |
| `server_type` | string | 是 | 类型 (`sse` 或 `stdio`) |
| `headers` | json/string | 否 | 请求头配置 |
| `variables` | json/string | 否 | 环境变量配置 |
| `timeout` | float | 否 | 超时时间 (默认 10秒) |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/create" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "name": "Weather MCP",
           "url": "http://weather-mcp.example.com/sse",
           "server_type": "sse",
           "headers": {"Authorization": "Basic xxx"}
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "tenant_id": "tenant_abc123",
    "name": "Weather MCP",
    "url": "http://weather-mcp.example.com/sse",
    "server_type": "sse",
    "headers": {"Authorization": "Basic xxx"},
    "variables": {
      "tools": {
        "get_weather": {
          "name": "get_weather",
          "description": "Get weather info for a location",
          "inputSchema": {
            "type": "object",
            "properties": {
              "city": {"type": "string", "description": "City name"}
            },
            "required": ["city"]
          },
          "enabled": true
        }
      }
    }
  }
}
```

---

## 4. 更新 MCP Server (Update MCP Server)

更新现有的 MCP Server 信息。

- **URL**: `/update`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_id` | string | 是 | 要更新的 MCP Server ID |
| `name` | string | 否 | 新名称 |
| `url` | string | 否 | 新地址 |
| `server_type` | string | 否 | 新类型 |
| `headers` | json/string | 否 | 新请求头 |
| `variables` | json/string | 否 | 新变量 |
| `timeout` | float | 否 | 超时时间 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/update" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_id": "mcp_1",
           "name": "Updated Weather MCP"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "name": "Updated Weather MCP",
    "tenant_id": "tenant_abc123",
    "url": "http://weather-mcp.example.com/sse",
    "server_type": "sse",
    "description": null,
    "variables": {
      "tools": {
        "get_weather": {
          "name": "get_weather",
          "description": "Get weather info",
          "enabled": true
        }
      }
    },
    "headers": {},
    "create_time": 1705312200000,
    "create_date": "2024-01-15 10:30:00",
    "update_time": 1705398600000,
    "update_date": "2024-01-16 10:30:00"
  }
}
```

---

## 5. 删除 MCP Server (Remove MCP Server)

删除一个或多个 MCP Server。

- **URL**: `/rm`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_ids` | list[string] | 是 | 要删除的 MCP Server ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/rm" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_ids": ["mcp_1", "mcp_2"]
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

## 6. 导入 MCP Server (Import MCP Servers)

批量导入 MCP Server 配置。

- **URL**: `/import`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcpServers` | dict | 是 | Server 名称到配置的映射 |
| `timeout` | float | 否 | 连接测试超时时间 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/import" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcpServers": {
             "my-server": {
               "type": "sse",
               "url": "http://localhost:8080/sse"
             }
           }
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "results": [
      {
        "server": "my-server",
        "success": true,
        "action": "created",
        "id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
        "new_name": "my-server"
      },
      {
        "server": "existing-server",
        "success": true,
        "action": "created",
        "id": "b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7",
        "new_name": "existing-server_0",
        "message": "Renamed from 'existing-server' to 'existing-server_0' avoid duplication"
      },
      {
        "server": "invalid-server",
        "success": false,
        "message": "Missing required fields (type or url)"
      }
    ]
  }
}
```

---

## 7. 导出 MCP Server (Export MCP Servers)

导出指定的 MCP Server 配置。

- **URL**: `/export`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_ids` | list[string] | 是 | 要导出的 MCP Server ID 列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/export" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_ids": ["mcp_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "mcpServers": {
      "My MCP Server": {
        "type": "sse",
        "url": "http://example.com/sse",
        "name": "My MCP Server",
        "authorization_token": "",
        "tools": {
          "get_weather": {
            "name": "get_weather",
            "description": "Get weather info",
            "inputSchema": {
              "type": "object",
              "properties": {
                "city": {"type": "string"}
              },
              "required": ["city"]
            },
            "enabled": true
          }
        }
      }
    }
  }
}
```

---

## 8. 获取工具列表 (List Tools)

从指定的 MCP Server 中获取可用工具列表。

- **URL**: `/list_tools`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_ids` | list[string] | 是 | MCP Server ID 列表 |
| `timeout` | float | 否 | 超时时间 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/list_tools" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_ids": ["mcp_1"]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6": [
      {
        "name": "get_weather",
        "description": "Get weather info for a location",
        "inputSchema": {
          "type": "object",
          "properties": {
            "city": {
              "type": "string",
              "description": "City name"
            }
          },
          "required": ["city"]
        },
        "enabled": true
      },
      {
        "name": "get_forecast",
        "description": "Get weather forecast",
        "inputSchema": {
          "type": "object",
          "properties": {
            "city": {"type": "string"},
            "days": {"type": "integer", "default": 7}
          },
          "required": ["city"]
        },
        "enabled": false
      }
    ]
  }
}
```

---

## 9. 测试工具 (Test Tool)

调用指定的 MCP 工具进行测试。

- **URL**: `/test_tool`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_id` | string | 是 | MCP Server ID |
| `tool_name` | string | 是 | 工具名称 |
| `arguments` | dict | 是 | 工具参数 |
| `timeout` | float | 否 | 超时时间 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/test_tool" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_id": "mcp_1",
           "tool_name": "get_weather",
           "arguments": {"city": "Beijing"}
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "content": [
      {
        "type": "text",
        "text": "Weather in Beijing: Sunny, 25°C, Humidity 45%"
      }
    ],
    "isError": false
  }
}
```

---

## 10. 缓存工具 (Cache Tools)

更新 MCP Server 的工具缓存配置。

- **URL**: `/cache_tools`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `mcp_id` | string | 是 | MCP Server ID |
| `tools` | list[dict] | 是 | 工具列表 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/cache_tools" \
     -H "Authorization: Bearer <YOUR_API_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
           "mcp_id": "mcp_1",
           "tools": [{"name": "get_weather", "enabled": true}]
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": {
    "get_weather": {
      "name": "get_weather",
      "description": "Get weather info",
      "inputSchema": {
        "type": "object",
        "properties": {
          "city": {"type": "string"}
        },
        "required": ["city"]
      },
      "enabled": true
    }
  }
}
```

---

## 11. 测试 MCP 连接 (Test MCP)

测试连接到一个 MCP Server 并获取其工具列表（不保存）。

- **URL**: `/test_mcp`
- **Method**: `POST`
- **Content-Type**: `application/json`

### 请求参数 (Body)

| 参数名 | 类型 | 必填 | 描述 |
| :--- | :--- | :--- | :--- |
| `url` | string | 是 | MCP Server URL |
| `server_type` | string | 是 | 类型 (`sse` 或 `stdio`) |
| `headers` | json/string | 否 | 请求头 |
| `variables` | json/string | 否 | 变量 |
| `timeout` | float | 否 | 超时时间 |

### 请求示例
```bash
curl -X POST "http://localhost:9380/v1/mcp_server/test_mcp" \
     -H "Content-Type: application/json" \
     -d '{
           "url": "http://localhost:8080/sse",
           "server_type": "sse"
         }'
```

### 响应示例
```json
{
  "code": 0,
  "data": [
    {
      "name": "get_weather",
      "description": "Get weather info for a location",
      "inputSchema": {
        "type": "object",
        "properties": {
          "city": {
            "type": "string",
            "description": "City name"
          }
        },
        "required": ["city"]
      },
      "enabled": true
    },
    {
      "name": "get_forecast",
      "description": "Get weather forecast for upcoming days",
      "inputSchema": {
        "type": "object",
        "properties": {
          "city": {"type": "string"},
          "days": {"type": "integer", "default": 7}
        },
        "required": ["city"]
      },
      "enabled": true
    }
  ]
}
```

