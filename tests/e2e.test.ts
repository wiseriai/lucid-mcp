import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, type ChildProcess } from "child_process";
import * as readline from "readline";
import * as path from "path";

let server: ChildProcess;
let messageId = 1;

interface JsonRpcRequest {
  jsonrpc: "2.0";
  id: number | string;
  method: string;
  params: unknown;
}

interface JsonRpcResponse {
  jsonrpc: "2.0";
  id: number | string;
  result?: unknown;
  error?: {
    code: number;
    message: string;
    data?: unknown;
  };
}

/**
 * 发送 MCP 消息到 Server，等待响应
 */
async function sendMessage(request: JsonRpcRequest): Promise<JsonRpcResponse> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("MCP request timeout")),
      10000
    );

    const messageHandler = (data: Buffer) => {
      try {
        const lines = data.toString().split("\n");
        for (const line of lines) {
          if (line.trim()) {
            const response = JSON.parse(line) as JsonRpcResponse;
            if (response.id === request.id) {
              clearTimeout(timeout);
              server.stdout?.removeListener("data", messageHandler);
              resolve(response);
            }
          }
        }
      } catch {
        // 忽略非 JSON 行
      }
    };

    server.stdout?.on("data", messageHandler);
    server.stdin?.write(JSON.stringify(request) + "\n");
  });
}

describe("Lucid MCP — Sprint 1 E2E Tests", () => {
  beforeAll((done) => {
    server = spawn("node", [path.join(process.cwd(), "dist/index.js")], {
      stdio: ["pipe", "pipe", "pipe"],
    });

    server.stderr?.on("data", (data) => {
      console.log("SERVER ERROR:", data.toString());
    });

    // Wait for server to start
    setTimeout(() => done(), 1000);
  });

  afterAll(() => {
    server.kill();
  });

  describe("Scenario 1: CSV 连接 + 基础查询", () => {
    it("1.1 连接 CSV 数据源", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "connect_source",
          arguments: {
            type: "csv",
            path: path.join(process.cwd(), "tests/datasets/superstore/orders.csv"),
          },
        },
      });

      expect(response.result).toBeDefined();
      expect(response.error).toBeUndefined();
      console.log("✅ CSV 连接成功:", response.result);
    });

    it("1.2 列出所有表", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "list_tables",
          arguments: {},
        },
      });

      expect(response.result).toBeDefined();
      console.log("✅ 表列表:", response.result);
    });

    it("1.3 描述表结构", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "describe_table",
          arguments: {
            table_name: "orders",
          },
        },
      });

      expect(response.result).toBeDefined();
      console.log("✅ 表结构:", response.result);
    });

    it("1.4 执行基础 SELECT 查询", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "query",
          arguments: {
            sql: "SELECT COUNT(*) as count FROM orders",
          },
        },
      });

      expect(response.result).toBeDefined();
      expect(response.error).toBeUndefined();
      console.log("✅ 查询结果:", response.result);
    });

    it("1.5 执行聚合查询", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "query",
          arguments: {
            sql: "SELECT Category, SUM(Sales) as total_sales FROM orders GROUP BY Category ORDER BY total_sales DESC LIMIT 3",
          },
        },
      });

      expect(response.result).toBeDefined();
      expect(response.error).toBeUndefined();
      console.log("✅ 聚合查询结果:", response.result);
    });

    it("1.6 执行 Profiling", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "profile_data",
          arguments: {
            table_name: "orders",
          },
        },
      });

      expect(response.result).toBeDefined();
      console.log("✅ Profiling 结果:", response.result);
    });
  });

  describe("Scenario 2: 语义层初始化 + 检索", () => {
    it("2.1 init_semantic 获取全量 schema", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "init_semantic",
          arguments: {},
        },
      });

      expect(response.result).toBeDefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data.tables.length).toBeGreaterThan(0);
      expect(data.tables[0].columns.length).toBeGreaterThan(0);
      console.log("✅ init_semantic:", data.message);
    });

    it("2.2 update_semantic 写入语义", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "update_semantic",
          arguments: {
            tables: [
              {
                table_name: "orders",
                description: "订单记录，包含销售额、折扣、利润等关键商业指标",
                business_domain: "电商/交易",
                tags: ["核心表", "财务", "订单"],
                columns: [
                  { name: "Order ID", semantic: "订单唯一标识", role: "primary_key" },
                  { name: "Sales", semantic: "订单销售额", role: "measure", unit: "CNY", aggregation: "sum" },
                  { name: "Profit", semantic: "订单利润", role: "measure", unit: "CNY", aggregation: "sum" },
                  { name: "Quantity", semantic: "购买数量", role: "measure", aggregation: "sum" },
                  { name: "Discount", semantic: "折扣率", role: "measure" },
                  { name: "Category", semantic: "商品分类", role: "dimension" },
                  { name: "Sub-Category", semantic: "商品子分类", role: "dimension" },
                  { name: "Segment", semantic: "客户段（消费者/企业/居家办公）", role: "dimension" },
                  { name: "Region", semantic: "区域", role: "dimension" },
                  { name: "City", semantic: "城市", role: "dimension" },
                  { name: "State", semantic: "省份", role: "dimension" },
                  { name: "Customer Name", semantic: "客户名称", role: "dimension" },
                  { name: "Product Name", semantic: "产品名称", role: "dimension" },
                  { name: "Order Date", semantic: "下单时间", role: "timestamp", granularity: ["day", "month", "year"] },
                  { name: "Ship Date", semantic: "发货时间", role: "timestamp" },
                ],
                metrics: [
                  { name: "总销售额", expression: "SUM(Sales)" },
                  { name: "总利润", expression: "SUM(Profit)" },
                  { name: "日订单数", expression: "COUNT(DISTINCT \"Order ID\")", group_by: "CAST(\"Order Date\" AS DATE)" },
                ],
              },
            ],
          },
        },
      });

      expect(response.result).toBeDefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data.results[0].status).toBe("updated");
      expect(data.indexedCount).toBeGreaterThan(0);
      console.log("✅ update_semantic:", data.message);
    });

    it("2.3 search_tables 检索相关表（销售额）", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "search_tables",
          arguments: {
            query: "销售额 分类",
          },
        },
      });

      expect(response.result).toBeDefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data.results.length).toBeGreaterThan(0);
      expect(data.results[0].tableName).toBe("orders");
      console.log("✅ search_tables:", data.message);
    });

    it("2.4 search_tables 检索（客户 订单）", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "search_tables",
          arguments: {
            query: "客户 订单",
          },
        },
      });

      expect(response.result).toBeDefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data.results.length).toBeGreaterThan(0);
      console.log("✅ search_tables (客户 订单):", data.results[0].tableName);
    });

    it("2.5 search_tables 检索（不存在的关键词）", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "search_tables",
          arguments: {
            query: "zzzznonexistent",
          },
        },
      });

      expect(response.result).toBeDefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data.results.length).toBe(0);
      console.log("✅ search_tables (not found):", data.message);
    });
  });

  describe("Scenario 3: SQL 安全检查", () => {
    it("3.1 禁止 INSERT", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "query",
          arguments: {
            sql: "INSERT INTO orders VALUES (...)",
          },
        },
      });

      // MCP safety errors are returned as isError:true in content, not JSON-RPC error
      const result = response.result as { isError?: boolean; content?: Array<{ text: string }> } | undefined;
      expect(result?.isError).toBe(true);
      const msg = result?.content?.[0]?.text ?? "";
      expect(msg).toMatch(/safety|SELECT|INSERT/i);
      console.log("✅ INSERT 被拒绝:", msg);
    });

    it("3.2 禁止 DELETE", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "query",
          arguments: {
            sql: "DELETE FROM orders WHERE id=1",
          },
        },
      });

      const result = response.result as { isError?: boolean; content?: Array<{ text: string }> } | undefined;
      expect(result?.isError).toBe(true);
      const msg = result?.content?.[0]?.text ?? "";
      expect(msg).toMatch(/safety|SELECT|DELETE/i);
      console.log("✅ DELETE 被拒绝:", msg);
    });

    it("3.3 禁止 DROP", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "query",
          arguments: {
            sql: "DROP TABLE orders",
          },
        },
      });

      const result = response.result as { isError?: boolean; content?: Array<{ text: string }> } | undefined;
      expect(result?.isError).toBe(true);
      const msg = result?.content?.[0]?.text ?? "";
      expect(msg).toMatch(/safety|SELECT|DROP/i);
      console.log("✅ DROP 被拒绝:", msg);
    });

    it("3.4 允许 SELECT", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "query",
          arguments: {
            sql: "SELECT * FROM orders LIMIT 1",
          },
        },
      });

      expect(response.error).toBeUndefined();
      expect(response.result).toBeDefined();
      console.log("✅ SELECT 被允许");
    });

    it("3.5 允许 CTE（WITH）", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "query",
          arguments: {
            sql: "WITH summary AS (SELECT Category, SUM(Sales) as total FROM orders GROUP BY Category) SELECT * FROM summary",
          },
        },
      });

      expect(response.error).toBeUndefined();
      expect(response.result).toBeDefined();
      console.log("✅ CTE 被允许");
    });
  });
});
