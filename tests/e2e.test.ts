import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, type ChildProcess } from "child_process";
import * as readline from "readline";
import * as path from "path";
import * as fs from "fs";
import * as os from "os";

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
  beforeAll(async () => {
    // Clean up old catalog DB and semantic store to avoid state pollution
    const lucidDir = path.join(os.homedir(), ".lucid-skill");
    const catalogPath = path.join(lucidDir, "lucid-catalog.db");
    const semanticStorePath = path.join(lucidDir, "semantic_store");
    if (fs.existsSync(catalogPath)) {
      fs.unlinkSync(catalogPath);
    }
    if (fs.existsSync(semanticStorePath)) {
      fs.rmSync(semanticStorePath, { recursive: true });
    }

    server = spawn("node", [path.join(process.cwd(), "dist/index.js")], {
      stdio: ["pipe", "pipe", "pipe"],
    });

    server.stderr?.on("data", (data) => {
      console.log("SERVER ERROR:", data.toString());
    });

    // Wait for server to start
    await new Promise((resolve) => setTimeout(resolve, 1000));
  });

  afterAll(() => {
    server.kill();
  });

  describe("Scenario 0: get_overview", () => {
    it("0.1 启动时 get_overview 返回空状态", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "get_overview",
          arguments: {},
        },
      });

      expect(response.result).toBeDefined();
      expect(response.error).toBeUndefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data).toHaveProperty("sources");
      expect(data).toHaveProperty("summary");
      expect(data.summary.totalSources).toBe(0);
      console.log("✅ get_overview (empty):", data.summary);
    });
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

    it("1.1b get_overview 连接后返回数据源", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "get_overview",
          arguments: {},
        },
      });

      expect(response.result).toBeDefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data.sources.length).toBeGreaterThan(0);
      expect(data.summary.totalSources).toBeGreaterThan(0);
      expect(data.summary.activeSources).toBeGreaterThan(0);
      console.log("✅ get_overview (after connect):", data.summary);
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

  describe("Scenario 2b: JOIN Path Discovery (ecommerce)", () => {
    it("2b.1 连接 ecommerce CSV 数据源", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "connect_source",
          arguments: {
            type: "csv",
            path: path.join(process.cwd(), "tests/datasets/ecommerce"),
          },
        },
      });

      expect(response.result).toBeDefined();
      expect(response.error).toBeUndefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data.tables.length).toBe(4);
      console.log("✅ ecommerce CSV 连接成功:", data.tables.map((t: { name: string }) => t.name));
    });

    it("2b.2 get_join_paths: orders ↔ customers", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "get_join_paths",
          arguments: {
            table_a: "orders",
            table_b: "customers",
          },
        },
      });

      expect(response.result).toBeDefined();
      expect(response.error).toBeUndefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data.direct_paths.length).toBeGreaterThan(0);
      // Should find orders.customer_id = customers.customer_id
      const hasCustomerId = data.direct_paths.some((p: { join_condition: string }) =>
        p.join_condition.includes("customer_id")
      );
      expect(hasCustomerId).toBe(true);
      console.log("✅ JOIN paths (orders ↔ customers):", data.direct_paths.length, "direct,", data.indirect_paths.length, "indirect");
      console.log("  Best path:", data.direct_paths[0]?.join_condition);
    });

    it("2b.3 get_join_paths: orders ↔ products (indirect via order_items)", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "get_join_paths",
          arguments: {
            table_a: "orders",
            table_b: "products",
          },
        },
      });

      expect(response.result).toBeDefined();
      expect(response.error).toBeUndefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      // Should find indirect path via order_items
      const totalPaths = data.direct_paths.length + data.indirect_paths.length;
      expect(totalPaths).toBeGreaterThan(0);
      console.log("✅ JOIN paths (orders ↔ products):", data.direct_paths.length, "direct,", data.indirect_paths.length, "indirect");
      if (data.indirect_paths.length > 0) {
        console.log("  Indirect via:", data.indirect_paths[0]?.via);
      }
    });

    it("2b.4 get_join_paths: 不存在的表报错", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "get_join_paths",
          arguments: {
            table_a: "nonexistent_table",
            table_b: "customers",
          },
        },
      });

      expect(response.result).toBeDefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBe(true);
      console.log("✅ 不存在的表报错:", result.content?.[0]?.text);
    });
  });

  describe("Scenario 2c: Business Domain Clustering", () => {
    it("2c.0 连接 HR CSV 数据源", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "connect_source",
          arguments: {
            type: "csv",
            path: path.join(process.cwd(), "tests/datasets/hr"),
          },
        },
      });

      expect(response.result).toBeDefined();
      expect(response.error).toBeUndefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data.tables.length).toBe(2);
      console.log("✅ HR CSV 连接成功:", data.tables.map((t: { name: string }) => t.name));
    });

    it("2c.1 get_business_domains 返回 2+ 个域（ecommerce + HR）", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "get_business_domains",
          arguments: {},
        },
      });

      expect(response.result).toBeDefined();
      expect(response.error).toBeUndefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data.domains.length).toBeGreaterThanOrEqual(2);
      expect(data.total_tables).toBeGreaterThanOrEqual(6);
      // Each domain should have at least 1 table
      for (const domain of data.domains) {
        expect(domain.tables.length).toBeGreaterThan(0);
        expect(domain.keywords.length).toBeGreaterThan(0);
        expect(domain.name).toBeTruthy();
      }
      console.log("✅ get_business_domains:", data.domains.length, "domains found");
      for (const d of data.domains) {
        console.log(`  Domain "${d.name}": ${d.tables.join(", ")}`);
      }
    });

    it("2c.2 get_business_domains 按数据源过滤", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "get_business_domains",
          arguments: {
            datasource: "csv:ecommerce",
          },
        },
      });

      expect(response.result).toBeDefined();
      expect(response.error).toBeUndefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);
      expect(data.domains.length).toBeGreaterThanOrEqual(1);
      // All tables in domains should be from ecommerce
      const ecommerceTables = new Set(["orders", "order_items", "customers", "products"]);
      for (const domain of data.domains) {
        for (const table of domain.tables) {
          expect(ecommerceTables.has(table)).toBe(true);
        }
      }
      console.log("✅ get_business_domains (filtered):", data.domains.length, "domains for ecommerce");
    });
  });

  describe("Scenario 2d: Cross-Source JOIN Discovery", () => {
    it("2d.1 连接两个独立 CSV source", async () => {
      // Connect source A (orders)
      const responseA = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "connect_source",
          arguments: {
            type: "csv",
            path: path.join(process.cwd(), "tests/datasets/cross-source-a"),
          },
        },
      });
      expect(responseA.result).toBeDefined();
      expect(responseA.error).toBeUndefined();
      const resultA = responseA.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(resultA.isError).toBeFalsy();
      const dataA = JSON.parse(resultA.content![0].text);
      expect(dataA.tables.length).toBe(1);
      console.log("✅ Cross-source A 连接成功:", dataA.tables.map((t: { name: string }) => t.name));

      // Connect source B (customers)
      const responseB = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "connect_source",
          arguments: {
            type: "csv",
            path: path.join(process.cwd(), "tests/datasets/cross-source-b"),
          },
        },
      });
      expect(responseB.result).toBeDefined();
      expect(responseB.error).toBeUndefined();
      const resultB = responseB.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(resultB.isError).toBeFalsy();
      const dataB = JSON.parse(resultB.content![0].text);
      expect(dataB.tables.length).toBe(1);
      console.log("✅ Cross-source B 连接成功:", dataB.tables.map((t: { name: string }) => t.name));
    });

    it("2d.2 get_join_paths: 跨 source 的 shop_orders ↔ shop_customers 通过 customer_id", async () => {
      const response = await sendMessage({
        jsonrpc: "2.0",
        id: messageId++,
        method: "tools/call",
        params: {
          name: "get_join_paths",
          arguments: {
            table_a: "shop_orders",
            table_b: "shop_customers",
          },
        },
      });

      expect(response.result).toBeDefined();
      expect(response.error).toBeUndefined();
      const result = response.result as { content?: Array<{ text: string }>; isError?: boolean };
      expect(result.isError).toBeFalsy();
      const data = JSON.parse(result.content![0].text);

      // Should find cross-source paths via customer_id
      expect(data.direct_paths.length).toBeGreaterThanOrEqual(1);
      const customerIdPath = data.direct_paths.find(
        (p: { join_condition: string }) => p.join_condition.includes("customer_id"),
      );
      expect(customerIdPath).toBeDefined();
      expect(customerIdPath.confidence).toBeGreaterThanOrEqual(0.65);
      console.log("✅ Cross-source JOIN paths (shop_orders ↔ shop_customers):", data.direct_paths.length, "direct");
      console.log("  Best path:", customerIdPath.join_condition, "confidence:", customerIdPath.confidence);
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
