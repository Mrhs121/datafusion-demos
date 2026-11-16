# mini-kylin：DataFusion 上的最小 Cube 路由 + Aggregate 改写

一个可运行的 "mini Kylin"：

- 基表 `sales` 以 **Parquet** 存储（换成 Iceberg 只需替换注册方式）；
- Cube `sales_by_region_category` 是 `(region, category)` 粒度的预聚合 Parquet，带
  `amount`（SUM）、`cnt_rows`、`cnt_amount`、`min_amount`、`max_amount`；
- 一条 SQL 是否命中 cube 由 **hit 配置**决定：`归一化 SQL -> cube`；
- 命中后分两种执行方式：
  - **Scan**：全是 `SUM(度量)`，把 `sales` 注册成 cube Parquet，SQL 不变；
  - **Rewrite**：出现 `COUNT(*)` / `COUNT(x)` / `AVG` / `MIN` / `MAX`，把逻辑计划的
    `Aggregate` 改写成读 cube 的预计算度量；
- 未命中、或 cube 覆盖不了查询，扫描基表；
- 每次命中都会把结果和基表结果对比，确认改写没有改变答案；
- `EXPLAIN` 能直接看到 `DataSourceExec` 指向 `data/cube` 还是 `data/base`。

## 用法

```bash
cd output/mini-kylin

# 1) 跑内置的 8 个用例（hit / miss / 改写 / 回退）
cargo run

# 2) 跑自己的 SQL：默认没有 hit 配置，走基表
cargo run -- "SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region"

# 3) 模拟命中：强制把这个 SQL 当作配置了 cube，仍会做覆盖校验
cargo run -- --hit "SELECT region, AVG(amount) AS avg_amount FROM sales GROUP BY region ORDER BY region"
```

第一次运行会下载 DataFusion 依赖、生成 `data/base` 和 `data/cube`；之后复用。
`--hit` 不是绕过校验，它只是把"这个 SQL 在 hit 配置里"设为真：
`cube_mode` 仍会检查维度、聚合和过滤列，不能回答就回退基表。

## 两种命中模式

| 查询 | 模式 | 发生了什么 |
| --- | --- | --- |
| `SELECT region, SUM(amount) ... GROUP BY region` | Scan | cube 的 `amount` 已是 SUM，`SUM(SUM(amount))` 正确 |
| `SELECT region, SUM, COUNT(*), AVG, MIN, MAX ...` | Rewrite | `Aggregate` 改为 `SUM(amount)`, `SUM(cnt_rows)`, `SUM(amount)/SUM(cnt_amount)`, `MIN(min_amount)`, `MAX(max_amount)` |
| `SELECT region, AVG(amount) ... WHERE region = 'east' GROUP BY region` | Rewrite | 同上，并保留 `Filter`；只把 `sales` 的 `TableScan` 换成 cube |
| `COUNT(DISTINCT amount)` | Miss | 没有预计算 HLL/bitmap，回退基表 |
| `WHERE sale_id > 100` | Miss | `sale_id` 不在 cube，无法下推，回退基表 |
| `WHERE amount > 10` | Miss | 过滤度量不能直接用 cube 的预聚合值，回退基表 |

Scan 模式成立的前提：**cube 粒度 ≤ 查询粒度，且度量可加**。Rewrite 模式则把非可加聚合
（COUNT/AVG/MIN/MAX）拆成可合并的度量。

## Cube 的构造

```sql
COPY (
  SELECT region, category,
         SUM(amount)   AS amount,       -- 可加度量
         COUNT(*)      AS cnt_rows,     -- 行数
         COUNT(amount) AS cnt_amount,   -- 非空 amount 数
         MIN(amount)   AS min_amount,
         MAX(amount)   AS max_amount
  FROM sales
  GROUP BY region, category
) TO 'data/cube/' STORED AS PARQUET
```

`amount` 在 cube 里是"该 `(region, category)` 分组的 SUM"。查询按 `region` 汇总时，
`SUM(amount)` 就是对 3 个 category 的预聚合值再求和。

## Aggregate 改写规则

| 原始聚合 | 改写后 |
| --- | --- |
| `SUM(x)` | `SUM(x)`（cube 的 `x` 已是 SUM） |
| `COUNT(*)` | `SUM(cnt_rows)` |
| `COUNT(x)` | `SUM(cnt_x)` |
| `AVG(x)` | `SUM(x) / CAST(SUM(cnt_x) AS Float64)` |
| `MIN(x)` | `MIN(min_x)` |
| `MAX(x)` | `MAX(max_x)` |

拒绝：

- `DISTINCT`（`COUNT(DISTINCT x)`、`SUM(DISTINCT x)`）；
- 聚合上的 `FILTER` / `ORDER BY`；
- 参数不是裸列（如 `SUM(amount + 1)`）；
- 未列出的聚合函数。

改写后的逻辑计划（`EXPLAIN` 实际输出）：

```text
Projection: sales.region,
            sum(amount) AS total,
            sum(cnt_rows) AS n,
            sum(amount) / CAST(sum(cnt_amount) AS Float64) AS avg_amount,
            min(min_amount) AS min_amount,
            max(max_amount) AS max_amount
  Aggregate: groupBy=[[sales.region]],
             aggr=[[sum(amount), sum(cnt_rows), sum(cnt_amount),
                    min(min_amount), max(max_amount)]]
    TableScan: sales projection=[region, amount, cnt_rows, cnt_amount, min_amount, max_amount]
```

### 为什么上层计划不用改

`Aggregate` 的输出字段名由聚合表达式决定。改写时给每个新表达式加别名，指回**旧的输出名**：

```text
sum(cnt_rows)                        -> alias "count(Int64(1))"
sum(amount) / CAST(sum(cnt_amount) …) -> alias "avg(sales.amount)"
min(min_amount)                      -> alias "min(sales.amount)"
```

于是 `Aggregate` 上方的 `Projection` / `Sort` / `Limit` 继续按原名字引用，不需要递归改写。
`rewrite_with_cube` 用 `transform_up` 只替换 `Aggregate` 节点；它原来的输入子树被丢弃，
新输入是 cube 的 `TableScan`。

Cube 扫描的表名仍然是 `sales`，所以原来的 `sales.region` 这类限定列名也能直接解析。

## 完整流程

```text
SQL
 ├─ normalize(sql)
 ├─ registry.hit(sql) ──没有──> scan 基表
 └─ 有 cube
     ├─ 用基表上下文 create_logical_plan(sql)
     ├─ cube_mode(cube, plan)
     │    ├─ 维度不在 cube 里 ──> 回退
     │    ├─ 读到 cube 没有的列（如 sale_id）──> 回退
     │    ├─ 全是 SUM(度量) ──> Scan
     │    ├─ 全部可改写 ──> Rewrite
     │    └─ 否则 ──> 回退
     ├─ Scan:    新 SessionContext 中 register_parquet("sales", cube)
     └─ Rewrite: cube_ctx 的 TableScan + rewrite_with_cube(plan)
```

命中判定不只是"配置里有"，还会用逻辑计划做一次覆盖检查：只允许
`GROUP BY` 用 cube 维度、聚合可答、且聚合下方读到的列都在 cube 里。
这样 `WHERE region = 'east'` 可以命中；`WHERE sale_id > 100` 和 `WHERE amount > 10` 会回退基表
——前者列不在 cube，后者过滤的是度量，而 cube 的 `amount` 已经是 SUM，不能当作原始行过滤。

## 代码结构

```text
src/main.rs
├─ Cube / Registry            // hit 配置：归一化 SQL -> cube(path, dims, measures)
├─ build_data()               // 生成基表 Parquet + cube Parquet
├─ cube_mode()                // Scan / Rewrite / None 的覆盖检查
├─ rewrite_agg_expr()         // 单个聚合的改写规则
├─ rewrite_aggregate()        // 构造新的 Aggregate，保留旧输出名
├─ replace_sales_scan()       // 只替换输入里的 sales TableScan，保留 Filter/Projection
├─ rewrite_with_cube()        // transform_up 替换 Aggregate 节点
├─ run_one()                  // 路由单个 SQL（内置用例和 CLI 共用）
├─ run_scan()                 // 换 provider，执行原 SQL
├─ run_rewritten()            // 执行改写后的 LogicalPlan
└─ check_equal()              // 和基表结果对比
```

demo 采用"每查询一个 `SessionContext`"的最简实现；生产引擎通常改成：

1. 一个 `OptimizerRule`（逻辑优化期）做同样的 `Aggregate` 替换；
2. 或一个 `CubeRouter` 的 `TableProvider`，在 `scan()` 里根据会话上下文选择基表/cube；
3. 需要保留原计划里的 filter/projection/join 时，在替换 `TableScan` 的同时做局部改写。

## 运行输出（节选）

```text
=== HIT (scan): cube 'sales_by_region_category' ===
SQL: SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region
scan: .../data/cube
DataSourceExec: file_groups={.../data/cube/...}, projection=[region, amount]
rows=4, elapsed=...
check: scan result == base result

=== HIT (aggregate rewrite): cube 'sales_by_region_category' ===
SQL: SELECT region, SUM(amount) AS total, COUNT(*) AS n, AVG(amount) AS avg_amount,
            MIN(amount) AS min_amount, MAX(amount) AS max_amount
     FROM sales GROUP BY region ORDER BY region
scan: .../data/cube + aggregate rewrite
Projection: ... sum(cnt_rows) AS n, sum(amount) / CAST(sum(cnt_amount) AS Float64) ...
  Aggregate: groupBy=[[sales.region]], aggr=[[sum(amount), sum(cnt_rows), ...]]
    TableScan: sales projection=[region, amount, cnt_rows, cnt_amount, min_amount, max_amount]
rows=4, elapsed=...
check: aggregate rewrite result == base result

=== HIT (scan): === SELECT ... WHERE region = 'east' ...
check: scan result == base result

=== HIT (aggregate rewrite): === SELECT AVG(amount) ... WHERE region = 'east' ...
check: aggregate rewrite result == base result

=== MISS: configured, but the cube cannot answer it ===
SELECT region, COUNT(DISTINCT amount) ...      // 没有 HLL/bitmap
SELECT region, SUM(amount) ... WHERE sale_id > 100   // sale_id 不在 cube
SELECT region, SUM(amount) ... WHERE amount > 10     // 过滤度量，不能扫预聚合

=== MISS: no cube configured for this SQL ===
SELECT category, AVG(amount) ...
```

命中 cube 时扫描 12 行的 cube；未命中时扫描 10,000 行的基表。数据量小，耗时差异不明显，
但 `EXPLAIN` 的扫描路径是确定的；结果对比则确认两种模式都返回和基表相同的数据。

## 换成 Iceberg

基表侧的注册换成 Iceberg 即可，cube 侧仍然是 Parquet：

```rust
// use iceberg_datafusion::IcebergTableProvider;
//
// let provider = IcebergTableProvider::try_new(catalog, namespace, table).await?;
// let ctx = new_context(&config);
// ctx.register_table("sales", Arc::new(provider))?;
```

注意：

- `iceberg-datafusion` 的 DataFusion 版本必须与你的 DF 版本匹配；不匹配时不要强行
  链接两个 DataFusion。
- 如果只需要读 Iceberg 的 Parquet 数据文件，也可以用 `ListingTable` + 自己的 snapshot
  管理，但会失去 Iceberg 的 manifest/快照语义。
- 本 demo 把"基表 scan"抽象成 `register_sales(path)`；换成 provider 注册即可，路由逻辑不变。

## 离真正的 Kylin 还差什么

- **COUNT(DISTINCT)**：需要 HLL / bitmap / 精确去重结构，不能靠普通度量合并；
- **更多聚合**：`STDDEV`/`VAR` 需要 `SUM(x²)`、`SUM(x)`、`COUNT(x)`；`MEDIAN`/分位数需要
  近似结构；
- **多度量**：cube 列名要按度量维度命名（`sum_sales`、`cnt_sales`…），demo 只有一个 `amount`；
- **cuboid 选择**：一个 cube 组通常有多个 cuboid（不同维度组合），要按查询选最合适的；
- **过滤/连接**：`WHERE` 只引用 cube 维度时才能下推到 cube；join 需要按事实表/维度表拆分；
- **部分命中**：查询粒度比 cube 粗可以上卷；比 cube 细则不能命中；
- **cube 维护**：基表更新后的增量刷新、版本/快照、失效；
- **代价模型**：按 cube 行数、扫描字节、聚合代价选择 cuboid；
- **SQL hint/优化器集成**：`/*+ CUBE(name) */` 或自动匹配，最好做成 `OptimizerRule`；
- **权限**：cube 可能暴露基表的聚合信息，需要和基表权限对齐；
- **改写正确性**：生产环境要保留 `check_equal` 这类回归对比，覆盖 NULL、空分组、重复行。

## 依赖

- DataFusion 54（`datafusion = "54"`）
- Arrow 58（由 DataFusion 带入）
- Tokio 1

`Cargo.toml` 里加了 `[workspace]`，让这个 crate 独立于仓库根 workspace，避免被
`crates/*` 的 workspace 吸进去。`rust-toolchain.toml` 固定用 stable，避免继承仓库根
`1.96.0` 的 pin；如果你的环境没有 stable，改成已安装的版本即可。
