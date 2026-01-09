# StarRocks × SQLMesh

## System Test Design & Execution Guide

> **目标**
> 验证：在真实用户使用路径下，SQLMesh 在 StarRocks 上是否
> **稳定、可预测、符合 SQLMesh 语义，并且完整覆盖 StarRocks dialect 的实现能力边界**

---

## 0️⃣ 测试范围与原则（先定边界）

### System Test **要验证的**

* SQLMesh **已决策**的 plan，StarRocks dialect 是否：

  * 能完整执行（DDL / DML）
  * 执行结果正确
  * 行为稳定（可重复、可恢复）

### System Test **不验证的**

* SQLMesh 的决策算法正确性
* SQLMesh 的跨数据库通用逻辑
* SQL 语义本身的正确性

---

## 1️⃣ 覆盖内容总览（StarRocks dialect 视角）

### dialect 在 system test 中需要被覆盖的能力

| 能力类别                            | 是否必须覆盖   |
| ------------------------------- | -------- |
| CREATE / DROP TABLE             | ✅        |
| CREATE / REPLACE VIEW           | ✅        |
| CREATE / DROP MATERIALIZED VIEW | ✅        |
| INSERT SELECT                   | ✅        |
| INCREMENTAL（time-range）         | ✅        |
| 全量 rebuild                      | ✅        |
| partitioned_by                  | ✅        |
| distribution / bucket           | ✅        |
| properties                      | ✅        |
| schema rebuild（新增列）             | ✅        |
| alter table                     | ❌（明确不支持） |

---

## 2️⃣ System Test Case 总览（含优先级）

> **优先级定义**
>
> * **P0**：高频使用 + 强 dialect 覆盖（必须）
> * **P1**：中频使用 / 演进场景（建议）
> * **P2**：低频 / 边缘行为（可选）

### 测试用例一览表

| Case ID | 名称                    | 优先级 | 主要目的                |
| ------- | --------------------- | --- | ------------------- |
| ST-01   | 项目初始化（bootstrap）      | P0  | 验证基础 DDL / DAG      |
| ST-02   | 增量模型运行                | P0  | 验证 incremental 核心路径 |
| ST-03   | 增量 SQL 非破坏性变更         | P0  | 验证 no-backfill      |
| ST-04   | 增量 SQL 破坏性变更          | P0  | 验证 backfill         |
| ST-05   | FULL → INCREMENTAL 演进 | P1  | 验证 kind evolution   |
| ST-06   | MV SQL 变更             | P1  | 验证 MV rebuild       |
| ST-07   | Schema 演进（新增列）        | P1  | 验证 rebuild fallback |
| ST-08   | DAG 级联变更              | P2  | 验证依赖传播              |

---

## 3️⃣ 公共测试 Project 结构（所有 case 共用）

```text
system_test_project/
  models/
    raw_orders.sql
    orders_view.sql
    orders_full.sql
    orders_incremental.sql
    orders_summary_mv.sql
  seeds/
    raw_orders.csv
```

### 基础模型说明

#### `raw_orders`（seed / source）

* columns: order_id, user_id, amount, ds

#### `orders_view`

* kind: VIEW
* 用于测试 view replace

#### `orders_full`

* kind: FULL
* partitioned_by ds
* distributed_by order_id

#### `orders_incremental`

* kind: INCREMENTAL_BY_TIME_RANGE
* time_column = ds
* partitioned_by ds
* distributed_by order_id

#### `orders_summary_mv`

* kind: VIEW
* materialized = true
* refresh supported

### Value-form 覆盖策略（对齐 `test_design.md`）

> **目标**：验证 **StarRocks dialect 在真实 `sqlmesh plan/apply` 路径下** 能处理关键属性的不同 value 形式。但 system test 不做“组合覆盖”。

本 system test project 的模型会覆盖（代表性子集）：

* **Partition**：
  * RANGE / LIST / 表达式三类中的代表形式（单列、多列、列为时间函数表达式）
* **Distribution**：
  * `HASH(...) [BUCKETS n]` / `RANDOM [BUCKETS n]`
  * string form 与 structured form（`(kind=HASH|RANDOM, expressions=..., buckets=...)`）
* **Table Key types**：`primary_key` / `duplicate_key` / `unique_key` / `aggregate_key` 的典型 value form（string/tuple）
* **Order By / Clustered By**：`order_by` / `clustered_by` 的 tuple / string 代表形式
* **Generic properties**：`replication_num` / boolean 等代表形式
* **View / MV**：`SECURITY <value>` 与 MV `REFRESH ...` 的代表形式

---

## 4️⃣ 详细 Test Case 设计

> **执行约定补充**
>
> * `kind=FULL` 的模型在 SQLMesh 中默认只会重放 metadata，不会自动重算历史数据。因此当测试需要强制 FULL 模型 backfill 时，必须显式传入 `--restate-model <model>`。
> * `kind=INCREMENTAL_BY_TIME_RANGE` 必须结合 `--start` / `--end`，否则第一次 plan 会直接计算到“当前时间”。在 system test 中，我们通过指定时间窗口将增量计算拆成多次运行。Production 的初次 run 不允许指定 start/end，因此需要首次跑之后，再通过 `--restate-model starrocks_system_test.orders_incremental` 的方式触发复算。
> * 示例命令统一用 `sqlmesh plan dev --skip-tests --start <YYYY-MM-DD> --end <YYYY-MM-DD> [--restate-model ...]`，执行过程中在提示时输入 `y` 即可 apply（或使用 `--auto-apply`）。

---

### 🧪 ST-01：项目初始化（Bootstrap）

**优先级**：P0
**目的**：验证 StarRocks dialect 的最小完整能力闭环

#### 覆盖的 dialect 能力

* create table
* create view
* create materialized view
* insert select
* 前面 case 中加入 partition / distribution 等的不同 value form

#### 步骤（ST-01）

1. 初始化 project。以及创建数据库

    ```bash
    mkdir system_test_project & cd system_test_project

    sqlmesh init
    ```

    ```SQL
    create database if NOT EXISTS starrocks_system_test;
    ```

2. 执行：

   ```bash
   # 进入 system test project 根目录
   # cd system_test_project

   # 清理旧的 duckdb state（可选）
   rm -f ./state/system_test_state.db

   # 生成计划并在提示时输入 y 进行 apply（无需额外的 apply 命令）
   sqlmesh plan dev --skip-tests --start 2025-01-01 --end 2025-01-04
   ```

   > 提示 `Apply plan? [y/n]:` 时输入 `y`。如果需要完全无交互，可使用 `sqlmesh plan --skip-tests --auto-apply`。

3. 验证（StarRocks SQL shell）：

   ```sql
   -- 对象存在性
   SHOW TABLES FROM starrocks_system_test;
   SHOW MATERIALIZED VIEWS FROM starrocks_system_test.orders_incremental;

   -- DDL 细节（partition / distribution / properties）
   SHOW CREATE TABLE starrocks_system_test.orders_full;
   SHOW PARTITIONS FROM starrocks_system_test.orders_full;

   -- 数据正确性
   SELECT COUNT(*) FROM starrocks_system_test.orders_full;
   SELECT * FROM starrocks_system_test.orders_summary_mv ORDER BY ds, user_id;
   ```

4. 记录：

   * 保存 `SHOW CREATE TABLE` / `SHOW PARTITIONS` 输出到 `logs/ST-01/`
   * 异常时保留完整 CLI + SQL shell 输出，方便回溯

---

### 🧪 ST-02：增量模型运行

**优先级**：P0
**目的**：验证 incremental 正常运行路径

#### 覆盖内容

* incremental insert
* time range filter
* partition append

#### 步骤（ST-02）

1. 向 `raw_orders` 增加新日期数据：
   1. 运行 `data/st-02/find_physical_table.sql` 获取当前 physical table 名称（例如 `sqlmesh__starrocks_system_test.starrocks_system_test__raw_orders__3488006467`）
   2. 依次执行 `data/st-02/insert_batch_1.sql` / `insert_batch_2.sql` / `insert_batch_3.sql`（脚本里已填入当前 hash；这些批次专用于 ST-02，可分多次运行以模拟连续增量）
2. 执行增量计划（指定时间窗口 + 强制 FULL backfill，执行3次操作）：

   ```bash
   sqlmesh plan dev --skip-tests --start 2025-01-05 --end 2025-01-06 --restate-model starrocks_system_test.orders_full

   sqlmesh plan dev --skip-tests --start 2025-01-07 --end 2025-01-08 --restate-model starrocks_system_test.orders_full

   sqlmesh plan dev --skip-tests --start 2025-01-09 --end 2025-02-01 --restate-model starrocks_system_test.orders_full
   ```

3. 验证：

   * 仅新增分区被写入
   * 历史分区不变

---

### 🧪 ST-03：增量 SQL 非破坏性变更

**优先级**：P0
**目的**：验证 no-backfill 行为

#### 变更内容（ST-03）

* 在 `orders_incremental` 中新增一列 `amount_double`，并在查询中赋值：

   ```diff
   columns (
       order_id INT,
       user_id INT,
       region VARCHAR(20),
       amount DECIMAL(10, 2),
       ts BIGINT,
       ds DATE,
   +   amount_double DECIMAL(10, 2),
   )
   ```

   ```diff
   SELECT
       order_id,
       user_id,
       region,
       amount,
       ts,
   +   amount * 2 AS amount_double,
       ds
   FROM @raw_orders
   WHERE ds BETWEEN @start_ds AND @end_ds;
   ```

* `orders_summary_mv` 会受到**间接/Indirect**影响（新增列需要同步变更 `SELECT` 或保持聚合逻辑），因此需要一并 restate。

#### 步骤（ST-03）

1. 修改 `models/orders_incremental.sql` 如上，并在 `models/orders_summary_mv.sql` 中引用新列（例如增加 `SUM(amount_double)` 或保留原列以确保 schema 对齐）
   > 不过当前不新增的情况下，`orders_summary_mv` 也会被认定 `Indirect breaking`.
2. 执行 plan（选择 ST-02 之后的时间窗口，例如 2025-02-02～2025-02-03）：

   ```bash
   sqlmesh plan dev \
     --skip-tests \
     --start 2025-02-02 \
     --end 2025-02-03 \
     --restate-model starrocks_system_test.orders_incremental \
     --restate-model starrocks_system_test.orders_summary_mv
   ```

3. 验证：

   * plan 中 **不包含 backfill**
   * 新列仅在 restated 时间窗口生效
   * MV 的列结构和数据同步更新

---

### 🧪 ST-04：增量 SQL 破坏性变更

**优先级**：P0
**目的**：验证 backfill 路径

#### 变更内容（ST-04）

* 执行 `data/st-02/insert_overlap_backfill.sql`，写入与 2025-01-06 旧数据重叠的新值
* 修改 `orders_incremental` 的过滤逻辑（示例：禁止 `region = 'apac'`）

  ```diff
  WHERE ds BETWEEN @start_ds AND @end_ds
  +    AND region != 'apac'
  ```

  或者调整 JOIN / 聚合条件，确保历史分区需要重新计算。

#### 步骤（ST-04）

1. 运行 overlap insert，制造冲突数据
2. 修改 `orders_incremental.sql` 的 WHERE（或 JOIN）逻辑
3. 执行 plan，强制重算冲突日期：

   ```bash
   sqlmesh plan dev \
     --skip-tests \
     --start 2025-01-06 \
     --end 2025-01-06 \
     --restate-model starrocks_system_test.orders_incremental \
     --restate-model starrocks_system_test.orders_full
   ```

#### 验证重点（ST-04）

* plan 显示 backfill（restated 模型需要 delete+insert）
* 2025-01-06 的数据在 `orders_incremental` 中被更新为新值

---

### 🧪 ST-05：FULL → INCREMENTAL 演进

**优先级**：P1
**目的**：验证 kind evolution 下 dialect 稳定性

#### 步骤（ST-05）

1. 初始使用 `orders_full_4_evolution`
2. 修改为 `INCREMENTAL_BY_TIME_RANGE`
3. 执行 `sqlmesh plan dev --start 2025-01-10 --end 2025-01-12 --restate-model starrocks_system_test.orders_full`（提示 apply 时输入 `y`，或使用 `--auto-apply`）

#### 验证

* 原表被 drop
* 新表按 incremental 创建
* 全量 backfill 成功

---

### 🧪 ST-06：MV SQL 变更

**优先级**：P1
**目的**：验证 materialized view rebuild

#### 步骤（ST-06）

1. 修改 MV 的 SELECT SQL

   ```diff
   MODEL (
      ...
      columns (
   -     order_cnt BIGINT,
   -     gross_amount DECIMAL(18, 2)
   +     distinct_orders BIGINT,
   +     gross_amount DECIMAL(18, 2),
   +     gross_amount_expr DECIMAL(18, 2)
      )
   );

   SELECT
         ds,
         user_id,
   -     COUNT(*) AS order_cnt,
   -     SUM(amount) AS gross_amount
   +     COUNT(DISTINCT order_id) AS distinct_orders,
   +     SUM(amount) AS gross_amount,
   +     SUM(amount_double) AS gross_amount_double
   FROM @orders_incremental
   GROUP BY ds, user_id;
   ```

2. 执行 `sqlmesh plan dev --start 2025-01-13 --end 2025-01-13 --restate-model starrocks_system_test.orders_summary_mv`

#### 验证（ST-06）

* drop + create MV
* refresh 参数正确

---

### 🧪 ST-07：Schema 演进（新增 nullable 列）

**优先级**：P1
**目的**：验证 alter 不支持时的 rebuild fallback

#### 变更内容（ST-07）

* 在 orders_full.sql 新增 nullable 列，并在 SELECT 中赋值：

   ```diff
     columns (
         order_id INT,
         user_id INT,
         region VARCHAR(20),
         amount DECIMAL(10, 2),
         ts BIGINT,
         ds DATE,
   +     comment VARCHAR(100)
     )

     SELECT
         order_id,
         user_id,
         region,
         amount,
         ts,
         ds,
   +     NULL AS comment
     FROM starrocks_system_test.raw_orders;
   ```

#### 验证（ST-07）

* SQLMesh 选择 rebuild
* dialect 成功 drop + create + backfill

---

### 🧪 ST-08：DAG 级联变更

**优先级**：P2
**目的**：验证依赖传播

#### 变更内容（ST-08）

* 修改 `orders_view` SQL

#### 验证（ST-08）

* 下游 model 进入 plan
* 执行顺序正确

---

## 5️⃣ 覆盖矩阵（最终检查）

| 能力                 | 覆盖 case               |
| ------------------ | --------------------- |
| create table       | ST-01                 |
| incremental insert | ST-02                 |
| rebuild            | ST-04 / ST-05 / ST-07 |
| partition          | ST-01 / ST-02         |
| distribution       | ST-01                 |
| MV                 | ST-01 / ST-06         |
| kind evolution     | ST-05                 |
| DAG                | ST-08                 |

✅ **100% 覆盖 StarRocks dialect 的 system-level 风险面**

---

## 6️⃣ 最后一个重要建议（经验之谈）

> **System test 文档本身就是 dialect 的“行为契约”**

未来如果：

* StarRocks 升级
* SQLMesh 行为变化
* dialect 增加 alter 支持

👉 你只需要看：
**哪些 case 的“预期行为”要改**
