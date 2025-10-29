# StarRocks Engine Adapter 设计文档

> **文档目的**：指导在 SQLMesh 中实现 StarRocks 数据库的完整支持
> **基础参考**：复用 Doris Engine Adapter 的实现（StarRocks 与 Doris 高度相似）
> **创建日期**：2025-10-21
> **目标版本**：SQLMesh 0.x + StarRocks 3.x

---

## 目录

1. [概述](#1-概述)
2. [SQLMesh 架构与 Engine Adapter 的角色](#2-sqlmesh-架构与-engine-adapter-的角色)
3. [Doris Engine Adapter 深度分析](#3-doris-engine-adapter-深度分析)
4. [方法重载的设计原则](#4-方法重载的设计原则)
5. [核心概念详解](#5-核心概念详解)
6. [StarRocks vs Doris 差异分析](#6-starrocks-vs-doris-差异分析)
7. [StarRocks Adapter 实现方案](#7-starrocks-adapter-实现方案)
8. [SQLGlot Dialect 支持](#8-sqlglot-dialect-支持)
9. [测试策略](#9-测试策略)
10. [实施路线图](#10-实施路线图)

---

## 1. 概述

### 1.1 为什么需要 Engine Adapter？

SQLMesh 是一个**数据库无关**的数据转换框架，需要与多种数据库引擎交互。Engine Adapter 充当 SQLMesh 核心逻辑与具体数据库之间的**适配层**，负责：

- **SQL 方言转换**：将通用 SQL 转换为特定数据库的方言
- **DDL 操作**：创建/删除表、视图、物化视图、索引等
- **DML 操作**：数据插入、更新、删除、回填
- **元数据查询**：获取表结构、数据对象列表、统计信息
- **事务管理**：处理数据库的事务语义（如果支持）

### 1.2 StarRocks 与 Doris 的关系

StarRocks 是从 Doris 分支出来的项目，两者共享大量底层架构：

**相同点**：
- 使用 **MySQL 协议**进行连接
- 基本兼容 **MySQL 语法**
- 采用 **MPP 架构**（大规模并行处理）
- 支持**列式存储**和**向量化执行**
- **不支持事务**（非 ACID 数据库）
- Schema 管理方式相同（DATABASE = Schema）
- 分区和分布式表语法相似

**差异点**（需要重点关注）：
- **主键语法**：StarRocks 原生支持 `PRIMARY KEY`，Doris 使用 `UNIQUE KEY`
- **物化视图**：语法和刷新机制可能有细微差异
- **特殊函数**：某些聚合函数、窗口函数的实现可能不同
- **性能优化**：各自有不同的查询优化器特性

### 1.3 实现策略

采用**继承复用策略**：
```
EngineAdapter (基类)
    ↓
DorisEngineAdapter (复用 90% 的实现)
    ↓
StarRocksEngineAdapter (只重载差异部分)
```

---

## 2. SQLMesh 架构与 Engine Adapter 的角色

### 2.1 SQLMesh 完整执行流程

用户操作 SQLMesh 时，Engine Adapter 在整个生命周期中扮演关键角色。以下是详细的执行流程说明：

#### 阶段 1: 项目初始化 (`sqlmesh init`)

这个阶段不涉及 Engine Adapter，主要是：
- 用户选择数据库类型（如 StarRocks）
- 生成配置文件（config.yaml）
- 创建示例模型文件

#### 阶段 2: 计划创建 (`sqlmesh plan`)

这是 Engine Adapter **第一次被使用**的阶段，主要用于查询数据库的当前状态：

**2.1 Context 初始化**

当用户执行 `sqlmesh plan` 时，SQLMesh 会：
1. 读取配置文件（config.yaml 或 config.py）
2. 根据配置中的连接信息创建 Engine Adapter 实例
3. 建立与数据库的连接

配置文件示例：
```yaml
gateways:
  my_gateway:
    connection:
      type: starrocks
      host: localhost
      port: 9030
      user: root
      password: ''
```

SQLMesh 内部会调用：
```python
from sqlmesh.core.engine_adapter import create_engine_adapter

adapter = create_engine_adapter(
    connection_config,
    dialect="starrocks"
)
# 返回 StarRocksEngineAdapter 实例
```

**2.2 查询现有数据对象**

Engine Adapter 需要了解数据库中已存在哪些对象：

```python
# 获取指定 schema 中的所有表和视图
data_objects = adapter.get_data_objects(
    schema_name="my_database",
    object_names={"fact_sales", "dim_customers"}  # 可选过滤
)

# 返回示例：
# [
#   DataObject(schema="my_database", name="fact_sales", type=DataObjectType.TABLE),
#   DataObject(schema="my_database", name="dim_customers", type=DataObjectType.TABLE)
# ]
```

内部实现（重载方法）：
- 查询 `information_schema.tables` 系统表
- 过滤出指定 schema 和表名的记录
- 区分表（BASE TABLE）和视图（VIEW）

**2.3 获取表结构信息**

对于每个已存在的表，需要获取其列定义：

```python
# 获取表的列定义
columns = adapter.columns(table_name="my_database.fact_sales")

# 返回示例：
# {
#   "id": DataType(this="INT"),
#   "customer_id": DataType(this="INT"),
#   "amount": DataType(this="DECIMAL", expressions=[Literal(18), Literal(2)]),
#   "dt": DataType(this="DATE")
# }
```

内部实现：
- 执行 `DESCRIBE TABLE my_database.fact_sales`
- 解析返回的列名和数据类型
- 转换为 SQLGlot 的 DataType 对象

**2.4 计算变更**

SQLMesh 对比模型定义与现有表结构，确定需要执行的操作：
- 哪些表需要新建
- 哪些表需要修改（添加列、修改类型等）
- 哪些表需要删除
- 哪些数据需要回填

这些信息会展示给用户进行确认。

#### 阶段 3: 应用变更 (`sqlmesh apply`)

用户确认计划后，Engine Adapter 开始执行实际的数据库操作：

**3.1 创建 Schema（数据库）**

如果模型引用的 schema 不存在，需要先创建：

```python
adapter.create_schema(
    schema_name="my_database",
    ignore_if_exists=True
)
```

这会生成并执行：
```sql
CREATE DATABASE IF NOT EXISTS `my_database`
```

**重载说明**：Doris/StarRocks 使用 `DATABASE` 关键字而非 `SCHEMA`，因此需要重载 `create_schema()` 方法来指定 `kind="DATABASE"`。

**3.2 创建表**

这是最复杂的操作之一，涉及多个步骤：

```python
adapter.create_table(
    table_name="my_database.fact_sales",
    target_columns_to_types={
        "id": DataType.build("INT"),
        "customer_id": DataType.build("INT"),
        "amount": DataType.build("DECIMAL(18,2)"),
        "dt": DataType.build("DATE")
    },
    primary_key=("id",),
    partitioned_by=[Column("dt")],
    table_properties={
        "distributed_by": Tuple(expressions=[
            EQ(this=Column("kind"), expression=Literal.string("HASH")),
            EQ(this=Column("expressions"), expression=Array(expressions=[Literal.string("id")])),
            EQ(this=Column("buckets"), expression=Literal.number(10))
        ])
    },
    table_description="销售事实表",
    column_descriptions={
        "id": "销售ID",
        "customer_id": "客户ID"
    }
)
```

**内部调用链**：
1. `create_table()` (公共接口，不重载)
2. → `_create_table_from_columns()` (Doris 重载：primary_key → unique_key)
3. → `_build_schema_exp()` (构建列定义)
4. → `_build_table_properties_exp()` (构建表属性)
5. → `_create_table()` (执行 CREATE TABLE)
6. → `execute(exp.Create(...))` (转换为 SQL 并执行)

**生成的 SQL**（StarRocks）：
```sql
CREATE TABLE IF NOT EXISTS `my_database`.`fact_sales` (
    `id` INT COMMENT '销售ID',
    `customer_id` INT COMMENT '客户ID',
    `amount` DECIMAL(18,2),
    `dt` DATE
)
PRIMARY KEY(`id`)
PARTITION BY RANGE(`dt`) ()
DISTRIBUTED BY HASH(`id`) BUCKETS 10
COMMENT '销售事实表'
```

**关键点**：
- StarRocks 支持 `PRIMARY KEY`，直接使用
- Doris 需要转换为 `UNIQUE KEY`
- 表属性（分区、分布）通过 `_build_table_properties_exp()` 构建

**3.3 创建视图**

普通视图和物化视图都通过 `create_view()` 方法创建：

```python
# 普通视图
adapter.create_view(
    view_name="my_database.v_customer_summary",
    query_or_df=Select().select("customer_id", "SUM(amount)").from_("fact_sales").group_by("customer_id"),
    materialized=False,
    replace=True
)

# 物化视图
adapter.create_view(
    view_name="my_database.mv_daily_sales",
    query_or_df=Select(...),
    materialized=True,
    materialized_properties={
        "partitioned_by": [Column("dt")],
        "build": "IMMEDIATE",
        "refresh": "AUTO"
    },
    view_properties={
        "unique_key": Column("customer_id"),
        "distributed_by": {...}
    },
    table_description="每日销售汇总物化视图"
)
```

**内部处理**：
- 如果 `materialized=False`：调用基类的 `create_view()`，生成标准的 `CREATE VIEW` 语句
- 如果 `materialized=True`：调用 `_create_materialized_view()`，生成 `CREATE MATERIALIZED VIEW` 语句

**物化视图的特殊之处**：
- 需要指定刷新策略（AUTO/MANUAL/SCHEDULE）
- 需要指定构建方式（IMMEDIATE/DEFERRED）
- 可能需要分区和分布式属性
- 支持列定义和注释

#### 阶段 4: 数据回填 (Backfill)

这是 Engine Adapter **使用最频繁**的阶段，负责将数据插入到表中：

**4.1 Scheduler 调度**

SQLMesh 的 Scheduler 会：
1. 根据模型的时间列和增量配置，将回填任务划分为多个时间区间
2. 为每个区间创建一个回填任务
3. 并行或顺序执行这些任务

示例：
- 模型定义：`start='2024-01-01'`, `end='2024-12-31'`, `cron='@daily'`
- Scheduler 创建 365 个任务，每个任务处理一天的数据

**4.2 SnapshotEvaluator 执行**

对于每个时间区间，SnapshotEvaluator 会：
1. 构建该区间的查询（渲染模型 SQL，替换时间宏）
2. 调用 Engine Adapter 的插入方法

```python
evaluator.evaluate(
    start="2024-01-01",
    end="2024-01-02"
)
```

**4.3 Engine Adapter 执行插入**

```python
adapter.insert_overwrite_by_time_partition(
    table_name="my_database.fact_sales",
    query_or_df=Select(...).where(
        Column("dt").between("2024-01-01", "2024-01-02")
    ),
    start="2024-01-01",
    end="2024-01-02",
    time_column="dt"
)
```

**内部调用链**：
1. `insert_overwrite_by_time_partition()`
2. → `_insert_overwrite_by_condition()`
3. → 根据 `INSERT_OVERWRITE_STRATEGY` 选择执行路径

**对于 Doris/StarRocks（DELETE-INSERT 策略）**：

步骤 1：删除旧数据
```python
adapter.delete_from(
    table_name="my_database.fact_sales",
    where=Column("dt").between("2024-01-01", "2024-01-02")
)
```

这会生成并执行：
```sql
DELETE FROM `my_database`.`fact_sales`
WHERE `dt` >= '2024-01-01' AND `dt` < '2024-01-02'
```

**特殊处理**：如果 WHERE 条件包含子查询（如 `WHERE id IN (SELECT ...)`），Doris 不支持标准语法，需要转换为 `DELETE ... USING` 语法。这是通过重载 `delete_from()` 方法实现的。

步骤 2：插入新数据
```python
adapter.insert_append(
    table_name="my_database.fact_sales",
    query_or_df=Select(...)
)
```

这会生成并执行：
```sql
INSERT INTO `my_database`.`fact_sales` (`id`, `customer_id`, `amount`, `dt`)
SELECT `id`, `customer_id`, `amount`, `dt`
FROM source_table
WHERE `dt` >= '2024-01-01' AND `dt` < '2024-01-02'
```

**为什么使用 DELETE-INSERT 而非 INSERT OVERWRITE？**

Doris/StarRocks 不支持 Hive/Spark 的 `INSERT OVERWRITE` 语法。通过声明 `INSERT_OVERWRITE_STRATEGY = DELETE_INSERT`，基类会自动使用分步执行的方式。

**4.4 数据验证（可选）**

回填完成后，可能需要验证数据完整性：

```python
count = adapter.fetchone(
    Select().select(func("COUNT", Star())).from_("my_database.fact_sales")
                .where(Column("dt").eq("2024-01-01"))
)[0]

if count == 0:
    raise ValidationError("No data loaded for 2024-01-01")
```

### 2.2 Engine Adapter 的核心职责总结

从上述流程可以看出，Engine Adapter 的职责贯穿整个 SQLMesh 生命周期：

| 阶段 | Engine Adapter 职责 | 主要方法 |
|------|---------------------|----------|
| **计划** | 查询数据库状态 | `get_data_objects()`, `columns()`, `table_exists()` |
| **应用** | 创建和修改对象 | `create_schema()`, `create_table()`, `create_view()` |
| **回填** | 插入和更新数据 | `insert_overwrite_by_time_partition()`, `delete_from()`, `insert_append()` |
| **验证** | 查询数据 | `fetchall()`, `fetchone()` |

**设计原则**：
1. **不直接拼接 SQL 字符串**：使用 SQLGlot Expression 构建语法树
2. **自动方言转换**：通过 `expression.sql(dialect="starrocks")` 生成目标 SQL
3. **统一执行接口**：所有 SQL 通过 `execute(expression)` 执行
4. **声明式配置**：通过类属性声明数据库特性，基类自动选择执行路径
5. **最小重载原则**：只重载必须改变的方法，其他复用基类实现

---

## 3. Doris Engine Adapter 深度分析

### 3.1 类属性配置（声明式特性）

Doris Engine Adapter 通过**类属性**声明其特性，这是一种**策略模式**的应用：

```python
class DorisEngineAdapter(EngineAdapter):
    DIALECT = "doris"
    DEFAULT_BATCH_SIZE = 5000
    SUPPORTS_TRANSACTIONS = False
    # ... 更多配置
```

#### 3.1.1 基础配置

**DIALECT = "doris"**
- **作用**：指定 SQLGlot 方言名称
- **用途**：当调用 `expression.sql()` 时，自动使用此方言生成 SQL
- **示例**：`exp.Create(...).sql(dialect="doris")` 会生成 Doris 兼容的 SQL

**DEFAULT_BATCH_SIZE = 5000**
- **作用**：批量操作时每批处理的记录数
- **用途**：在批量插入或更新时，控制每次提交的数据量
- **为什么需要**：防止单次操作数据量过大导致超时或内存溢出

**SUPPORTS_TRANSACTIONS = False**
- **作用**：声明 Doris/StarRocks 不支持事务
- **影响**：
  - 基类不会自动包装 `BEGIN`/`COMMIT` 语句
  - 数据操作失败时无法回滚，需要应用层处理
  - 回填操作采用分区级别的覆盖，确保原子性

#### 3.1.2 INSERT 策略配置

**INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT**
- **作用**：声明如何实现 INSERT OVERWRITE 语义
- **可选策略**：
  - `INSERT_OVERWRITE`：使用原生 `INSERT OVERWRITE` 语法（Hive/Spark）
  - `DELETE_INSERT`：先 DELETE 再 INSERT（Doris/StarRocks/MySQL）
  - `REPLACE`：使用 `REPLACE INTO` 语法（MySQL）
- **对于 Doris/StarRocks**：必须使用 `DELETE_INSERT`，因为不支持 `INSERT OVERWRITE` 语法
- **执行流程**：
  1. 基类的 `_insert_overwrite_by_condition()` 检测到此策略
  2. 自动调用 `delete_from(where=condition)` 删除旧数据
  3. 然后调用 `insert_append()` 插入新数据
  4. 两步操作**非原子性**，但通过分区级别锁定保证一致性

**为什么不重载 insert_overwrite 方法？**

因为基类已经完美实现了 DELETE-INSERT 逻辑，只需声明策略即可。这是**声明式编程**的典范：
```python
# 基类的实现（伪代码）
if self.INSERT_OVERWRITE_STRATEGY == DELETE_INSERT:
    self.delete_from(table_name, where=condition)
    self.insert_append(table_name, query_or_df)
elif self.INSERT_OVERWRITE_STRATEGY == INSERT_OVERWRITE:
    self.execute(exp.Insert(overwrite=True, ...))
```

只要声明 `INSERT_OVERWRITE_STRATEGY = DELETE_INSERT`，基类自动选择正确路径，无需重载。

#### 3.1.3 数据类型映射

**SCHEMA_DIFFER = MySQLSchemaDiffer**
- **作用**：指定如何比较两个表结构是否相同
- **用途**：在 `sqlmesh plan` 阶段，判断表是否需要变更
- **为什么使用 MySQL**：Doris/StarRocks 的数据类型与 MySQL 高度兼容，直接复用 MySQL 的比较逻辑

**支持的数据类型示例**：
- 数值：INT, BIGINT, DECIMAL(p,s)
- 字符串：VARCHAR(n), CHAR(n), TEXT
- 日期：DATE, DATETIME, TIMESTAMP
- 复杂类型：ARRAY, JSON（取决于版本）

#### 3.1.4 其他配置

**ESCAPE_JSON = True**
- **作用**：在生成 SQL 时，自动转义 JSON 字符串中的特殊字符
- **用途**：防止 JSON 字段中的引号、换行符等破坏 SQL 语法

**SUPPORTS_REPLACE_TABLE = False**
- **作用**：声明不支持 `REPLACE TABLE` 语法
- **影响**：需要先 `DROP TABLE IF EXISTS`，然后 `CREATE TABLE`

### 3.2 方法重载分析

#### 3.2.1 create_schema()

**为什么需要重载？**

Doris 和 StarRocks 使用 `DATABASE` 关键字而非标准的 `SCHEMA`。虽然语义相同，但 SQL 语法不同：

```sql
-- 标准 SQL (PostgreSQL/Snowflake)
CREATE SCHEMA IF NOT EXISTS my_schema;

-- Doris/StarRocks (MySQL 风格)
CREATE DATABASE IF NOT EXISTS my_database;
```

**重载实现**：

```python
def create_schema(
    self,
    schema_name: SchemaName,
    ignore_if_exists: bool = True,
    warn_on_error: bool = True,
    properties: t.Optional[dict] = None,
    kind: str = "DATABASE",  # 关键参数
) -> None:
    super().create_schema(
        schema_name=schema_name,
        ignore_if_exists=ignore_if_exists,
        warn_on_error=warn_on_error,
        properties=properties,
        kind=kind  # 传递给基类
    )
```

**详细说明**：

1. **不改变逻辑，只改变关键字**：重载方法只是将默认的 `kind="SCHEMA"` 改为 `kind="DATABASE"`
2. **调用基类**：所有其他逻辑（如 `IF NOT EXISTS` 的处理）都复用基类实现
3. **向下兼容**：如果用户显式传递 `kind="SCHEMA"`，也会被覆盖为 `DATABASE`

**执行流程**：
1. 用户调用 `adapter.create_schema("my_db")`
2. Doris 重载方法将 `kind` 设置为 `"DATABASE"`
3. 基类的 `create_schema()` 构建 `exp.Create(kind="DATABASE", this=Schema("my_db"), exists=True)`
4. 调用 `execute(expression)`
5. SQLGlot 生成 SQL：`CREATE DATABASE IF NOT EXISTS \`my_db\``

#### 3.2.2 _create_table_from_columns()

**为什么需要重载？**

Doris 使用 `UNIQUE KEY` 而非标准的 `PRIMARY KEY` 来声明唯一约束。这是两者最核心的差异之一：

```sql
-- 标准 SQL (PostgreSQL/MySQL 8.0+)
CREATE TABLE users (
    id INT,
    name VARCHAR(100),
    PRIMARY KEY (id)
);

-- Doris
CREATE TABLE users (
    id INT,
    name VARCHAR(100)
)
UNIQUE KEY(id);
```

**重载实现思路**：

```python
def _create_table_from_columns(
    self,
    table_name: TableName,
    column_definitions: dict,
    primary_key: t.Optional[t.Tuple[str, ...]] = None,
    **kwargs
) -> None:
    # 获取或初始化 table_properties
    table_properties = kwargs.get("table_properties", {})

    # 核心转换逻辑：primary_key → unique_key
    if primary_key and "unique_key" not in table_properties:
        # 将 primary_key 元组转换为 SQLGlot 表达式
        table_properties["unique_key"] = exp.Tuple(
            expressions=[
                exp.to_column(col) for col in primary_key
            ]
        )

    # 更新 kwargs
    kwargs["table_properties"] = table_properties

    # 调用基类，但传递 primary_key=None
    # 这样基类就不会生成 PRIMARY KEY 子句
    super()._create_table_from_columns(
        table_name=table_name,
        column_definitions=column_definitions,
        primary_key=None,  # 关键：阻止基类处理主键
        **kwargs
    )
```

**详细说明**：

1. **接收 primary_key 参数**：从上游（如 Model 定义）接收主键列列表，例如 `("id", "created_at")`
2. **转换为 unique_key 属性**：
   - 将每个列名转换为 `exp.Column` 对象
   - 包装为 `exp.Tuple` 对象（表示多列组合）
   - 添加到 `table_properties` 字典的 `unique_key` 键
3. **阻止基类添加 PRIMARY KEY**：传递 `primary_key=None` 给基类，避免生成标准的主键约束
4. **让 _build_table_properties_exp() 处理**：`unique_key` 属性会在后续的表属性构建阶段转换为 `UNIQUE KEY(...)` 子句

**为什么不直接生成 SQL 字符串？**

因为表属性的处理是高度结构化的：
- 可能有多个属性（unique_key, distributed_by, partition_by 等）
- 需要统一的顺序和格式
- 需要与 SQLGlot 的方言系统集成

通过将 `unique_key` 放入 `table_properties` 字典，交由 `_build_table_properties_exp()` 统一处理，保持了代码的一致性和可维护性。

**StarRocks 需要重载吗？**

不需要！StarRocks 原生支持 `PRIMARY KEY`，可以直接使用基类的实现。这是继承的优势：只重载必须改变的部分。

#### 3.2.3 create_view()

**为什么需要重载？**

普通视图不需要特殊处理，但**物化视图**在 Doris/StarRocks 中有复杂的语法和属性：

```sql
-- 普通视图（标准 SQL）
CREATE VIEW v_summary AS
SELECT customer_id, SUM(amount) FROM sales GROUP BY customer_id;

-- 物化视图（Doris/StarRocks 特有）
CREATE MATERIALIZED VIEW mv_summary
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(customer_id) BUCKETS 10
REFRESH AUTO
AS
SELECT customer_id, SUM(amount), dt FROM sales GROUP BY customer_id, dt;
```

**重载实现思路**：

```python
def create_view(
    self,
    view_name: TableName,
    query_or_df: QueryOrDF,
    materialized: bool = False,
    **kwargs
) -> None:
    if materialized:
        # 物化视图：调用专门的方法
        self._create_materialized_view(
            view_name=view_name,
            query_or_df=query_or_df,
            **kwargs
        )
    else:
        # 普通视图：调用基类
        super().create_view(
            view_name=view_name,
            query_or_df=query_or_df,
            materialized=False,
            **kwargs
        )
```

**_create_materialized_view() 的实现**：

这是一个新增的方法，专门处理物化视图的复杂属性：

```python
def _create_materialized_view(
    self,
    view_name: TableName,
    query_or_df: QueryOrDF,
    materialized_properties: t.Optional[dict] = None,
    view_properties: t.Optional[dict] = None,
    table_description: t.Optional[str] = None,
    column_descriptions: t.Optional[t.Dict[str, str]] = None,
    replace: bool = False,
    **kwargs
) -> None:
    # 1. 构建表属性表达式
    properties_exp = self._build_table_properties_exp(
        catalog_name=None,
        table_properties=view_properties or {},
        partitioned_by=materialized_properties.get("partitioned_by"),
        partition_interval_unit=materialized_properties.get("partition_interval_unit"),
    )

    # 2. 添加物化视图特有的属性
    if "refresh" in materialized_properties:
        # BUILD IMMEDIATE/DEFERRED
        # REFRESH AUTO/MANUAL/SCHEDULE
        properties_exp.expressions.extend([...])

    # 3. 构建 CREATE MATERIALIZED VIEW 表达式
    create_exp = exp.Create(
        kind="MATERIALIZED VIEW",
        this=exp.to_table(view_name),
        exists=replace,
        expression=query_or_df if isinstance(query_or_df, exp.Expression) else parse_one(query_or_df),
        properties=properties_exp
    )

    # 4. 添加注释
    if table_description:
        create_exp.set("comment", exp.Literal.string(table_description))

    # 5. 执行
    self.execute(create_exp)
```

**详细说明**：

1. **分离属性**：
   - `materialized_properties`：物化视图特有的属性（BUILD, REFRESH, PARTITION BY 等）
   - `view_properties`：表级别的属性（UNIQUE KEY, DISTRIBUTED BY 等）
   - 两者合并后传递给 `_build_table_properties_exp()`

2. **复用表属性构建逻辑**：物化视图本质上是一个"可刷新的表"，因此可以复用 `_build_table_properties_exp()` 方法来处理分区、分布等属性

3. **添加特殊属性**：
   - `BUILD IMMEDIATE`：创建后立即构建数据
   - `BUILD DEFERRED`：创建后延迟构建（手动触发）
   - `REFRESH AUTO`：自动刷新（查询时）
   - `REFRESH MANUAL`：手动刷新
   - `REFRESH SCHEDULE`：定时刷新（需要 cron 表达式）

4. **统一执行接口**：最终通过 `execute(exp.Create(...))` 执行，SQLGlot 自动生成目标 SQL

#### 3.2.4 delete_from()

**为什么需要重载？**

Doris 对 `DELETE ... WHERE ... IN (SELECT ...)` 的语法有特殊要求，必须使用 `DELETE ... USING` 语法：

```sql
-- 标准 SQL (PostgreSQL/MySQL 8.0+)
DELETE FROM sales
WHERE customer_id IN (SELECT id FROM blacklist);

-- Doris 要求的语法
DELETE FROM sales
USING blacklist
WHERE sales.customer_id = blacklist.id;
```

此外，还有一个优化：当 WHERE 条件为空时，使用 `TRUNCATE TABLE` 替代 `DELETE`，性能更好。

**重载实现思路**：

```python
def delete_from(
    self,
    table_name: TableName,
    where: t.Optional[ExpOrStr] = None,
) -> None:
    # 优化 1：无条件删除 → TRUNCATE
    if not where:
        self.execute(f"TRUNCATE TABLE {table_name}")
        return

    # 转换为表达式对象
    where_exp = exp.to_column(where) if isinstance(where, str) else where

    # 优化 2：检查是否包含子查询
    if self._has_subquery(where_exp):
        # 转换为 DELETE ... USING 语法
        delete_exp = self._convert_to_delete_using(
            table_name=table_name,
            where=where_exp
        )
    else:
        # 标准 DELETE 语法
        delete_exp = exp.Delete(
            this=exp.to_table(table_name),
            where=where_exp
        )

    self.execute(delete_exp)
```

**_has_subquery() 的实现**：

```python
def _has_subquery(self, where_exp: exp.Expression) -> bool:
    """检查 WHERE 表达式中是否包含子查询"""
    for node in where_exp.walk():
        if isinstance(node, (exp.Subquery, exp.In)):
            return True
    return False
```

**_convert_to_delete_using() 的实现**：

这是最复杂的部分，需要将 `WHERE ... IN (SELECT ...)` 转换为 `DELETE ... USING ... WHERE ...`：

```python
def _convert_to_delete_using(
    self,
    table_name: TableName,
    where: exp.Expression
) -> exp.Delete:
    # 示例：DELETE FROM sales WHERE customer_id IN (SELECT id FROM blacklist)
    # 转换为：DELETE FROM sales USING blacklist WHERE sales.customer_id = blacklist.id

    # 1. 提取子查询
    subquery = self._extract_subquery(where)

    # 2. 提取子查询的 FROM 表
    from_table = subquery.find(exp.From).this

    # 3. 提取关联列
    join_column = self._extract_join_column(where)

    # 4. 构建 DELETE ... USING 表达式
    return exp.Delete(
        this=exp.to_table(table_name),
        using=from_table,
        where=exp.EQ(
            this=exp.Column(this=join_column, table=table_name),
            expression=exp.Column(this="id", table=from_table)
        )
    )
```

**为什么这么复杂？**

1. **语法限制**：Doris 的解析器对子查询支持有限，必须转换为 JOIN 形式
2. **性能优化**：`DELETE ... USING` 通常比子查询性能更好（避免多次扫描）
3. **保持一致性**：SQLMesh 的其他 Adapter 可能使用子查询，Doris Adapter 需要自动转换以保持接口一致

**StarRocks 需要重载吗？**

需要检查 StarRocks 的具体版本：
- 如果 StarRocks 也不支持 `DELETE ... WHERE ... IN (SELECT ...)`，则需要复用 Doris 的实现
- 如果 StarRocks 支持标准语法，则可以不重载，直接使用基类

#### 3.2.5 _build_table_properties_exp()

**为什么需要重载？**

这是 Doris/StarRocks 最复杂的部分：表属性（Table Properties）的构建。这些属性包括：

1. **UNIQUE KEY / PRIMARY KEY**：唯一键约束
2. **PARTITION BY**：分区策略（RANGE / LIST）
3. **DISTRIBUTED BY**：分布策略（HASH / RANDOM）
4. **BUCKETS**：桶数量
5. **PROPERTIES**：其他属性（replication_num, storage_medium 等）
6. **COMMENT**：表注释

标准 SQL 没有统一的表属性语法，每个数据库都有自己的方言。因此，需要重载此方法来生成 Doris/StarRocks 兼容的语法。

**方法签名**：

```python
def _build_table_properties_exp(
    self,
    catalog_name: t.Optional[str] = None,
    table_format: t.Optional[str] = None,
    storage_format: t.Optional[str] = None,
    partitioned_by: t.Optional[t.List[exp.Expression]] = None,
    partition_interval_unit: t.Optional[IntervalUnit] = None,
    clustered_by: t.Optional[t.List[str]] = None,
    table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
    columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    table_description: t.Optional[str] = None,
    table_kind: t.Optional[str] = None,
) -> t.Optional[exp.Properties]:
```

**实现思路**：

```python
def _build_table_properties_exp(self, **kwargs) -> t.Optional[exp.Properties]:
    table_properties = kwargs.get("table_properties", {})
    properties_list = []

    # 1. 处理 UNIQUE KEY (Doris) / PRIMARY KEY (StarRocks)
    if "unique_key" in table_properties:
        properties_list.append(
            exp.Property(
                this="UNIQUE KEY",
                value=table_properties["unique_key"]
            )
        )

    # 2. 处理 PARTITION BY
    partitioned_by = kwargs.get("partitioned_by")
    if partitioned_by:
        # RANGE 分区
        properties_list.append(
            exp.PartitionedByProperty(
                this=exp.RangePartition(
                    expressions=partitioned_by
                )
            )
        )

    # 3. 处理 DISTRIBUTED BY
    if "distributed_by" in table_properties:
        dist_config = table_properties["distributed_by"]
        if dist_config.get("kind") == "HASH":
            properties_list.append(
                exp.Property(
                    this="DISTRIBUTED BY HASH",
                    value=exp.Tuple(expressions=dist_config["columns"])
                )
            )
            properties_list.append(
                exp.Property(
                    this="BUCKETS",
                    value=exp.Literal.number(dist_config["buckets"])
                )
            )

    # 4. 处理其他 PROPERTIES
    if "properties" in table_properties:
        # PROPERTIES ("replication_num" = "3", "storage_medium" = "SSD")
        props = table_properties["properties"]
        properties_list.append(
            exp.Properties(
                expressions=[
                    exp.Property(this=key, value=exp.Literal.string(value))
                    for key, value in props.items()
                ]
            )
        )

    # 5. 处理 COMMENT
    if "table_description" in kwargs:
        properties_list.append(
            exp.Property(
                this="COMMENT",
                value=exp.Literal.string(kwargs["table_description"])
            )
        )

    # 6. 返回合并后的 Properties 对象
    if properties_list:
        return exp.Properties(expressions=properties_list)
    return None
```

**详细说明**：

1. **统一数据结构**：所有属性都表示为 `exp.Property` 对象，包含 `this`（属性名）和 `value`（属性值）

2. **顺序很重要**：Doris/StarRocks 要求表属性按特定顺序出现：
   - UNIQUE KEY / PRIMARY KEY
   - PARTITION BY
   - DISTRIBUTED BY
   - BUCKETS
   - PROPERTIES
   - COMMENT

3. **嵌套结构**：某些属性（如 DISTRIBUTED BY）包含多个子属性，需要构建嵌套的表达式树

4. **类型转换**：
   - 列名 → `exp.Column`
   - 数字 → `exp.Literal.number()`
   - 字符串 → `exp.Literal.string()`

5. **方言支持**：SQLGlot 的 Doris dialect 知道如何将这些 `exp.Property` 对象转换为正确的 SQL 语法

**生成的 SQL 示例**：

```sql
CREATE TABLE sales (
    id INT,
    customer_id INT,
    amount DECIMAL(18,2),
    dt DATE
)
UNIQUE KEY(id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(customer_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "3",
    "storage_medium" = "SSD"
)
COMMENT '销售事实表';
```

**StarRocks 需要重载吗？**

可能需要微调：
- 如果 StarRocks 使用 `PRIMARY KEY` 而非 `UNIQUE KEY`，需要修改属性名
- 如果分区或分布语法有差异，需要调整生成逻辑
- 其他属性（PROPERTIES）应该保持兼容

### 3.3 未重载的方法（复用基类）

以下方法 Doris Adapter **没有重载**，说明基类的实现已经满足需求：

#### 3.3.1 create_table()

**为什么不重载？**

这是一个**模板方法**（Template Method），定义了创建表的整体流程，但具体实现委托给子方法：

```python
# 基类的实现（简化版）
def create_table(
    self,
    table_name: TableName,
    columns_to_types: dict,
    primary_key: tuple = None,
    **kwargs
) -> None:
    # 调用子类可重载的方法
    self._create_table_from_columns(
        table_name=table_name,
        columns_to_types=columns_to_types,
        primary_key=primary_key,
        **kwargs
    )
```

**设计模式原理**：

```
create_table()         [公共 API，定义流程]
    ↓
_create_table_from_columns()  [可重载，处理列定义]
    ↓
_build_table_properties_exp()  [可重载，处理表属性]
    ↓
execute(exp.Create(...))       [执行 SQL]
```

通过这种分层设计：
- 公共 API (`create_table`) 保持稳定，提供统一接口
- 子类只重载需要改变的部分（`_create_table_from_columns`、`_build_table_properties_exp`）
- 最大化代码复用，最小化重复逻辑

#### 3.3.2 create_table_like()

**为什么不重载？**

基类的实现已经使用 SQLGlot Expression 构建 `CREATE TABLE ... LIKE ...` 语句，与 Doris/StarRocks 兼容：

```python
# 基类的实现（简化版）
def create_table_like(
    self,
    target_table_name: TableName,
    source_table_name: TableName,
    **kwargs
) -> None:
    create_exp = exp.Create(
        kind="TABLE",
        this=exp.to_table(target_table_name),
        like=exp.to_table(source_table_name)
    )
    self.execute(create_exp)
```

生成的 SQL：
```sql
CREATE TABLE new_table LIKE old_table;
```

这是标准 MySQL 语法，Doris/StarRocks 完全支持，无需重载。

**为什么不直接拼接 SQL 字符串？**

使用 Expression 的优势：
1. **自动方言转换**：如果未来支持其他数据库，只需增加 dialect 配置
2. **自动转义**：表名、列名中的特殊字符自动转义（如反引号）
3. **类型安全**：编译时检查表达式结构，减少运行时错误
4. **可测试性**：可以在不执行 SQL 的情况下测试表达式构建逻辑

#### 3.3.3 insert_append() / insert_overwrite_by_time_partition()

**为什么不重载？**

因为已经通过 `INSERT_OVERWRITE_STRATEGY = DELETE_INSERT` 声明了执行策略，基类会自动选择正确的执行路径。

**基类的实现逻辑**：

```python
# 基类的 insert_overwrite_by_time_partition（简化版）
def insert_overwrite_by_time_partition(
    self,
    table_name: TableName,
    query_or_df: QueryOrDF,
    start: str,
    end: str,
    time_column: str,
    **kwargs
) -> None:
    # 构建时间过滤条件
    where_condition = exp.And(
        this=exp.GTE(this=exp.Column(time_column), expression=exp.Literal.string(start)),
        expression=exp.LT(this=exp.Column(time_column), expression=exp.Literal.string(end))
    )

    # 调用通用方法
    self._insert_overwrite_by_condition(
        table_name=table_name,
        query_or_df=query_or_df,
        where=where_condition
    )

# 基类的 _insert_overwrite_by_condition（简化版）
def _insert_overwrite_by_condition(
    self,
    table_name: TableName,
    query_or_df: QueryOrDF,
    where: exp.Expression
) -> None:
    if self.INSERT_OVERWRITE_STRATEGY == InsertOverwriteStrategy.DELETE_INSERT:
        # 步骤 1：删除旧数据
        self.delete_from(table_name=table_name, where=where)

        # 步骤 2：插入新数据
        self.insert_append(table_name=table_name, query_or_df=query_or_df)
    elif self.INSERT_OVERWRITE_STRATEGY == InsertOverwriteStrategy.INSERT_OVERWRITE:
        # 使用原生 INSERT OVERWRITE
        insert_exp = exp.Insert(
            this=exp.to_table(table_name),
            overwrite=True,
            expression=query_or_df
        )
        self.execute(insert_exp)
```

这是**策略模式**的典型应用：通过配置属性改变行为，而不是重载方法。

#### 3.3.4 get_data_objects() / columns() / table_exists()

**为什么不重载？**

Doris/StarRocks 实现了 MySQL 兼容的 `information_schema` 系统表，基类的 MySQL-style 实现可以直接使用：

```python
# 基类的实现（简化版）
def get_data_objects(
    self,
    schema_name: str,
    object_names: t.Optional[t.Set[str]] = None
) -> t.List[DataObject]:
    query = f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
    """

    if object_names:
        names_list = "', '".join(object_names)
        query += f" AND table_name IN ('{names_list}')"

    results = self.fetchall(query)

    return [
        DataObject(
            schema=schema_name,
            name=row[0],
            type=DataObjectType.TABLE if row[1] == "BASE TABLE" else DataObjectType.VIEW
        )
        for row in results
    ]

def columns(self, table_name: TableName) -> t.Dict[str, exp.DataType]:
    query = f"DESCRIBE {table_name}"
    results = self.fetchall(query)

    return {
        row[0]: exp.DataType.build(row[1])  # column_name: data_type
        for row in results
    }
```

**为什么这些方法不需要重载？**

1. **information_schema 兼容性**：Doris/StarRocks 完全实现了 MySQL 的 `information_schema.tables`、`information_schema.columns` 等系统表
2. **DESCRIBE 语法支持**：`DESCRIBE TABLE` 返回格式与 MySQL 一致
3. **数据类型兼容**：数据类型名称与 MySQL 保持一致（INT, VARCHAR, DECIMAL 等）

---

## 4. 方法重载的设计原则

### 4.1 何时重载？

根据 Doris Adapter 的经验，以下情况需要重载方法：

#### 4.1.1 SQL 关键字不同

**示例**：`SCHEMA` vs `DATABASE`

- **重载方法**：`create_schema()`
- **原因**：虽然语义相同，但关键字不同，需要指定 `kind="DATABASE"`

#### 4.1.2 语法结构差异

**示例**：`PRIMARY KEY` 约束位置

- **标准 SQL**：`CREATE TABLE t (id INT, PRIMARY KEY (id))`
- **Doris**：`CREATE TABLE t (id INT) UNIQUE KEY(id)`
- **重载方法**：`_create_table_from_columns()`
- **原因**：需要将 primary_key 参数转换为 table_properties 中的 unique_key

#### 4.1.3 不支持标准语法

**示例**：`DELETE ... WHERE ... IN (SELECT ...)`

- **标准 SQL**：`DELETE FROM t WHERE id IN (SELECT id FROM blacklist)`
- **Doris 要求**：`DELETE FROM t USING blacklist WHERE t.id = blacklist.id`
- **重载方法**：`delete_from()`
- **原因**：需要将子查询转换为 JOIN 语法

#### 4.1.4 复杂属性处理

**示例**：物化视图的 REFRESH 策略

- **重载方法**：`create_view()`、`_create_materialized_view()`
- **原因**：物化视图有大量特有属性（BUILD, REFRESH, PARTITION BY 等），需要特殊处理

#### 4.1.5 表属性语法差异

**示例**：`DISTRIBUTED BY HASH(col) BUCKETS 10`

- **重载方法**：`_build_table_properties_exp()`
- **原因**：Doris/StarRocks 特有的分布策略语法，需要构建专用的 Expression 树

### 4.2 何时不重载？

#### 4.2.1 可以通过配置属性解决

**示例**：`INSERT OVERWRITE` 策略

- **不重载**：`insert_overwrite_by_time_partition()`
- **替代方案**：声明 `INSERT_OVERWRITE_STRATEGY = DELETE_INSERT`
- **原因**：基类已经实现了所有策略的逻辑，只需选择即可

#### 4.2.2 数据库已经实现了标准接口

**示例**：`information_schema` 系统表

- **不重载**：`get_data_objects()`、`columns()`
- **原因**：Doris/StarRocks 完全实现了 MySQL 兼容的 `information_schema`

#### 4.2.3 基类的 Expression 生成逻辑已经兼容

**示例**：`CREATE TABLE ... LIKE ...`

- **不重载**：`create_table_like()`
- **原因**：基类使用的 `exp.Create(like=...)` 语法与 Doris/StarRocks 兼容

### 4.3 重载方法的最佳实践

#### 4.3.1 最小化重载范围

**原则**：只重载**必须改变**的方法，其他调用基类

```python
# 好的做法：只改变 kind 参数
def create_schema(self, schema_name, **kwargs):
    super().create_schema(schema_name, kind="DATABASE", **kwargs)

# 坏的做法：重写整个方法
def create_schema(self, schema_name, ignore_if_exists=True, **kwargs):
    # 大量重复代码...
    query = f"CREATE DATABASE {'IF NOT EXISTS ' if ignore_if_exists else ''} {schema_name}"
    self.execute(query)  # 还使用了字符串拼接！
```

#### 4.3.2 优先使用 SQLGlot Expression

**原则**：构建表达式树而非拼接 SQL 字符串

```python
# 好的做法：使用 Expression
def delete_from(self, table_name, where):
    delete_exp = exp.Delete(
        this=exp.to_table(table_name),
        where=exp.to_column(where) if isinstance(where, str) else where
    )
    self.execute(delete_exp)

# 坏的做法：直接拼接
def delete_from(self, table_name, where):
    query = f"DELETE FROM {table_name} WHERE {where}"
    self.execute(query)  # 没有转义，没有类型检查！
```

**优势**：
1. 自动处理转义（表名、列名中的特殊字符）
2. 自动方言转换（`expression.sql(dialect="starrocks")`）
3. 编译时检查语法错误
4. 可测试性更好

#### 4.3.3 分离关注点（Separation of Concerns）

**原则**：将不同职责分离到不同的方法

```python
# 好的做法：分离普通视图和物化视图
def create_view(self, view_name, query_or_df, materialized=False, **kwargs):
    if materialized:
        self._create_materialized_view(view_name, query_or_df, **kwargs)
    else:
        super().create_view(view_name, query_or_df, **kwargs)

def _create_materialized_view(self, view_name, query_or_df, **kwargs):
    # 专门处理物化视图的复杂逻辑
    ...

# 坏的做法：所有逻辑混在一起
def create_view(self, view_name, query_or_df, materialized=False, **kwargs):
    if materialized:
        # 大量 if-else 分支...
        if "refresh" in kwargs:
            # ...
        if "partitioned_by" in kwargs:
            # ...
    else:
        # 普通视图逻辑...
```

#### 4.3.4 保持向下兼容

**原则**：重载方法时，保持与基类相同的方法签名

```python
# 好的做法：保持签名一致，只改变默认值
def _create_table_from_columns(
    self,
    table_name: TableName,
    columns_to_types: dict,
    primary_key: t.Optional[tuple] = None,
    **kwargs  # 保持灵活性
) -> None:
    # 转换逻辑...
    super()._create_table_from_columns(
        table_name=table_name,
        columns_to_types=columns_to_types,
        primary_key=None,  # 阻止基类处理
        **kwargs
    )

# 坏的做法：改变了参数名或类型
def _create_table_from_columns(
    self,
    table: str,  # 不再是 TableName 类型！
    columns: list,  # 不再是 dict 类型！
    unique_key: tuple = None,  # 改变了参数名！
) -> None:
    # ...
```

---

## 5. 核心概念详解

### 5.1 SQLGlot Expression vs SQL 字符串

#### 5.1.1 什么是 SQLGlot Expression？

SQLGlot Expression 是一个 **SQL 抽象语法树（AST）**，以对象的形式表示 SQL 语句的结构：

```python
# SQL 字符串
sql = "SELECT id, name FROM users WHERE age > 18"

# 对应的 SQLGlot Expression
exp.Select(
    expressions=[
        exp.Column(this="id"),
        exp.Column(this="name")
    ],
    from_=exp.From(
        this=exp.Table(this="users")
    ),
    where=exp.Where(
        this=exp.GT(
            this=exp.Column(this="age"),
            expression=exp.Literal.number(18)
        )
    )
)
```

#### 5.1.2 为什么使用 Expression？

**1. 跨方言兼容**

同一个 Expression 可以生成不同数据库的 SQL：

```python
select_exp = exp.Select(...).from_("users").where(exp.Column("age") > 18)

# 生成不同方言的 SQL
select_exp.sql(dialect="mysql")      # MySQL 语法
select_exp.sql(dialect="postgres")   # PostgreSQL 语法
select_exp.sql(dialect="starrocks")  # StarRocks 语法
```

**2. 自动转义**

表名、列名中的特殊字符自动处理：

```python
# 使用 Expression
table = exp.Table(this="my-special-table")  # 包含连字符

# 自动添加反引号
table.sql(dialect="mysql")  # `my-special-table`

# 使用字符串拼接（错误）
sql = f"SELECT * FROM my-special-table"  # 语法错误！
```

**3. 类型安全**

编译时检查结构：

```python
# 使用 Expression：编译时发现错误
create_exp = exp.Create(
    kind="TABLEE",  # Typo！IDE 会提示
    this=exp.Table("users")
)

# 使用字符串：运行时才发现错误
sql = "CREATE TABLEE users (...)"  # 直到执行才报错
```

**4. 可测试性**

可以在不连接数据库的情况下测试：

```python
# 单元测试：验证表达式构建是否正确
def test_create_table_expression():
    adapter = DorisEngineAdapter(...)
    exp = adapter._build_create_table_exp(...)

    # 验证表达式结构
    assert isinstance(exp, exp.Create)
    assert exp.kind == "TABLE"
    assert "UNIQUE KEY" in exp.sql(dialect="doris")
```

### 5.2 exp.Schema vs Database Schema

这是一个容易混淆的概念：

#### 5.2.1 exp.Schema（SQLGlot 的类）

表示 **表的列定义**，相当于 `CREATE TABLE` 语句中的列部分：

```python
# exp.Schema 表示这部分：
CREATE TABLE users (
    id INT,          # ← 这是 exp.Schema
    name VARCHAR(100),
    age INT
) ...

# 代码示例
schema = exp.Schema(
    this=exp.Table("users"),  # 表名
    expressions=[
        exp.ColumnDef(this="id", kind=exp.DataType.build("INT")),
        exp.ColumnDef(this="name", kind=exp.DataType.build("VARCHAR(100)")),
        exp.ColumnDef(this="age", kind=exp.DataType.build("INT"))
    ]
)
```

**命名误导**：`exp.Schema` 这个名字容易让人误解，它不是数据库的 schema（即数据库/模式），而是表的列结构。

#### 5.2.2 Database Schema（数据库模式）

表示 **数据库** 或 **模式**，是表的容器：

```sql
-- 这是 Database Schema（PostgreSQL/Snowflake）
CREATE SCHEMA my_schema;

-- 或者 MySQL/Doris/StarRocks 中的 DATABASE
CREATE DATABASE my_database;
```

在 SQLMesh 中：
- `schema_name` 参数通常指 Database Schema（即数据库名）
- `exp.Schema` 对象指表的列定义

### 5.3 exp.Properties 和属性传递流程

#### 5.3.1 exp.Properties 是什么？

`exp.Properties` 是一个**容器对象**，用于存储表或视图的属性：

```python
properties = exp.Properties(
    expressions=[
        exp.Property(this="UNIQUE KEY", value=exp.Tuple(...)),
        exp.Property(this="DISTRIBUTED BY HASH", value=...),
        exp.Property(this="BUCKETS", value=exp.Literal.number(10)),
        exp.Property(this="COMMENT", value=exp.Literal.string("销售表"))
    ]
)
```

转换为 SQL：
```sql
UNIQUE KEY(...)
DISTRIBUTED BY HASH(...)
BUCKETS 10
COMMENT '销售表'
```

#### 5.3.2 属性的传递流程

属性从哪里来？如何传递到 Engine Adapter？

**流程 1：Model 定义 → Context → Engine Adapter**

```python
# 1. 用户在 Model 中定义（Python 或 YAML）
MODEL (
    name my_database.sales,
    kind FULL,
    partitioned_by (dt),
    physical_properties (
        distributed_by = HASH(customer_id),
        buckets = 10,
        unique_key = (id)
    )
);

# 2. SQLMesh 解析 Model 定义
model = Model(
    name="my_database.sales",
    partitioned_by=[Column("dt")],
    physical_properties={
        "distributed_by": {"kind": "HASH", "columns": ["customer_id"]},
        "buckets": 10,
        "unique_key": ["id"]
    }
)

# 3. Context 调用 Engine Adapter
context.adapter.create_table(
    table_name="my_database.sales",
    columns_to_types={...},
    primary_key=("id",),  # 从 unique_key 转换
    partitioned_by=[Column("dt")],
    table_properties={
        "distributed_by": {...},
        "buckets": 10
    }
)

# 4. Engine Adapter 构建 Expression
adapter._build_table_properties_exp(
    partitioned_by=[Column("dt")],
    table_properties={...}
)
# → 返回 exp.Properties 对象

# 5. 生成 SQL
exp.Create(..., properties=properties_exp).sql(dialect="starrocks")
```

**关键转换点**：

1. **Model → Context**：Model 的 `physical_properties` 被解析为 Python dict
2. **Context → Adapter**：通过 `**kwargs` 传递给 `create_table()` 等方法
3. **Adapter 内部**：`_build_table_properties_exp()` 将 dict 转换为 `exp.Properties` 对象
4. **SQLGlot**：`exp.Properties` 通过方言转换为目标 SQL

### 5.4 模板方法模式（Template Method Pattern）

#### 5.4.1 什么是模板方法模式？

模板方法模式定义了一个算法的**骨架**，将某些步骤延迟到子类中实现。

```python
class EngineAdapter:
    # 公共 API（模板方法）
    def create_table(self, table_name, columns_to_types, **kwargs):
        # 步骤 1：验证参数
        self._validate_table_name(table_name)

        # 步骤 2：调用子类实现（钩子方法）
        self._create_table_from_columns(
            table_name=table_name,
            columns_to_types=columns_to_types,
            **kwargs
        )

        # 步骤 3：日志记录
        logger.info(f"Table {table_name} created successfully")

    # 钩子方法（子类可重载）
    def _create_table_from_columns(self, table_name, columns_to_types, **kwargs):
        # 默认实现
        ...

class DorisEngineAdapter(EngineAdapter):
    # 重载钩子方法
    def _create_table_from_columns(self, table_name, columns_to_types, **kwargs):
        # Doris 特定逻辑：primary_key → unique_key
        ...
        # 调用父类的其他逻辑
        super()._create_table_from_columns(...)
```

**优势**：
1. **代码复用**：验证、日志等通用逻辑在基类中实现
2. **扩展性**：子类只需重载差异部分
3. **一致性**：公共 API 保持稳定，所有 Adapter 接口一致

#### 5.4.2 在 Engine Adapter 中的应用

```
公共 API           钩子方法                    说明
───────────────────────────────────────────────────────
create_table()  → _create_table_from_columns()  处理列定义和约束
                → _build_table_properties_exp() 构建表属性
                → _create_table()              执行 CREATE TABLE

create_view()   → _create_materialized_view()   处理物化视图

insert_overwrite_by_time_partition()
                → _insert_overwrite_by_condition() 根据策略选择执行路径
```

**命名约定**：
- 公共 API：无下划线前缀，如 `create_table()`
- 钩子方法：单下划线前缀，如 `_create_table_from_columns()`
- 内部工具方法：单下划线前缀，如 `_build_table_properties_exp()`

---

## 6. StarRocks vs Doris 差异分析

### 6.1 主键语法差异

#### 6.1.1 Doris

```sql
CREATE TABLE users (
    id INT,
    name VARCHAR(100),
    age INT
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;
```

- 使用 `UNIQUE KEY` 声明唯一性约束
- `UNIQUE KEY` 位于列定义**之后**

#### 6.1.2 StarRocks

```sql
CREATE TABLE users (
    id INT,
    name VARCHAR(100),
    age INT
)
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;
```

- 使用 `PRIMARY KEY` 声明主键
- `PRIMARY KEY` 位置相同

**影响**：
- DorisEngineAdapter 需要重载 `_create_table_from_columns()` 将 `primary_key` 转换为 `unique_key`
- StarRocksEngineAdapter **不需要**重载，直接使用基类的 `PRIMARY KEY` 支持

### 6.2 物化视图差异

需要具体比较 StarRocks 的物化视图语法，可能的差异点：

#### 6.2.1 刷新策略

**Doris**：
```sql
CREATE MATERIALIZED VIEW mv_sales
BUILD IMMEDIATE
REFRESH AUTO
AS SELECT ...;
```

**StarRocks**：
需要检查 StarRocks 官方文档，可能的差异：
- REFRESH 策略关键字不同（ASYNC/MANUAL/INCREMENTAL）
- PARTITION 刷新策略的语法

**建议**：
1. 查阅 StarRocks 最新文档
2. 如果语法相同，复用 Doris 的 `_create_materialized_view()`
3. 如果有差异，重载此方法

### 6.3 分区语法差异

#### 6.3.1 RANGE 分区

两者应该基本一致：

```sql
PARTITION BY RANGE(dt) (
    PARTITION p20240101 VALUES [("2024-01-01"), ("2024-01-02")),
    PARTITION p20240102 VALUES [("2024-01-02"), ("2024-01-03"))
)
```

#### 6.3.2 LIST 分区

需要检查 StarRocks 是否支持 LIST 分区，以及语法是否与 Doris 相同。

### 6.4 数据类型差异

需要比较的数据类型：

| 类型 | Doris | StarRocks | 注意事项 |
|------|-------|-----------|----------|
| 整数 | INT, BIGINT | INT, BIGINT | 应该一致 |
| 浮点 | FLOAT, DOUBLE | FLOAT, DOUBLE | 应该一致 |
| 小数 | DECIMAL(p,s) | DECIMAL(p,s) | 检查精度范围 |
| 字符 | VARCHAR(n), CHAR(n) | VARCHAR(n), CHAR(n) | 检查最大长度 |
| 日期 | DATE, DATETIME | DATE, DATETIME | 应该一致 |
| JSON | JSON | JSON | 版本支持情况 |
| ARRAY | ARRAY<T> | ARRAY<T> | 语法可能不同 |
| MAP | ? | MAP<K,V> | Doris 支持情况 |

**建议**：
1. 创建对照表，记录两者的类型差异
2. 如果差异很小，复用 `MySQLSchemaDiffer`
3. 如果差异较大，创建 `StarRocksSchemaDiffer`

### 6.5 DELETE 语法差异

#### 6.5.1 Doris 的限制

Doris 不支持 `DELETE ... WHERE ... IN (SELECT ...)`，必须使用：

```sql
DELETE FROM t1 USING t2 WHERE t1.id = t2.id;
```

#### 6.5.2 StarRocks 的支持情况

需要测试 StarRocks 是否支持标准语法：

```sql
-- 测试这个语句是否能执行
DELETE FROM sales WHERE customer_id IN (SELECT id FROM blacklist);
```

**如果支持**：StarRocksEngineAdapter 不需要重载 `delete_from()`
**如果不支持**：复用 Doris 的 `delete_from()` 实现

---

## 7. StarRocks Adapter 实现方案

### 7.1 继承策略

```python
from sqlmesh.core.engine_adapter.doris import DorisEngineAdapter

class StarRocksEngineAdapter(DorisEngineAdapter):
    """
    StarRocks Engine Adapter

    StarRocks 与 Doris 高度相似，因此继承 DorisEngineAdapter
    并只重载差异部分。
    """
    DIALECT = "starrocks"
    # 其他属性复用 Doris 的配置
```

**优势**：
1. **最大化代码复用**：90% 的功能直接继承
2. **减少维护成本**：Doris Adapter 的 bug 修复和优化自动继承
3. **保持一致性**：两个 Adapter 的行为高度一致

### 7.2 需要重载的方法列表

根据上述分析，需要重载的方法（**按优先级排序**）：

#### 7.2.1 高优先级（必须重载）

**1. _create_table_from_columns()**

- **原因**：StarRocks 使用 `PRIMARY KEY` 而非 `UNIQUE KEY`
- **实现**：

```python
def _create_table_from_columns(
    self,
    table_name: TableName,
    column_definitions: dict,
    primary_key: t.Optional[t.Tuple[str, ...]] = None,
    **kwargs
) -> None:
    # StarRocks 支持标准的 PRIMARY KEY，直接调用爷类（EngineAdapter）
    # 跳过 DorisEngineAdapter 的 unique_key 转换
    EngineAdapter._create_table_from_columns(
        self,
        table_name=table_name,
        column_definitions=column_definitions,
        primary_key=primary_key,
        **kwargs
    )
```

**注意**：不要调用 `super()._create_table_from_columns()`，因为那会调用 DorisEngineAdapter 的版本（会转换为 unique_key）。应该直接调用 EngineAdapter 的版本。

#### 7.2.2 中优先级（根据测试结果决定）

**2. _create_materialized_view()**

- **原因**：物化视图的 REFRESH 策略可能有差异
- **实现**：需要先测试 StarRocks 的语法，然后决定是否重载

**3. delete_from()**

- **原因**：需要测试 StarRocks 是否支持子查询删除
- **实现**：如果不支持，复用 Doris 的实现

#### 7.2.3 低优先级（可选）

**4. _build_table_properties_exp()**

- **原因**：如果表属性语法有细微差异（如 PROPERTIES 的格式）
- **实现**：先测试 Doris 的实现是否兼容，再决定是否重载

### 7.3 实现步骤

#### 阶段 1：最小可用版本（MVP）

1. **创建 StarRocksEngineAdapter 类**
   ```python
   # sqlmesh/core/engine_adapter/starrocks.py
   from sqlmesh.core.engine_adapter.doris import DorisEngineAdapter

   class StarRocksEngineAdapter(DorisEngineAdapter):
       DIALECT = "starrocks"

       def _create_table_from_columns(self, ...):
           # 重载以支持 PRIMARY KEY
           ...
   ```

2. **注册 Adapter**
   ```python
   # sqlmesh/core/engine_adapter/__init__.py
   from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter

   ADAPTER_REGISTRY = {
       ...
       "starrocks": StarRocksEngineAdapter,
   }
   ```

3. **测试基本功能**
   - 连接测试
   - 创建 Schema/Database
   - 创建表（带 PRIMARY KEY）
   - INSERT/SELECT/DELETE 操作

#### 阶段 2：完善功能

4. **测试物化视图**
   - 创建物化视图
   - 测试 REFRESH 策略
   - 如果有差异，重载 `_create_materialized_view()`

5. **测试 DELETE 语法**
   - 测试子查询删除
   - 如果不支持，保留 Doris 的 `delete_from()` 实现

6. **测试分区表**
   - RANGE 分区
   - LIST 分区（如果支持）
   - 按分区回填数据

#### 阶段 3：集成测试

7. **创建完整的单元测试**
   ```python
   # tests/core/engine_adapter/test_starrocks.py
   class TestStarRocksEngineAdapter:
       def test_create_table_with_primary_key(self):
           ...

       def test_create_materialized_view(self):
           ...

       def test_insert_overwrite_by_time_partition(self):
           ...
   ```

8. **创建集成测试**
   - 完整的 SQLMesh 项目
   - sqlmesh init / plan / apply / run 流程
   - 数据回填验证

### 7.4 代码示例

以下是一个完整的 StarRocksEngineAdapter 初始版本：

```python
"""
StarRocks Engine Adapter
"""
import typing as t
from sqlglot import exp
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.engine_adapter.doris import DorisEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
)

class StarRocksEngineAdapter(DorisEngineAdapter):
    """
    StarRocks Engine Adapter

    StarRocks 是从 Doris fork 出来的项目，两者高度相似。
    主要差异：
    1. StarRocks 支持标准的 PRIMARY KEY，Doris 使用 UNIQUE KEY
    2. 物化视图的 REFRESH 策略可能有差异
    """

    DIALECT = "starrocks"

    def _create_table_from_columns(
        self,
        table_name: exp.Table,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
        **kwargs
    ) -> None:
        """
        重载以支持 StarRocks 的 PRIMARY KEY 语法。

        与 DorisEngineAdapter 不同，StarRocks 支持标准的 PRIMARY KEY，
        不需要转换为 UNIQUE KEY。

        因此跳过 DorisEngineAdapter 的实现，直接调用基类 EngineAdapter。
        """
        # 跳过 DorisEngineAdapter 的 unique_key 转换，直接使用基类的 PRIMARY KEY 支持
        EngineAdapter._create_table_from_columns(
            self,
            table_name=table_name,
            columns_to_types=columns_to_types,
            primary_key=primary_key,
            **kwargs
        )
```

---

## 8. SQLGlot Dialect 支持

### 8.1 为什么需要 Dialect？

SQLGlot 需要知道如何将表达式树转换为 StarRocks 特定的 SQL 语法。

### 8.2 检查现有支持

**第一步**：检查 SQLGlot 是否已经支持 StarRocks

```python
import sqlglot

# 检查可用的方言
print(sqlglot.dialects.Dialects.STARROCKS)  # 如果存在，说明已支持
```

### 8.3 如果不支持

#### 8.3.1 方案 A：复用 Doris Dialect

如果 StarRocks 与 Doris 语法高度相似，可以临时使用 Doris dialect：

```python
class StarRocksEngineAdapter(DorisEngineAdapter):
    DIALECT = "doris"  # 临时使用 Doris dialect
```

**优点**：快速启动，无需修改 SQLGlot
**缺点**：无法处理 StarRocks 特有的语法差异

#### 8.3.2 方案 B：创建 StarRocks Dialect

在 SQLGlot 中添加 StarRocks 方言支持：

```python
# sqlglot/dialects/starrocks.py
from sqlglot import exp
from sqlglot.dialects.doris import Doris

class StarRocks(Doris):
    """
    StarRocks dialect, inheriting from Doris.
    """

    class Tokenizer(Doris.Tokenizer):
        # 如果有特殊关键字，在此添加
        KEYWORDS = {
            **Doris.Tokenizer.KEYWORDS,
            "SOME_STARROCKS_KEYWORD": TokenType.SOME_TYPE,
        }

    class Parser(Doris.Parser):
        # 如果有特殊语法，在此添加
        pass

    class Generator(Doris.Generator):
        # 定义如何生成 StarRocks SQL
        TRANSFORMS = {
            **Doris.Generator.TRANSFORMS,
            # 如果需要重载某些表达式的生成方式
            # exp.SomeExpression: lambda self, e: f"STARROCKS_SPECIFIC_SYNTAX",
        }
```

**注册 Dialect**：

```python
# sqlglot/dialects/__init__.py
from sqlglot.dialects.starrocks import StarRocks

class Dialects(str, Enum):
    # ...
    STARROCKS = "starrocks"

DIALECT_CLASSES = {
    # ...
    Dialects.STARROCKS: StarRocks,
}
```

### 8.4 需要测试的语法

以下语法需要确保 SQLGlot 能正确生成：

1. **CREATE DATABASE**
   ```sql
   CREATE DATABASE IF NOT EXISTS my_db;
   ```

2. **CREATE TABLE with PRIMARY KEY**
   ```sql
   CREATE TABLE t (
       id INT,
       name VARCHAR(100)
   )
   PRIMARY KEY(id)
   DISTRIBUTED BY HASH(id) BUCKETS 10;
   ```

3. **PARTITION BY RANGE**
   ```sql
   PARTITION BY RANGE(dt) ()
   ```

4. **CREATE MATERIALIZED VIEW**
   ```sql
   CREATE MATERIALIZED VIEW mv
   PARTITION BY RANGE(dt) ()
   REFRESH AUTO
   AS SELECT ...;
   ```

5. **DELETE ... USING** (如果需要)
   ```sql
   DELETE FROM t1 USING t2 WHERE t1.id = t2.id;
   ```

---

## 9. 测试策略

### 9.1 单元测试

#### 9.1.1 Adapter 方法测试

```python
# tests/core/engine_adapter/test_starrocks.py
import pytest
from sqlglot import exp
from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter

class TestStarRocksEngineAdapter:
    @pytest.fixture
    def adapter(self, mocker):
        """ 创建 mock 的 StarRocks adapter """
        connection = mocker.Mock()
        return StarRocksEngineAdapter(
            connection_factory=lambda: connection,
            dialect="starrocks"
        )

    def test_create_schema(self, adapter, mocker):
        """ 测试 CREATE DATABASE 语句 """
        adapter.create_schema("test_db")

        # 验证生成的 SQL
        executed_sql = adapter.connection.execute.call_args[0][0]
        assert "CREATE DATABASE" in executed_sql
        assert "test_db" in executed_sql

    def test_create_table_with_primary_key(self, adapter, mocker):
        """ 测试带 PRIMARY KEY 的表创建 """
        adapter.create_table(
            table_name="test_db.users",
            columns_to_types={
                "id": exp.DataType.build("INT"),
                "name": exp.DataType.build("VARCHAR(100)")
            },
            primary_key=("id",)
        )

        executed_sql = adapter.connection.execute.call_args[0][0]
        assert "CREATE TABLE" in executed_sql
        assert "PRIMARY KEY" in executed_sql  # 不是 UNIQUE KEY
        assert "UNIQUE KEY" not in executed_sql

    def test_insert_overwrite_by_time_partition(self, adapter, mocker):
        """ 测试按时间分区回填 """
        query = exp.Select().select("*").from_("source_table")

        adapter.insert_overwrite_by_time_partition(
            table_name="test_db.target_table",
            query_or_df=query,
            start="2024-01-01",
            end="2024-01-02",
            time_column="dt"
        )

        # 验证执行了 DELETE + INSERT
        calls = adapter.connection.execute.call_args_list
        assert len(calls) == 2
        assert "DELETE" in str(calls[0])
        assert "INSERT" in str(calls[1])
```

#### 9.1.2 Expression 构建测试

```python
def test_build_table_properties_exp(self, adapter):
    """ 测试表属性表达式构建 """
    properties_exp = adapter._build_table_properties_exp(
        partitioned_by=[exp.Column("dt")],
        table_properties={
            "distributed_by": {
                "kind": "HASH",
                "columns": [exp.Column("id")],
                "buckets": 10
            }
        }
    )

    # 验证表达式结构
    assert isinstance(properties_exp, exp.Properties)

    # 验证生成的 SQL
    sql = properties_exp.sql(dialect="starrocks")
    assert "PARTITION BY" in sql
    assert "DISTRIBUTED BY HASH" in sql
    assert "BUCKETS 10" in sql
```

### 9.2 集成测试

#### 9.2.1 数据库连接测试

需要真实的 StarRocks 实例（Docker 或本地安装）：

```python
# tests/integration/test_starrocks_integration.py
import pytest
from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.config import ConnectionConfig

@pytest.fixture(scope="module")
def starrocks_adapter():
    """ 创建连接到真实 StarRocks 实例的 adapter """
    config = ConnectionConfig(
        type="starrocks",
        host="localhost",
        port=9030,
        user="root",
        password=""
    )
    return create_engine_adapter(config, dialect="starrocks")

class TestStarRocksIntegration:
    def test_connection(self, starrocks_adapter):
        """ 测试基本连接 """
        result = starrocks_adapter.fetchone("SELECT 1")
        assert result[0] == 1

    def test_create_and_drop_database(self, starrocks_adapter):
        """ 测试创建和删除数据库 """
        db_name = "test_sqlmesh_db"

        starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

        # 验证数据库存在
        result = starrocks_adapter.fetchone(
            f"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '{db_name}'"
        )
        assert result[0] == 1

        # 清理
        starrocks_adapter.drop_schema(db_name, cascade=True)

    def test_full_table_lifecycle(self, starrocks_adapter):
        """ 测试完整的表生命周期 """
        db_name = "test_sqlmesh_db"
        table_name = f"{db_name}.test_table"

        try:
            # 创建数据库
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            # 创建表
            starrocks_adapter.create_table(
                table_name=table_name,
                columns_to_types={
                    "id": exp.DataType.build("INT"),
                    "name": exp.DataType.build("VARCHAR(100)"),
                    "dt": exp.DataType.build("DATE")
                },
                primary_key=("id",),
                partitioned_by=[exp.Column("dt")],
                table_properties={
                    "distributed_by": {
                        "kind": "HASH",
                        "columns": [exp.Column("id")],
                        "buckets": 10
                    }
                }
            )

            # 插入数据
            starrocks_adapter.execute(
                f"INSERT INTO {table_name} VALUES (1, 'Alice', '2024-01-01'), (2, 'Bob', '2024-01-02')"
            )

            # 查询数据
            result = starrocks_adapter.fetchall(f"SELECT * FROM {table_name} ORDER BY id")
            assert len(result) == 2
            assert result[0][1] == "Alice"

            # 删除数据
            starrocks_adapter.delete_from(
                table_name=table_name,
                where=exp.Column("dt").eq("2024-01-01")
            )

            # 验证删除
            result = starrocks_adapter.fetchall(f"SELECT * FROM {table_name}")
            assert len(result) == 1
            assert result[0][1] == "Bob"

        finally:
            # 清理
            starrocks_adapter.drop_schema(db_name, cascade=True)
```

### 9.3 E2E 测试（SQLMesh 完整流程）

#### 9.3.1 创建测试项目

```bash
# 创建测试目录
tests/e2e/starrocks/
├── config.yaml
├── models/
│   ├── staging/
│   │   └── stg_orders.sql
│   └── marts/
│       └── fact_orders.sql
└── tests/
    └── test_fact_orders.yaml
```

**config.yaml**：
```yaml
gateways:
  starrocks_local:
    connection:
      type: starrocks
      host: localhost
      port: 9030
      user: root
      password: ''

default_gateway: starrocks_local
model_defaults:
  dialect: starrocks
```

**models/staging/stg_orders.sql**：
```sql
MODEL (
  name staging.stg_orders,
  kind FULL,
  partitioned_by (order_date),
  physical_properties (
    distributed_by = HASH(order_id),
    buckets = 10
  )
);

SELECT
  order_id,
  customer_id,
  order_amount,
  order_date
FROM raw.orders;
```

#### 9.3.2 运行 E2E 测试

```python
# tests/e2e/test_starrocks_e2e.py
import subprocess
import pytest

def test_sqlmesh_full_workflow():
    """ 测试完整的 SQLMesh 工作流 """
    project_dir = "tests/e2e/starrocks"

    # 1. 初始化
    result = subprocess.run(
        ["sqlmesh", "init", project_dir],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0

    # 2. 创建计划
    result = subprocess.run(
        ["sqlmesh", "plan", "--auto-apply"],
        cwd=project_dir,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    assert "staging.stg_orders" in result.stdout

    # 3. 应用变更
    result = subprocess.run(
        ["sqlmesh", "apply"],
        cwd=project_dir,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0

    # 4. 数据回填
    result = subprocess.run(
        ["sqlmesh", "run"],
        cwd=project_dir,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0

    # 5. 验证数据
    # (这里需要连接数据库查询数据)
```

---

## 10. 实施路线图

### 10.1 阶段划分

#### 阶段 0：准备工作（1-2 天）

- [ ] 阅读 StarRocks 官方文档，重点关注：
  - [ ] CREATE TABLE 语法（PRIMARY KEY, PARTITION BY, DISTRIBUTED BY）
  - [ ] CREATE MATERIALIZED VIEW 语法（REFRESH 策略）
  - [ ] DELETE 语法（是否支持子查询）
  - [ ] 数据类型列表
- [ ] 搭建 StarRocks 开发环境（Docker 或本地安装）
- [ ] 测试基本 SQL 操作（CREATE/INSERT/SELECT/DELETE）
- [ ] 检查 SQLGlot 是否支持 StarRocks dialect

#### 阶段 1：核心功能实现（3-5 天）

- [ ] 创建 `sqlmesh/core/engine_adapter/starrocks.py`
- [ ] 实现 `StarRocksEngineAdapter` 类
  - [ ] 设置 `DIALECT = "starrocks"`
  - [ ] 重载 `_create_table_from_columns()` 支持 PRIMARY KEY
- [ ] 注册 Adapter 到 `__init__.py`
- [ ] 编写基础单元测试
  - [ ] `test_create_schema()`
  - [ ] `test_create_table_with_primary_key()`
  - [ ] `test_insert_and_select()`
- [ ] 集成测试：连接真实 StarRocks，执行基本操作

#### 阶段 2：高级特性（3-5 天）

- [ ] 测试和实现物化视图支持
  - [ ] 比较 StarRocks 与 Doris 的 MV 语法差异
  - [ ] 如果有差异，重载 `_create_materialized_view()`
  - [ ] 测试不同的 REFRESH 策略
- [ ] 测试 DELETE 语法支持
  - [ ] 测试子查询删除：`DELETE ... WHERE ... IN (SELECT ...)`
  - [ ] 如果不支持，保留 Doris 的 `delete_from()` 实现
- [ ] 测试分区表功能
  - [ ] RANGE 分区
  - [ ] 动态分区添加/删除
  - [ ] 按分区回填数据

#### 阶段 3：SQLGlot Dialect 支持（2-3 天）

- [ ] 检查 SQLGlot 现有的 StarRocks 支持
- [ ] 如果不存在，创建 StarRocks dialect
  - [ ] 继承 Doris dialect
  - [ ] 测试所有关键 SQL 语句的生成
  - [ ] 提交 PR 到 SQLGlot 项目
- [ ] 如果已存在，验证兼容性

#### 阶段 4：全面测试（3-5 天）

- [ ] 完善单元测试覆盖率
  - [ ] 目标：>80% 代码覆盖率
  - [ ] 所有重载方法均有测试
- [ ] 集成测试
  - [ ] 完整的表生命周期测试
  - [ ] 物化视图生命周期测试
  - [ ] 分区表操作测试
- [ ] E2E 测试
  - [ ] 创建完整的 SQLMesh 项目
  - [ ] 测试 plan/apply/run 流程
  - [ ] 测试数据回填正确性

#### 阶段 5：文档和发布（2-3 天）

- [ ] 编写用户文档
  - [ ] StarRocks 连接配置
  - [ ] 特性支持说明（分区、物化视图等）
  - [ ] 示例项目
- [ ] 更新 SQLMesh 官方文档
  - [ ] 添加 StarRocks 到支持数据库列表
  - [ ] 添加配置示例
- [ ] 准备 PR
  - [ ] 清理代码，确保符合编码规范
  - [ ] 添加 CHANGELOG
  - [ ] 提交 PR 到 SQLMesh 主仓库

### 10.2 预计工作量

| 阶段 | 时间 | 依赖 | 风险 |
|------|------|------|------|
| 阶段 0 | 1-2 天 | 无 | 低 |
| 阶段 1 | 3-5 天 | 阶段 0 | 低 |
| 阶段 2 | 3-5 天 | 阶段 1 | 中（取决于 StarRocks 特性差异） |
| 阶段 3 | 2-3 天 | 阶段 1 | 中（可能需要等待 SQLGlot PR 合并） |
| 阶段 4 | 3-5 天 | 阶段 1-3 | 低 |
| 阶段 5 | 2-3 天 | 阶段 4 | 低 |
| **总计** | **14-23 天** | - | - |

### 10.3 关键里程碑

1. **第一个表创建成功** (阶段 1 结束)
   - 能够连接 StarRocks
   - 能够创建带 PRIMARY KEY 的表
   - 能够插入和查询数据

2. **物化视图支持** (阶段 2 结束)
   - 能够创建物化视图
   - 能够指定 REFRESH 策略

3. **SQLGlot Dialect 支持** (阶段 3 结束)
   - SQLGlot 能够生成正确的 StarRocks SQL
   - 所有测试用例通过

4. **E2E 测试通过** (阶段 4 结束)
   - 完整的 SQLMesh 项目可以运行
   - 数据回填正确

5. **代码合并** (阶段 5 结束)
   - PR 被 SQLMesh 主仓库接受
   - 文档上线

---

## 附录

### A. 方法实现的判断逻辑与设计原则

#### A.1 方法重载决策树

在实现 StarRocks Adapter 时，对于每个方法，需要判断是否需要重载。以下是决策流程：

```
是否需要重载某个方法？
    │
    ├─> 1. StarRocks 的 SQL 语法与基类默认实现是否不同？
    │   ├─> 是 → 必须重载
    │   │   示例：
    │   │   • create_schema(): 基类使用 "CREATE SCHEMA"，StarRocks 使用 "CREATE DATABASE"
    │   │   • _create_table_from_columns(): Doris 转换 PRIMARY KEY → UNIQUE KEY
    │   │
    │   └─> 否 → 继续判断
    │
    ├─> 2. StarRocks 的行为/特性与基类假设是否不同？
    │   ├─> 是 → 需要重载
    │   │   示例：
    │   │   • delete_from(): StarRocks Primary Key 表支持子查询
    │   │   • _build_table_properties_exp(): 表属性语法可能不同
    │   │
    │   └─> 否 → 继续判断
    │
    ├─> 3. 能否通过类属性配置基类行为？
    │   ├─> 是 → 不需要重载，只设置类属性
    │   │   示例：
    │   │   • INSERT_OVERWRITE_STRATEGY = DELETE_INSERT → 基类自动处理
    │   │   • SUPPORTS_TRANSACTIONS = False → 基类知道不使用事务
    │   │
    │   └─> 否 → 继续判断
    │
    └─> 4. 方法实现是否数据库无关（通用逻辑）？
        └─> 是 → 不需要重载，使用基类实现
            示例：
            • fetchall(): 只返回查询结果
            • execute(): 只执行 SQL
            • columns(): 使用 information_schema（MySQL 兼容）
```

#### A.2 Public 方法 vs Private 方法

**命名约定**：
- **Public 方法**：无下划线前缀，如 `create_schema()`, `create_table()`
- **Private 方法**：单下划线前缀，如 `_create_schema()`, `_create_table_from_columns()`

**重载原则**：

| 方法类型 | 何时重载 | 示例 |
|---------|---------|------|
| Public 方法 | 需要改变**接口行为**或添加**预处理逻辑** | `create_schema()` - 改变 kind 参数 |
| Private 方法 | 需要改变**具体实现细节** | `_create_table_from_columns()` - 改变主键处理 |

**设计模式**：
```python
# Public 方法：模板方法模式
def create_table(self, ...):
    # 统一流程，不重载
    self._validate_inputs(...)
    self._create_table_from_columns(...)  # 调用可重载的私有方法
    self._post_create_hooks(...)

# Private 方法：策略模式
def _create_table_from_columns(self, ...):
    # 具体实现，可重载
    # StarRocks: 直接使用 primary_key
    # Doris: 转换 primary_key → unique_key
```

#### A.3 关键方法分析

##### 方法 #1: `create_schema()` - 语法差异

**为什么需要重载？**

StarRocks 使用 `CREATE DATABASE` 而非 `CREATE SCHEMA`：

```python
# 基类实现
def create_schema(self, schema_name, ...):
    return self._create_schema(
        kind="SCHEMA",  # ❌ StarRocks 不支持
        ...
    )

# StarRocks 重载
def create_schema(self, schema_name, ...):
    return super()._create_schema(
        kind="DATABASE",  # ✅ 使用 DATABASE
        ...
    )
```

**注意**：根据 StarRocks 3.x 文档，可能同时支持 `CREATE SCHEMA` 和 `CREATE DATABASE`，需要验证。

##### 方法 #2: `_create_table_from_columns()` - 核心差异

**为什么这是最重要的方法？**

这涉及 PRIMARY KEY vs UNIQUE KEY 的根本差异：

**执行流程**：
```
SQLMesh Model (定义 primary_key)
    ↓
create_table() [Public，不重载]
    ↓
_create_table_from_columns() [Private，需要重载]
    ↓
_build_table_properties_exp() [Private，构建表属性]
    ↓
SQLGlot Expression → SQL 生成
    ↓
execute() [执行 SQL]
```

**关键差异对比**：

| 数据库 | primary_key 参数处理 | 生成的 SQL |
|--------|---------------------|------------|
| **基类** | 传递给 `_build_table_properties_exp()` | `PRIMARY KEY(id)` |
| **Doris** | 转换为 `table_properties["unique_key"]`，传递 `primary_key=None` | `UNIQUE KEY(id)` |
| **StarRocks** | 直接传递，不转换 | `PRIMARY KEY(id)` |

**实现示例**：

```python
# Doris 实现（参考对比）
def _create_table_from_columns(self, ..., primary_key=None, ...):
    table_properties = kwargs.get("table_properties", {})

    # 🔄 转换：primary_key → unique_key
    if primary_key:
        table_properties["unique_key"] = exp.Tuple(
            expressions=[exp.to_column(col) for col in primary_key]
        )

    kwargs["table_properties"] = table_properties

    # ❌ 阻止基类处理 primary_key
    super()._create_table_from_columns(
        primary_key=None,
        **kwargs
    )

# StarRocks 实现（简单！）
def _create_table_from_columns(self, ..., primary_key=None, ...):
    # ✅ 无需转换，直接传递
    super()._create_table_from_columns(
        primary_key=primary_key,  # 保持原样
        **kwargs
    )
```

##### 方法 #3: `_get_data_objects()` - 无需重载

**为什么不需要重载？**

StarRocks 使用 MySQL 兼容的 `information_schema`，基类实现可以直接使用：

```python
# 基类实现（适用于 MySQL/Doris/StarRocks）
def _get_data_objects(self, schema_name, ...):
    query = f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
    """
    return self.fetchall(query)
```

**验证点**：
- StarRocks 的 `information_schema.tables` 结构与 MySQL 一致
- `table_type` 字段返回 'BASE TABLE' 或 'VIEW'
- 可以正确识别表和视图

##### 方法 #4: `delete_from()` - 条件重载

**复杂性**：StarRocks 不同表类型的 DELETE 能力不同

**准确的能力对比**（基于 StarRocks 官方文档）：

| 表类型 | DELETE USING 支持 | WHERE 子查询支持 | WHERE 简单条件 |
|--------|------------------|-----------------|----------------|
| Primary Key | ✅ 支持 | ✅ **支持** | ✅ 支持 |
| Unique Key | ❌ **不支持** | ❌ 不支持 | ✅ 支持（仅 key 列）|
| Duplicate Key | ❌ 不支持 | ❌ 不支持 | ✅ 支持 |
| Aggregate | ❌ 不支持 | ❌ 不支持 | ✅ 支持（仅 key 列）|

**重要澄清**：

```sql
-- ✅ Primary Key 表支持 USING（多表删除）
DELETE FROM pk_table USING other_table
WHERE pk_table.id = other_table.id;

-- ✅ Primary Key 表支持 WHERE 子查询（完整 DELETE...WHERE 语义）
DELETE FROM pk_table
WHERE id IN (SELECT id FROM blacklist);  -- OK for Primary Key table!

-- ❌ 其他表类型不支持 WHERE 子查询
DELETE FROM unique_table
WHERE id IN (SELECT id FROM blacklist);  -- ERROR!

-- ✅ 所有表类型都支持简单列条件
DELETE FROM any_table
WHERE id = 1 AND status = 'deleted';

-- ⚠️ Unique Key 和 Aggregate 表：WHERE 条件必须是 key 列
DELETE FROM unique_table
WHERE key_col = 1;  -- ✅ OK

DELETE FROM unique_table
WHERE value_col = 1;  -- ❌ ERROR: value 列不能作为条件
```

**设计决策**：

1. **保守方案**（推荐用于 MVP）：
   - 不重载，使用基类实现
   - 只支持简单 WHERE 条件（列名 + 操作符 + 值）
   - ✅ 兼容所有表类型的基础功能
   - ❌ 无法利用 Primary Key 表的子查询能力
   - ❌ 不支持 USING 语法

2. **增强方案**（未来优化）：
   - 重载 `delete_from()`
   - 检测表类型（通过查询元数据）
   - Primary Key 表：
     - ✅ 支持 WHERE 子查询（原生语法）
     - ✅ 支持 USING 语法（多表删除）
   - 其他表：
     - ✅ 只支持简单条件
     - ⚠️ 验证 Unique/Aggregate 表的 WHERE 条件是否为 key 列

3. **Doris 兼容方案**（参考）：
   - Doris 所有表都不支持子查询
   - Doris 通过 DELETE...USING 语法模拟子查询删除
   - StarRocks Primary Key 表更强大，直接支持子查询

**结论**：
- 对于 MVP：不重载，使用基类（简单条件足够）
- 对于增强版：重载以支持 Primary Key 表的完整 DELETE 语义
- StarRocks Primary Key 表的 DELETE 能力**强于** Doris

##### 方法 #5: `insert_overwrite_by_time_partition()` - 策略模式

**为什么不需要重载？**

通过类属性配置行为：

```python
class StarRocksEngineAdapter(EngineAdapter):
    # 配置策略
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT
```

**基类自动处理**：

```python
# 基类实现（伪代码）
def insert_overwrite_by_time_partition(self, table, query, start, end, ...):
    condition = f"time_column >= '{start}' AND time_column < '{end}'"

    if self.INSERT_OVERWRITE_STRATEGY == DELETE_INSERT:
        # 步骤 1：删除旧数据
        self.delete_from(table, where=condition)
        # 步骤 2：插入新数据
        self.insert_append(table, query)
    elif self.INSERT_OVERWRITE_STRATEGY == INSERT_OVERWRITE:
        # 使用原生 INSERT OVERWRITE
        self.execute(f"INSERT OVERWRITE {table} {query} WHERE {condition}")
```

这是**策略模式**的典型应用：通过配置选择行为，无需重载方法。

#### A.4 表达式分区（Expression Partitioning）

**StarRocks 分区类型澄清**：

**重要区分**（根据 StarRocks 文档）：

1. **RANGE 分区** - 范围分区，带 `RANGE` 关键字
   ```sql
   -- 标准 RANGE 分区
   PARTITION BY RANGE(dt) (
       PARTITION p20240101 VALUES [("2024-01-01"), ("2024-01-02")),
       PARTITION p20240102 VALUES [("2024-01-02"), ("2024-01-03"))
   );

   -- RANGE 分区支持函数表达式
   PARTITION BY RANGE(date_trunc('day', order_time)) (
       PARTITION p20240101 VALUES [("2024-01-01"), ("2024-01-02"))
   );

   -- RANGE 分区支持多列
   PARTITION BY RANGE(dt, region) (...);
   ```

2. **LIST 分区** - 列表分区，带 `LIST` 关键字
   ```sql
   -- 标准 LIST 分区（枚举值）
   PARTITION BY LIST(region) (
       PARTITION p_cn VALUES IN ("beijing", "shanghai", "guangzhou"),
       PARTITION p_us VALUES IN ("new_york", "los_angeles"),
       PARTITION p_eu VALUES IN ("london", "paris")
   );

   -- LIST 分区支持多列
   PARTITION BY LIST(country, city) (
       PARTITION p_cn_tier1 VALUES IN (("CN", "beijing"), ("CN", "shanghai")),
       PARTITION p_us_major VALUES IN (("US", "new_york"), ("US", "la"))
   );
   ```

3. **表达式分区** - 自动分区，没有 `RANGE`/`LIST` 关键字
   ```sql
   -- 表达式分区（自动创建分区）
   PARTITION BY (date_trunc('day', order_time));

   -- 多列表达式分区
   PARTITION BY (year, month, day);
   ```

**关键差异对比**：

| 特征 | RANGE 分区 | LIST 分区 | 表达式分区 |
|------|-----------|----------|----------|
| 语法 | `PARTITION BY RANGE(...)` | `PARTITION BY LIST(...)` | `PARTITION BY (...)` |
| 分区定义 | VALUES [(...), (...)) | VALUES IN (...) | 自动创建 |
| 手动定义分区 | ✅ 需要 | ✅ 需要 | ❌ 不需要 |
| 函数支持 | ✅ 少量支持 | ⚠️ 有限支持 | ✅ 支持 |
| 多列支持 | ✅ 支持 | ✅ 支持 | ✅ 支持 |
| 使用场景 | 时间范围、数值范围 | 枚举值、分类 | 自动管理分区 |
| 分区修剪效率 | 高（范围比较） | 高（等值匹配） | 高（自动优化） |

**在 SQLMesh Adapter 中的处理**：

1. **RANGE 分区**：
   - 已在 `_build_partitioned_by_exp()` 中实现
   - 生成 `exp.PartitionByRangeProperty`
   - 需要传递 `partition_expressions` 和 `create_expressions`
   - 边界使用 `[lower, upper)` 语法

2. **LIST 分区**：
   - 需要生成 `exp.PartitionByListProperty`
   - 需要传递 `partition_expressions` 和 `create_expressions`
   - 值使用 `IN (...)` 语法
   - **注意**：与 RANGE 分区的主要区别是值的表示方式

3. **表达式分区**：
   - 可能需要生成 `exp.PartitionedByProperty`
   - 不需要 `create_expressions`（自动分区）
   - 需要验证 SQLGlot 是否已支持

**验证方法**：
```python
from sqlglot import parse_one

# 测试 RANGE 分区with 函数
sql1 = "PARTITION BY RANGE(date_trunc('day', dt)) ()"
parsed1 = parse_one(sql1, dialect="starrocks")
print(parsed1)

# 测试 LIST 分区
sql2 = "PARTITION BY LIST(region) (PARTITION p1 VALUES IN ('beijing', 'shanghai'))"
parsed2 = parse_one(sql2, dialect="starrocks")
print(parsed2)

# 测试表达式分区
sql3 = "PARTITION BY (date_trunc('day', dt))"
parsed3 = parse_one(sql3, dialect="starrocks")
print(parsed3)
```

- 如果不支持，优先在 SQLGlot 中增强（而非 Adapter）
- SQLGlot 负责语法解析，Adapter 只负责传递表达式

#### A.5 SQLGlot Dialect 现状分析

**发现**：SQLGlot 已有 StarRocks Dialect 实现！

**关键信息**：

1. **继承关系**：`StarRocks(MySQL)` - 继承自 MySQL，**不是** Doris
   ```python
   class StarRocks(MySQL):
       # ...
   ```

2. **PRIMARY KEY 支持**：已实现
   ```python
   # Parser: 解析 PRIMARY KEY
   # Generator: 在 POST_SCHEMA 位置生成 PRIMARY KEY
   PROPERTIES_LOCATION = {
       exp.PrimaryKey: exp.Properties.Location.POST_SCHEMA,
   }
   ```

3. **分区支持**：已实现表达式分区
   ```python
   def _parse_partition_by_opt_range(self):
       # 支持 PARTITION BY RANGE(...)
       # 支持动态分区（START/END/EVERY）
   ```

4. **特殊函数**：已映射
   ```python
   FUNCTIONS = {
       "DATE_TRUNC": build_timestamp_trunc,
       "ARRAY_FLATTEN": exp.Flatten.from_arg_list,
       # ...
   }
   ```

**结论**：
- ✅ SQLGlot 的 StarRocks dialect 已经比较完善
- ✅ PRIMARY KEY 生成已支持
- ✅ 表达式分区已支持
- ⚠️ 需要验证是否完全满足 SQLMesh Adapter 的需求
- ⚠️ 可能需要微调但不需要大规模修改

#### A.6 实现优先级总结

**必须实现（MVP）**：

1. ✅ **类属性配置**（5分钟）
   ```python
   DIALECT = "starrocks"
   INSERT_OVERWRITE_STRATEGY = DELETE_INSERT
   SUPPORTS_TRANSACTIONS = False
   # ...
   ```

2. ⚠️ **create_schema()**（可选，需验证）
   - 验证 StarRocks 是否同时支持 `CREATE SCHEMA` 和 `CREATE DATABASE`
   - 如果只支持 DATABASE，则需要重载

3. ✅ **_create_table_from_columns()**（核心，10分钟）
   - 直接调用基类，不做转换
   - 让 SQLGlot 生成 `PRIMARY KEY(...)`

**可选实现（增强）**：

4. ⚠️ **delete_from()**（如果需要子查询支持）
   - 参考 Doris 实现
   - 转换子查询为 `DELETE ... USING ...`

5. ⚠️ **_create_materialized_view()**（如果语法不同）
   - 验证 StarRocks MV 语法与 Doris 的差异
   - 如果相同，无需重载

**无需实现（使用基类）**：

- ❌ `_get_data_objects()` - MySQL 兼容
- ❌ `columns()` - DESCRIBE TABLE
- ❌ `table_exists()` - information_schema
- ❌ `insert_append()` - 标准 INSERT
- ❌ `insert_overwrite_by_time_partition()` - 策略模式

---

### B. 参考资料

1. **StarRocks 官方文档**
   - [SQL Reference](https://docs.starrocks.io/docs/sql-reference/sql-statements/)
   - [Table Design](https://docs.starrocks.io/docs/table_design/)
   - [Materialized View](https://docs.starrocks.io/docs/using_starrocks/Materialized_view/)

2. **SQLMesh 文档**
   - [Engine Adapter](https://sqlmesh.readthedocs.io/en/stable/integrations/overview/)
   - [Custom Adapter](https://sqlmesh.readthedocs.io/en/stable/integrations/custom/)

3. **SQLGlot 文档**
   - [Dialects](https://github.com/tobymao/sqlglot/tree/main/sqlglot/dialects)
   - [Expression API](https://github.com/tobymao/sqlglot#expressions)

### B. 相关代码路径

```
sqlmesh/
├── core/
│   ├── engine_adapter/
│   │   ├── __init__.py          # Adapter 注册
│   │   ├── base.py              # EngineAdapter 基类
│   │   ├── doris.py             # DorisEngineAdapter
│   │   └── starrocks.py         # StarRocksEngineAdapter (待创建)
│   └── config/
│       └── connection.py        # ConnectionConfig
└── tests/
    ├── core/
    │   └── engine_adapter/
    │       └── test_starrocks.py  # 单元测试 (待创建)
    └── integration/
        └── test_starrocks_integration.py  # 集成测试 (待创建)

sqlglot/
└── dialects/
    ├── doris.py
    └── starrocks.py         # StarRocks Dialect (可能需要创建)
```

### C. 快速参考

#### C.1 常用命令

```bash
# 运行单元测试
pytest tests/core/engine_adapter/test_starrocks.py -v

# 运行集成测试
pytest tests/integration/test_starrocks_integration.py -v

# 检查代码覆盖率
pytest --cov=sqlmesh.core.engine_adapter.starrocks tests/

# 启动 StarRocks Docker
docker run -d --name starrocks \
  -p 9030:9030 -p 8030:8030 \
  starrocks/starrocks:latest

# 连接 StarRocks
mysql -h 127.0.0.1 -P 9030 -u root
```

#### C.2 关键语法对比

| 功能 | Doris | StarRocks | 备注 |
|------|-------|-----------|------|
| **主键** | `UNIQUE KEY(id)` | `PRIMARY KEY(id)` | ⭐ 核心差异 |
| **RANGE 分区** | `PARTITION BY RANGE(dt) (...)` | `PARTITION BY RANGE(dt) (...)` | ✅ 相同 |
| **LIST 分区** | `PARTITION BY LIST(col) (...)` | `PARTITION BY LIST(col) (...)` | ✅ 相同 |
| **表达式分区** | 不支持 | `PARTITION BY (expr)` | ⭐ StarRocks 特有 |
| **分区函数** | 支持 | 支持 | ✅ 相同 |
| **分布** | `DISTRIBUTED BY HASH(id) BUCKETS 10` | `DISTRIBUTED BY HASH(id) BUCKETS 10` | ✅ 相同 |
| **物化视图** | `BUILD IMMEDIATE REFRESH AUTO` | `BUILD IMMEDIATE REFRESH AUTO` | ⚠️ 需验证 |
| **DELETE USING** | 不支持 | 仅 Primary Key 表支持 | ⭐ 差异 |
| **DELETE 子查询** | 不支持 | 仅 Primary Key 表支持 | ⭐ 差异！|
| **DELETE 简单条件** | 支持 | 支持 | ✅ 相同 |
| **DATABASE 关键字** | `CREATE DATABASE` | `CREATE DATABASE` / `CREATE SCHEMA`？ | ⚠️ 需验证 |
| **information_schema** | MySQL 兼容 | MySQL 兼容 | ✅ 相同 |

**说明**：
- ⭐ 核心差异：必须在 Adapter 中处理或利用
- ✅ 相同：可以复用 Doris 实现或基类实现
- ⚠️ 需验证：需要在 StarRocks 3.5.3 上测试确认

**重要发现**：
- ⭐⭐⭐ StarRocks Primary Key 表的 DELETE 能力显著强于 Doris
- Primary Key 表支持完整的 `DELETE...WHERE` 语义，包括子查询
- 这是 SQLMesh Adapter 可以利用的重要优化点

---

## 总结

本设计文档提供了在 SQLMesh 中实现 StarRocks 支持的完整指南：

1. **核心理念**：通过深入分析 Doris Engine Adapter，理解了 Engine Adapter 的设计模式和最佳实践

2. **实现策略**：采用继承 DorisEngineAdapter 的方式，最大化代码复用，只重载必须改变的部分

3. **重点关注**：
   - PRIMARY KEY vs UNIQUE KEY 的处理
   - 物化视图的语法差异
   - DELETE 语句的支持情况
   - SQLGlot Dialect 的支持

4. **实施路径**：分 5 个阶段逐步实现，从核心功能到全面测试，预计 2-3 周完成

5. **测试保障**：单元测试 + 集成测试 + E2E 测试，确保功能的正确性和稳定性

通过遵循本文档的指导，可以高质量地完成 StarRocks Engine Adapter 的开发，为 SQLMesh 生态系统增加一个重要的数据库支持。