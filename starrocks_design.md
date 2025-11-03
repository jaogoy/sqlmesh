# StarRocks Engine Adapter 设计文档

> **文档目的**：StarRocks Adapter 实现的完整技术参考
> **适用范围**：SQLMesh 开发者、StarRocks 用户
> **创建日期**：2025-10-21
> **更新日期**：2025-11-03
> **目标版本**：SQLMesh 0.x + StarRocks 3.3+

---

## 目录

1. [概述](#1-概述)
2. [SQLMesh EngineAdapter 工作机制](#2-sqlmesh-engineadapter-工作机制)
3. [SQLMesh 约定与规范](#3-sqlmesh-约定与规范)
4. [StarRocks 核心实现要点](#4-starrocks-核心实现要点)
5. [StarRocks 能力限制与适配](#5-starrocks-能力限制与适配)
6. [View 和 Materialized View 支持](#6-view-和-materialized-view-支持)
7. [实现清单与方法重载](#7-实现清单与方法重载)
8. [Doris 实现参考](#8-doris-实现参考)
9. [SQLGlot StarRocks Dialect](#9-sqlglot-starrocks-dialect)
10. [测试策略](#10-测试策略)

**附录**:

- [A. 核心概念详解](#附录-a-核心概念详解)
- [B. SQLMesh 关键设计模式](#附录-b-sqlmesh-关键设计模式)
- [C. 常见问题与解决方案](#附录-c-常见问题与解决方案)
- [D. 参考资源](#附录-d-参考资源)

---

## 1. 概述

### 1.1 文档目的

本文档为 **StarRocks Engine Adapter** 的完整技术参考，涵盖：

- 实现原理与设计决策
- 核心功能实现要点
- 完整的方法重载清单
- 测试验证策略

### 1.2 StarRocks 简介

StarRocks 是高性能 MPP 分析数据库，实现中的这一些主要要注意的核心点：

- MySQL 协议基本兼容
- MPP 分布式架构
- 原生支持 PRIMARY KEY（StarRocks 3.0+）
- 一些SQL功能并不一定很完备（相对MySQL）

### 1.3 实现策略

**继承关系**：

```plain text
EngineAdapter (SQLMesh 基类)
    ↓
StarRocksEngineAdapter (直接继承，参考 Doris 实现)
```

**关键决策**：

- ✅ 直接继承 `EngineAdapter`（不继承 `MySQLEngineAdapter`）
- ✅ 参考 Doris 实现思路（方法结构、属性处理）
- ✅ 使用 SQLGlot StarRocks dialect（并在合理访问内尽量使用当前 SQLGlot 已经支持的内容）
- ✅ 重载必要方法，复用基类标准功能

### 1.4 关键实现要点速查

本节提供核心实现要点的快速索引，详细内容见后续章节。

| # | 实现要点 | 原因 | 影响范围 |
|---|---------|------|----------|
| 1 | **PRIMARY KEY 原生支持** | 与 Doris 不同，无需转换为 UNIQUE KEY | _create_table_from_columns() |
| 2 | **列重排序** | 所有 key 类型表要求 key 列在前 | PRIMARY/DUPLICATE/UNIQUE/AGGREGATE KEY |
| 3 | **多列表达式分区** | 支持函数和多列组合分区 | partitioned_by 处理 |
| 4 | **PROPERTIES 平铺** | 所有属性平铺到 physical_properties | table_properties 处理 |
| 5 | **DELETE WHERE TRUE → TRUNCATE** | 非 Primary Key 表不支持 WHERE TRUE | delete_from() 方法 |
| 6 | **CREATE INDEX 跳过** | 不支持独立索引，但需 SUPPORTS_INDEXES=True | create_index() 方法 |
| 7 | **VIEW SECURITY 子句** | 使用 SQLGlot 默认 SecurityProperty | _build_table_properties_exp() |
| 8 | **MV REFRESH 子句** | 自定义 StarRocksRefreshProperty，支持字符串 | _create_materialized_view() |
| 9 | **FOR UPDATE 移除** | OLAP 不支持行级锁 | execute() 方法 |

**表类型实现状态**：

- ✅ PRIMARY KEY：完整实现
- ✅ DUPLICATE KEY：完整实现
- ❌ UNIQUE KEY：暂不实现
- ❌ AGGREGATE KEY：暂不实现

**能力限制**：

- `SUPPORTS_TUPLE_IN = False`：不支持 tuple IN 语法
- `FOR UPDATE`：execute() 中自动移除
- `AUTO` 分桶：StarRocks 不支持
- **MV 不支持 OR REPLACE**：需手动 DROP + CREATE
- **INCREMENTAL_BY 要求**：推荐使用 PRIMARY KEY 表

### 1.5 SQLGlot 扩展实现总结

本实现对 SQLGlot 进行了以下扩展：

#### 1.5.1 StarRocksRefreshProperty（已实现）

**位置**：`sqlglot/expressions.py`

**作用**：StarRocks 异步物化视图的 REFRESH 子句

**设计要点**：

- 支持字符串和未来的结构化扩展
- `refresh_moment`：可选的 IMMEDIATE/DEFERRED，字符串或没有引号的名字都可以
- `refresh_scheme`：完整的刷新方案字符串

**Generator 实现**：`starrocks.py:starrocksrefreshproperty_sql()`

**使用示例**：

```python
exp.StarRocksRefreshProperty(
    refresh_moment=exp.Var(this="DEFERRED"),
    refresh_scheme=exp.Literal.string("ASYNC START (...) EVERY (INTERVAL 5 MINUTE)")
)
```

#### 1.5.2 SecurityProperty 配置（复用默认实现）

**位置**：`sqlglot/generator.py`（已存在）

**作用**：VIEW 的 SECURITY 子句（INVOKER/DEFINER/NONE）

**设计要点**：

- 无需自定义 TRANSFORMS，使用默认 lambda 实现
- 必须在 StarRocks Generator 中配置 PROPERTIES_LOCATION
- 使用 `exp.Var` 避免引号问题

**StarRocks 配置**：

```python
PROPERTIES_LOCATION = {
    exp.SecurityProperty: exp.Properties.Location.POST_SCHEMA,
}
```

**使用示例**：

```python
exp.SecurityProperty(this=exp.Var(this="INVOKER"))
```

---

## 2. SQLMesh EngineAdapter 工作机制

### 2.1 EngineAdapter 职责

EngineAdapter 是 SQLMesh 与数据库之间的适配层，负责：

1. **DDL 操作**：CREATE/DROP TABLE, VIEW, MATERIALIZED VIEW
2. **DML 操作**：INSERT, DELETE, UPDATE
3. **查询操作**：SELECT, DESCRIBE, SHOW
4. **元数据管理**：获取表结构、数据对象列表
5. **SQL 生成**：将 SQLGlot AST 转换为特定方言的 SQL

### 2.2 执行流程概览

```Plain text
用户 MODEL 定义
    ↓
SQLMesh 解析 MODEL 语法
    ↓
生成通用 SQLGlot Expression
    ↓
EngineAdapter 处理
    ├─ 提取 table_properties
    ├─ 调用重载方法（如 _create_table_from_columns）
    └─ 生成数据库特定的 SQL
    ↓
执行 SQL 到 StarRocks
```

### 2.3 两级 AST 转换

SQLMesh 采用两级转换模式：

**Level 1**: MODEL 语法 → 通用 Expression

```sql
MODEL (
  partitioned_by (event_date),
  physical_properties (
    primary_key = (id, dt)
  )
)
```

→ 解析为通用的 `partitioned_by`, `table_properties` 字典

**Level 2**: 通用 Expression → 数据库特定 SQL

```python
# EngineAdapter 处理
primary_key = table_properties["primary_key"]
# 生成 StarRocks SQL
CREATE TABLE ... PRIMARY KEY(id, dt)
```

---

## 3. SQLMesh 约定与规范

### 3.1 table_properties 规则

#### 3.1.1 Expression 对象要求

**核心规则**：最终给 SQLGlot 去编译为 SQL 的对象都必须是 Expression 对象。其中，所有 table_properties 的值也必须是 SQLGlot Expression 对象，不能是普通 Python dict 或 list。

**原因**：

- SQLMesh 使用 SQLGlot AST 进行 SQL 生成
- Generator 需要调用 Expression 的 `sql()` 方法
- 普通 dict/list 无法转换为 SQL 语法

❌ **错误示例**：

```python
table_properties = {
    "distributed_by": {"kind": "HASH", "columns": ["id"]},  # ❌ 普通 dict
    "replication_num": 3  # ❌ 普通 int
}
```

✅ **正确示例**：

```python
table_properties = {
    "distributed_by": exp.Tuple(expressions=[...]),  # ✅ Expression
    "replication_num": exp.Literal.number(3)  # ✅ Literal Expression
}
```

#### 3.1.2 StarRocks PROPERTIES 平铺到 physical_properties

**设计决策**：StarRocks 的所有 PROPERTIES 都平铺（flatten）到 `physical_properties` 字典中，与 `distributed_by`, `partitioned_by` 等属性平级。

**示例**：

```python
# MODEL 定义
MODEL (
  physical_properties (
    primary_key = (id, dt),
    distributed_by = (HASH(id), 10),
    replication_num = 1,
    storage_medium = 'SSD',
    storage_cooldown_time = '2025-01-01 00:00:00'
  )
)

# 解析后的 physical_properties
physical_properties = {
    "primary_key": exp.Tuple(...),
    "distributed_by": exp.Tuple(...),
    "replication_num": exp.Literal.number(1),
    "storage_medium": exp.Literal.string("SSD"),
    "storage_cooldown_time": exp.Literal.string("2025-01-01 00:00:00")
}
```

**优势**：

- 统一的属性处理方式，直接利用当前已经支持的能力。

**注意事项**：

- 字面量值（如 `replication_num = 1`）解析为 `exp.Literal`
- 复杂属性（如 `distributed_by`）使用嵌套 `exp.Tuple`

#### 3.1.3 distributed_by 嵌套结构

**设计决策**：`distributed_by` 采用嵌套 Tuple 结构，与 Doris 保持一致，复用 SQLGlot 已有实现。

**原因**：

- SQLGlot 已有 DistributedByProperty
- 避免修改 SQLGlot，降低维护成本

**结构**：

```python
distributed_by = exp.Tuple(expressions=[
    exp.EQ(this=exp.Literal.string("kind"),
           expression=exp.Literal.string("HASH")),      # kind='HASH'
    exp.EQ(this=exp.Literal.string("expressions"),
           expression=exp.Tuple(expressions=[...])),     # columns
    exp.EQ(this=exp.Literal.string("buckets"),
           expression=exp.Literal.number(10))            # buckets=10
])
```

**生成的 SQL**：

```sql
DISTRIBUTED BY HASH(col1, col2) BUCKETS 10
```

### 3.2 partitioned_by 表达式分区支持

**支持的表达式类型**：

- `exp.Column` - 单列分区
- `exp.Anonymous` - 函数调用（`date_trunc`等之外的其他 SQLGlot 中未与定义的函数）
- `exp.Tuple` - 多列/多表达式组合

**示例**：

```python
# 单列
partitioned_by = [exp.Column("event_date")]

# 多列/多函数的表达式分区
partitioned_by = [
    exp.Anonymous(this="date_trunc", expressions=[...]),
    exp.Column("region")
]
```

### 3.3 SUPPORTS_* 标志位

**SUPPORTS_INDEXES**：

- 作用：控制是否生成 PRIMARY KEY 子句
- StarRocks 设置：`SUPPORTS_INDEXES = True`
- 注意：必须同时重载 `create_index()` 跳过独立索引创建（StarRocks当前不支持 CREATE INDEX 语句）

**SUPPORTS_TUPLE_IN**：

- 作用：控制 `(col1, col2) IN ((val1, val2), ...)` 语法支持
- StarRocks 设置：`SUPPORTS_TUPLE_IN = False`（不支持）

### 3.4 primary_key 参数处理

**参数来源**：

1. 方法参数 `primary_key`（优先级最高）
2. `table_properties["primary_key"]`
3. `table_properties["duplicate_key"]`（其他 key 类型）

**处理原则**：

- 参数 primary_key > table_properties
- 检测冲突（不允许同时定义多种 key， Key columns 及其排序要求）
- StarRocks 可直接传递 primary_key 给基类

---

## 4. StarRocks 核心实现要点

### 4.1 Column Reordering for Key Tables

**问题**：StarRocks 所有 key 类型的表（PRIMARY KEY, UNIQUE KEY, DUPLICATE KEY, AGGREGATE KEY）要求 key 列必须是 CREATE TABLE 中的前 N 列，且顺序与 KEY 子句一致。

**示例**：

```sql
-- ❌ 错误：key 列不在前面
CREATE TABLE t (
  customer_id INT,
  order_id BIGINT,    -- PK column 1
  region VARCHAR(50),
  event_date DATE     -- PK column 2
) PRIMARY KEY(order_id, event_date);

-- ✅ 正确：key 列在前
CREATE TABLE t (
  order_id BIGINT,    -- PK column 1 (first)
  event_date DATE,    -- PK column 2 (second)
  customer_id INT,
  region VARCHAR(50)
) PRIMARY KEY(order_id, event_date);
```

**实现思路**：

```python
def _reorder_columns_for_key(target_columns_to_types, key_columns):
    # 1. 验证 key 列存在
    # 2. 构建新字典：key 列在前（按 key 顺序）
    # 3. 添加剩余列（保持原顺序）
    # 4. 返回重排序后的字典
```

**影响范围**：

- PRIMARY KEY 表：必须
- DUPLICATE KEY 表：必须
- UNIQUE KEY 表：必须（虽暂不实现）
- AGGREGATE KEY 表：必须（但暂不实现）

**代码位置**：`starrocks.py:_reorder_columns_for_key()`, `_create_table_from_columns()`

---

### 4.2 PRIMARY KEY 支持

**StarRocks 实现要点**：

```python
def _create_table_from_columns(...):
    # 1. 提取 key（primary_key 或 duplicate_key）
    key_type, key_columns = self._extract_and_validate_key_columns(...)

    # 2. PRIMARY KEY 类型直接使用
    if key_type == "primary_key":
        primary_key = key_columns  # ✅ StarRocks 支持
    else:
        primary_key = None  # 其他 key 类型不生成 PRIMARY KEY

    # 3. 传递给基类
    super()._create_table_from_columns(primary_key=primary_key, ...)
```

---

### 4.3 DELETE 操作

#### 4.3.1 WHERE TRUE → TRUNCATE TABLE

**问题**：非 Primary Key 表 不支持 `DELETE FROM table WHERE TRUE`，并且 `TRUNCATE TABLE` 效率更高。

**解决方案**：

```python
def delete_from(table_name, where):
    if not where or where == exp.true():
        self.execute(f"TRUNCATE TABLE {table_name}")
        return
    super().delete_from(table_name, where)
```

#### 4.3.2 PRIMARY KEY vs DUPLICATE KEY 的 WHERE 限制

**重要区别**：

| 表类型 | WHERE 支持 | 示例 |
|--------|-----------|------|
| PRIMARY KEY | 复杂条件 | 子查询、BETWEEN、CAST 等 |
| DUPLICATE KEY | 简单条件 | 只支持列=值、AND、OR |

**实现考虑**：

- 当前实现：保守策略，统一简化 WHERE 条件
- 原因：运行时难以判断表类型
- 影响：PRIMARY KEY 表失去部分灵活性，但保证兼容性

**代码位置**：`starrocks.py:delete_from()`

---

### 4.4 CREATE INDEX 处理

**问题**：StarRocks 不支持独立的 `CREATE INDEX` 语句

**为什么不能设置 SUPPORTS_INDEXES=False**：

```python
# ❌ 错误做法
class StarRocksEngineAdapter:
    SUPPORTS_INDEXES = False  # 会导致 PRIMARY KEY 子句被抑制！
```

**正确做法**：

```python
# ✅ 正确做法
class StarRocksEngineAdapter:
    SUPPORTS_INDEXES = True  # 保持 True 以支持 PRIMARY KEY

    def create_index(self, ...):
        logger.info("Skipping CREATE INDEX - StarRocks uses PRIMARY KEY")
        return  # 优雅地跳过，不执行
```

**代码位置**：`starrocks.py:create_index()`

---

### 4.5 Multi-Column Expression Partitioning

**StarRocks 特性**：支持多列和函数表达式分区

**示例**：

```sql
MODEL (
  partitioned_by (date_trunc('day', event_time), region)
)
```

**实现要点**：

- 复用 `PartitionedByProperty`（不创建新类型）
- expressions 列表可包含 `exp.Column`, `exp.Anonymous`, `exp.Tuple`
- 基类 `_build_partitioned_by_exp()` 已支持，通常无需重载

**代码位置**：Model DDL 解析，`_build_partitioned_by_exp()`

---

## 5. StarRocks 能力限制与适配

### 5.1 SUPPORTS_TUPLE_IN = False

**限制**：StarRocks 不支持 Tuple IN 语法

**示例**：

```sql
-- ❌ StarRocks 不支持
SELECT * FROM t
WHERE (col1, col2) IN ((val1, val2), (val3, val4));

-- ✅ 需要改写为
SELECT * FROM t
WHERE (col1=val1 AND col2=val2) OR (col1=val3 AND col2=val4);
```

**Adapter 设置**：

```python
class StarRocksEngineAdapter(EngineAdapter):
    SUPPORTS_TUPLE_IN = False
```

---

### 5.2 不支持 SELECT ... FOR UPDATE

**限制**：StarRocks 不支持行级锁（Row-Level Locking）

**原因**：

- StarRocks 是 OLAP 数据库，为分析查询优化
- OLAP 场景不需要事务锁
- 行级锁会影响并发查询性能

**问题示例**：

```sql
-- ❌ StarRocks 不支持
SELECT * FROM users WHERE id = 100 FOR UPDATE;

-- ✅ 移除 FOR UPDATE
SELECT * FROM users WHERE id = 100;
```

**实现**：在 `execute()` 中自动移除 FOR UPDATE

```python
def execute(
    self,
    expressions: t.Union[str, exp.Expression, t.List[exp.Expression]],
    **kwargs
):
    # 规范化为列表
    if not isinstance(expressions, list):
        expressions = [expressions]

    # 移除 FOR UPDATE
    processed = []
    for e in expressions:
        if isinstance(e, exp.Select) and e.args.get("locks"):
            e = e.copy()
            e.set("locks", None)  # 移除 locks 子句
            logger.warning(
                f"Removed FOR UPDATE clause - StarRocks does not support row locks"
            )
        processed.append(e)

    super().execute(processed, **kwargs)
```

**代码位置**：`starrocks.py:execute()`

**使用注意事项**：

1. **不能用于需要悲观锁的场景**：
   - ❌ 金融交易系统（需要悲观锁）
   - ❌ 库存管理系统（需要锁定库存）
   - ✅ 数据分析、报表生成
   - ✅ 日志查询、数据探索

2. **替代方案**：
   - 如需要事务，使用 OLTP 数据库（MySQL, PostgreSQL）
   - StarRocks 适用于读密集型分析场景

3. **SQLMesh 中的影响**：
   - SQLMesh 内部可能使用 FOR UPDATE 锁定 state 表
   - StarRocks adapter 自动移除，但可能影响并发安全性
   - **建议**：state 表使用单独的 OLTP 数据库（MySQL/PostgreSQL）

**限制总结**：

| 限制 | 影响 | 解决方案 |
|------|------|----------|
| 不支持 FOR UPDATE | 无法使用悲观锁 | 使用 OLTP 数据库 |
| OLAP 场景 | 适用分析，不适用事务 | 区分 OLAP/OLTP 场景 |
| 并发安全 | state 表可能冲突 | state 表用 MySQL/PostgreSQL |

---

### 5.3 支持的表类型

| 表类型 | 实现状态 | 说明 |
|--------|----------|------|
| PRIMARY KEY | ✅ 完整实现 | 核心支持 |
| DUPLICATE KEY | ✅ 完整实现 | 核心支持 |
| UNIQUE KEY | ⚠️ 代码差不多 | 未测试，暂不支持使用 |
| AGGREGATE KEY | ❌ 暂不实现 | 需修改 model 支持 value 聚合类型 |

**注意**：

- 列重排序逻辑已支持所有 key 类型
- `_extract_and_validate_key_columns()` 可处理 unique_key/aggregate_key
- 但未针对 AGGREGATE KEY 的特殊需求（SUM/MAX/MIN 等聚合函数）做适配

---

### 5.4 INCREMENTAL_BY 策略与表类型要求

**问题背景**：

SQLMesh 的 INCREMENTAL_BY 策略通过 DELETE 条件化删除旧数据，然后 INSERT 新数据实现增量更新。不同的 StarRocks 表类型对 DELETE 的支持不同。

**表类型与 DELETE 能力**：

| 表类型 | DELETE WHERE 支持 | 复杂条件支持 | INCREMENTAL_BY 推荐 |
|--------|-------------------|-------------|---------------------|
| PRIMARY KEY | ✅ 完整支持 | ✅ 支持子查询、CAST、BETWEEN 等 | ✅ **强烈推荐** |
| DUPLICATE KEY | ⚠️ 有限支持 | ⚠️ 只支持简单条件（列=值, AND, OR） | ⚠️ 简单场景可用 |
| UNIQUE KEY | ⚠️ 有限支持 | ⚠️ 只支持简单条件（列=值, AND, OR） | ❌ 暂不支持 |
| AGGREGATE KEY |⚠️ 有限支持 | ⚠️ 只支持简单条件（列=值, AND, OR） | ❌ 暂不支持 |

**实现策略**：

```python
# 不强制要求 PRIMARY KEY，但发出警告
def _validate_incremental_by(self, table_properties):
    """Validate INCREMENTAL_BY strategy against table type."""
    # 检测是否使用了 INCREMENTAL_BY
    if not self._uses_incremental_by():
        return

    # 提取表类型
    key_type = self._get_table_key_type(table_properties)

    # PRIMARY KEY: 最佳选择，无需警告
    if key_type == "primary_key":
        return

    # 其他类型: 发出警告
    logger.warning(
        f"Using INCREMENTAL_BY with {key_type.upper()} table. "
        "PRIMARY KEY tables are recommended for better DELETE support. "
        "If you encounter DELETE WHERE errors, consider switching to PRIMARY KEY."
    )
```

**设计原则**：

- ✅ **不强制报错**：允许 DUPLICATE KEY 表使用 INCREMENTAL_BY
- ⚠️ **发出警告**：提醒用户可能的风险
- ✅ **推荐 PRIMARY KEY**：明确最佳实践
- ✅ **运行时检测**：如果 DELETE 失败，提示切换到 PRIMARY KEY

**用户指导**：

```sql
# 推荐：使用 PRIMARY KEY
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE,
  time_column event_date,
  physical_properties (
    primary_key = (id, event_date)  # ← 推荐
  )
);

# 可用但有限制：DUPLICATE KEY
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE,
  time_column event_date,
  physical_properties (
    duplicate_key = (event_date, id)  # ⚠️ 简单条件可用
  )
);
```

**代码位置**：

- 警告逻辑：`starrocks.py:_create_table_from_columns()` 或专门的验证方法
- 参考：Doris adapter 类似逻辑

---

### 5.5 Materialized View 不支持 OR REPLACE

**限制说明**：

StarRocks 的 Materialized View 不支持 `CREATE OR REPLACE MATERIALIZED VIEW` 语法，但普通 VIEW 支持。

**对比**：

| 对象类型 | OR REPLACE 支持 | 实现方式 |
|---------|----------------|---------|
| Regular VIEW | ✅ 支持 | `CREATE OR REPLACE VIEW` |
| Materialized VIEW | ❌ 不支持 | 需先 `DROP` 再 `CREATE` |

**实现方案**：

```python
def _create_materialized_view(
    self,
    view_name: TableName,
    query_or_obj: t.Union[exp.Query, QueryOrDF],
    table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
    replace: bool = False,
    **kwargs
):
    """Create materialized view with manual replace handling."""

    # StarRocks MV 不支持 OR REPLACE，需手动处理
    if replace:
        logger.info(
            f"StarRocks does not support OR REPLACE for materialized views. "
            f"Dropping {view_name} before creation."
        )
        try:
            self.drop_view(view_name, materialized=True, cascade=False)
        except Exception as e:
            # 如果视图不存在，忽略错误
            logger.debug(f"Failed to drop {view_name}: {e}")

    # 调用基类创建（不传递 replace=True）
    super()._create_materialized_view(
        view_name=view_name,
        query_or_obj=query_or_obj,
        table_properties=table_properties,
        replace=False,  # ← 强制为 False
        **kwargs
    )
```

**关键要点**：

1. ✅ **检测 replace=True**：如果用户请求替换
2. ✅ **手动 DROP**：显式删除旧视图
3. ✅ **忽略错误**：如果视图不存在，继续执行（因为不存在也是正常的）
4. ✅ **强制 replace=False**：调用基类时不传递 replace
5. ✅ **日志记录**：告知用户执行了手动替换

**用户体验**：

```python
# 用户代码保持不变
sqlmesh.create_external_models([model], replace=True)

# Adapter 自动处理：
# 1. DROP MATERIALIZED VIEW IF EXISTS my_mv;
# 2. CREATE MATERIALIZED VIEW my_mv ...;
```

**限制说明**：

- ⚠️ **非原子操作**：DROP 和 CREATE 之间有间隙
- ⚠️ **并发风险**：多个进程同时替换可能冲突
- ✅ **兼容性好**：用户无需修改代码

**代码位置**：`starrocks.py:_create_materialized_view()`

---

### 5.6 其他限制

- ❌ 不支持事务（OLAP 数据库特性）
- ❌ 不支持独立 CREATE INDEX
- ⚠️ DELETE WHERE 条件限制（DUPLICATE KEY 表）

---

## 6. View 和 Materialized View 支持

### 6.1 Regular View

**StarRocks 语法特点**：

- 支持 `OR REPLACE`
- 支持列级和视图级 COMMENT
- 支持 SECURITY 子句（v3.4.1+）：

**语法**：

```sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [database.]view_name (
    column_name [COMMENT 'column comment']
    [, ...]
)
[COMMENT 'view comment']
[SECURITY {NONE | INVOKER | DEFINER}]
AS query_statement
```

**与 Doris 差异分析**：

| 特性 | StarRocks | Doris | 需要实现 |
|------|-----------|-------|---------|
| OR REPLACE | ✅ | ✅ | 否（基类支持） |
| COMMENT | ✅ 列级+视图级 | ✅ 列级+视图级 | 否（基类支持） |
| SECURITY 子句 | ✅ v3.4.1+ | ❌ | ✅ 是（已实现） |
| 基本语法 | 相同 | 相同 | 否 |

#### 6.1.1 SECURITY 子句实现方案

**SQLGlot 默认支持**：

SQLGlot Generator 已内置 `SecurityProperty` 的 TRANSFORMS：

```python
# sqlglot/generator.py (Line 190)
exp.SecurityProperty: lambda self, e: f"SECURITY {self.sql(e, 'this')}",
```

**StarRocks Generator 配置**：

```python
# starrocks.py
class Generator(MySQL.Generator):
    PROPERTIES_LOCATION = {
        **MySQL.Generator.PROPERTIES_LOCATION,
        exp.SecurityProperty: exp.Properties.Location.POST_SCHEMA,  # ← 必须添加
    }
    # TRANSFORMS 继承基类，无需重写
```

**MODEL 定义示例**：

```sql
MODEL (
  kind VIEW,
  physical_properties (
    security = 'INVOKER'  # 或 DEFINER, NONE （无引号）
  )
);
```

**Adapter 处理**：

```python
def _build_table_properties_exp(self, ..., view_properties=None, **kwargs):
    properties = []

    security = view_properties.pop("security", None) if view_properties else None
    if security:
        # 统一转换为 Var（Column 也支持，但推荐 Var）
        if isinstance(security, (exp.Column, exp.Var)):
            value = security.name if isinstance(security, exp.Column) else security.this
        elif isinstance(security, exp.Literal):
            value = security.this
        else:
            value = str(security)

        # 验证并创建
        value_upper = value.upper()
        if value_upper not in ("INVOKER", "DEFINER", "NONE"):
            raise SQLMeshError(f"Invalid SECURITY value: {value}")

        properties.append(exp.SecurityProperty(this=exp.Var(this=value_upper)))
```

**生成的 SQL**：

```sql
CREATE VIEW my_view
SECURITY INVOKER
AS SELECT * FROM t
```

**关键实现要点**：

| 表达式类型 | 生成结果 | 是否正确 |
|-----------|---------|----------|
| `exp.Column(this='INVOKER')` | `SECURITY INVOKER` | ✅ 正确 |
| `exp.Var(this='INVOKER')` | `SECURITY INVOKER` | ✅ 正确（推荐） |
| `exp.Literal.string('INVOKER')` | `SECURITY 'INVOKER'` | ❌ 有引号 |

**实现总结**：

- ✅ **无需自定义 TRANSFORMS**：使用默认实现
- ✅ **必须设置 PROPERTIES_LOCATION**：确保 SECURITY 出现在 POST_SCHEMA 位置
- ✅ **使用 exp.Var**：避免 Literal.string 产生引号
- ✅ **验证有效值**：INVOKER, DEFINER, NONE

---

### 6.2 Materialized View

**StarRocks MV 类型**：

1. **同步 MV (Synchronous)**：类似 Rollup，与基表同步更新。**先不支持**。
2. **异步 MV (Asynchronous)**：独立刷新，支持多表 JOIN

#### 6.2.1 同步 MV 语法

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]mv_name
[COMMENT "comment"]
[PROPERTIES ("key"="value", ...)]
AS query_statement
```

#### 6.2.2 异步 MV 语法

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] mv_name
[COMMENT "comment"]
[REFRESH
    [IMMEDIATE | DEFERRED]
    [ASYNC | ASYNC [START(datetime)] EVERY(INTERVAL interval_value) | MANUAL]]
[PARTITION BY ...]
[DISTRIBUTED BY ...]
[ORDER BY (columns)]
[PROPERTIES ("key"="value", ...)]
AS query_statement
```

**关键参数**：

- `REFRESH [IMMEDIATE | DEFERRED]`：首次刷新时机。
- `REFRESH ASYNC`：异步自动刷新（后台定时）
- `REFRESH ASYNC START(...)`：异步刷新，指定首次刷新时间
- `REFRESH MANUAL`：手动刷新（需要显式触发）

#### 6.2.3 与 Doris MV 差异

| 特性 | Doris | StarRocks | 需要实现 |
|------|-----------|-------|---------|
| 异步 MV | ✅ 完整支持 | ❌ 不支持 | 是 |
| REFRESH 子句 | ✅ ASYNC/MANUAL | ❌ | 是 |
| 多表 JOIN | ✅ （异步 MV） | ❌ | 是 |
| PARTITION BY | ✅ | ❌ | 是 |
| DISTRIBUTED BY | ✅ | ✅ | 否（已支持）|

#### 6.2.4 实现要点

**1. REFRESH 参数处理 - 最终方案**：

**设计决策**：使用 SQLGlot 自定义 `StarRocksRefreshProperty` 类，支持字符串和未来的结构化扩展。

**SQLGlot Expression 定义**：

```python
# sqlglot/expressions.py
class StarRocksRefreshProperty(Property):
    """StarRocks REFRESH clause for materialized views.

    Supports flexible refresh_scheme:
    - String: "ASYNC START ('...') EVERY (INTERVAL 5 MINUTE)"
    - Tuple: Can be extended for structured parsing in the future
    """
    arg_types = {
        "refresh_moment": False,       # IMMEDIATE or DEFERRED (optional)
        "refresh_scheme": True,  # Full refresh scheme (string or structured)
    }
```

**StarRocks Generator 配置**：

```python
# starrocks.py
class Generator(MySQL.Generator):
    PROPERTIES_LOCATION = {
        **MySQL.Generator.PROPERTIES_LOCATION,
        exp.StarRocksRefreshProperty: exp.Properties.Location.POST_SCHEMA,
    }

    def starrocksrefreshproperty_sql(self, expression: exp.StarRocksRefreshProperty) -> str:
        """Generate StarRocks REFRESH clause for materialized views."""
        parts = ["REFRESH"]

        # Add refresh_moment (IMMEDIATE/DEFERRED) if provided
        refresh_moment = expression.args.get("refresh_moment")
        if refresh_moment:
            parts.append(self.sql(refresh_moment))

        # Add refresh_scheme (can be string, Tuple, or other Expression)
        refresh_scheme = expression.args.get("refresh_scheme")
        if refresh_scheme:
            if isinstance(refresh_scheme, exp.Literal):
                # String literal - use the raw string value
                parts.append(refresh_scheme.this)
            elif isinstance(refresh_scheme, exp.Tuple):
                # Future extension: structured parsing
                scheme_parts = [self.sql(e) for e in refresh_scheme.expressions]
                parts.append(" ".join(scheme_parts))
            else:
                # Other Expression types
                parts.append(self.sql(refresh_scheme))

        return " ".join(parts)
```

**MODEL 定义示例**：

```sql
MODEL (
  kind MV,
  physical_properties (
    refresh_moment = 'DEFERRED',  # IMMEDIATE 或 DEFERRED （可以没有引号）
    refresh_scheme = 'ASYNC START ("2023-09-12 16:30:25") EVERY (INTERVAL 5 MINUTE)'
  )
);
```

**Adapter 处理**：

```python
def _build_table_properties_exp(self, ..., view_properties=None, **kwargs):
    properties = []

    refresh_moment = view_properties.pop("refresh_moment", None) if view_properties else None
    refresh_scheme = view_properties.pop("refresh_scheme", None) if view_properties else None

    if refresh_scheme:
        # 直接使用字符串，无需解析！
        if isinstance(refresh_scheme, exp.Literal):
            scheme_str = refresh_scheme.this
        elif isinstance(refresh_scheme, str):
            scheme_str = refresh_scheme
        else:
            scheme_str = str(refresh_scheme)

        properties.append(
            exp.StarRocksRefreshProperty(
                refresh_moment=exp.Var(this=refresh_moment) if refresh_moment else None,
                refresh_scheme=exp.Literal.string(scheme_str)
            )
        )
```

**生成的 SQL**：

```sql
CREATE MATERIALIZED VIEW my_mv
REFRESH DEFERRED ASYNC START ("2023-09-12 16:30:25") EVERY (INTERVAL 5 MINUTE)
AS SELECT ...
```

**支持的 refresh_scheme 格式**（3种）：

| 刷新模式 | refresh_scheme 字符串示例 |
|---------|---------------------------|
| 异步定时刷新 | `"ASYNC START ('2023-09-12 16:30:25') EVERY (INTERVAL 5 MINUTE)"` |
| 异步自动刷新 | `"ASYNC"` |
| 手动刷新 | `"MANUAL"` |

**属性说明**：

| 属性名 | 说明 | 可选值 | 示例 |
|--------|------|--------|------|
| `refresh_moment` | 刷新时机 | IMMEDIATE / DEFERRED | `"DEFERRED"` |
| `refresh_scheme` | 完整刷新方案 | ASYNC / MANUAL | `"ASYNC START (...) EVERY (...)"` |

**注意**：

- StarRocks **不支持** `REFRESH ON COMMIT`
- 只有 ASYNC、ASYNC START、MANUAL 三种刷新模式
- refresh_moment 和 refresh_scheme 是 StarRocks 的实际属性名

**2. 可能需要重载的方法**：

```python
def _create_materialized_view(self, view_name, query_or_obj, table_properties, ...):
    # 1. 提取 refresh_moment, refresh_scheme
    # 2. 生成 REFRESH 子句
    # 3. 处理 PARTITION BY（MV 专用）
    # 4. 调用基类或自行生成 DDL
```

**实现状态**：

- ✅ 同步 MV：基类可能支持（待验证）
- ⏳ 异步 MV：需要实现 REFRESH 子句和参数处理

---

## 7. 实现清单与方法重载

本章节是实现 StarRocks Adapter 的完整参考指南。

### 7.1 必须重载的方法

#### 7.1.1 `_create_table_from_columns()`

**作用**：CREATE TABLE 的核心方法

**为什么必须重载**：

1. StarRocks 原生支持 PRIMARY KEY
2. StarRocks 要求 key 列在前（所有 key 类型）

**实现要点**：

```python
def _create_table_from_columns(
    self,
    table_name: TableName,
    target_columns_to_types: t.Dict[str, exp.DataType],
    primary_key: t.Optional[t.Tuple[str, ...]] = None,
    **kwargs
):
    # 1. 获取 table_properties
    table_properties = kwargs.setdefault("table_properties", {})

    # 2. 提取 key（primary_key 或 duplicate_key）
    key_type, key_columns = self._extract_and_validate_key_columns(
        table_properties, primary_key
    )

    # 3. 重排序列（key 列在前）
    if key_columns:
        target_columns_to_types = self._reorder_columns_for_key(
            target_columns_to_types, key_columns, key_type
        )

    # 4. 根据 key 类型处理
    if key_type == "primary_key":
        primary_key = key_columns  # StarRocks 支持
    elif key_type == "duplicate_key":
        # DUPLICATE KEY 通过 table_properties 传递
        primary_key = None

    # 5. 调用基类
    super()._create_table_from_columns(
        table_name=table_name,
        target_columns_to_types=target_columns_to_types,
        primary_key=primary_key,  # StarRocks 可传递
        **kwargs
    )
```

**参考**：Doris 的结构，但不转换 primary_key → unique_key

---

#### 7.1.2 `delete_from()`

**作用**：DELETE 操作

**为什么必须重载**：

- StarRocks 不支持 `WHERE TRUE`
- 需要转换为 `TRUNCATE TABLE`

**实现要点**：

```python
def delete_from(
    self,
    table_name: TableName,
    where: t.Optional[exp.Expression] = None
):
    # WHERE TRUE → TRUNCATE TABLE
    if not where or where == exp.true():
        table_expr = exp.to_table(table_name)
        sql = f"TRUNCATE TABLE {table_expr.sql(dialect=self.dialect, identify=True)}"
        self.execute(sql)
        return

    # 其他条件使用基类
    super().delete_from(table_name, where)
```

**注意**：PRIMARY KEY vs DUPLICATE KEY 的 WHERE 限制在运行时难以判断，采用保守策略。

**参考**：与 Doris 实现相同

---

#### 7.1.3 `create_index()`

**作用**：跳过 CREATE INDEX 执行

**为什么必须重载**：

- StarRocks 不支持独立 CREATE INDEX
- 必须保持 `SUPPORTS_INDEXES=True`（支持 PRIMARY KEY）

**实现要点**：

```python
def create_index(
    self,
    table_name: TableName,
    index_name: str,
    columns: t.Tuple[str, ...],
    exists: bool = True
):
    logger.info(
        f"Skipping CREATE INDEX {index_name} on {table_name} - "
        "StarRocks uses PRIMARY KEY for indexing"
    )
    return  # 不执行任何操作
```

**参考**：不参考 Doris（Doris 有bug，会尝试执行）

---

#### 7.1.4 `execute()`

**作用**：移除 SELECT ... FOR UPDATE

**为什么必须重载**：

- StarRocks 是 OLAP，不支持 FOR UPDATE

**实现要点**：

```python
def execute(
    self,
    expressions: t.Union[str, exp.Expression, t.List[exp.Expression]],
    **kwargs
):
    # 规范化为列表
    if not isinstance(expressions, list):
        expressions = [expressions]

    # 移除 FOR UPDATE
    processed = []
    for e in expressions:
        if isinstance(e, exp.Select) and e.args.get("locks"):
            e = e.copy()
            e.set("locks", None)
        processed.append(e)

    super().execute(processed, **kwargs)
```

---

### 7.2 辅助方法（必须实现）

#### 7.2.1 `_extract_and_validate_key_columns()`

**作用**：从 table_properties 提取并验证 key 定义

**返回**：`(key_type: str, key_columns: Tuple[str, ...])`

**处理逻辑**：

```python
def _extract_and_validate_key_columns(
    self,
    table_properties: t.Dict[str, exp.Expression],
    primary_key: t.Optional[t.Tuple[str, ...]]
):
    # 1. 检测冲突（不允许同时定义多种 key）
    # 2. 优先级：primary_key 参数 > table_properties
    # 3. 提取 primary_key / duplicate_key / unique_key
    # 4. 返回 (key_type, key_columns)
```

**支持的 key 类型**：

- `primary_key`：PRIMARY KEY 表
- `duplicate_key`：DUPLICATE KEY 表
- `unique_key`：UNIQUE KEY 表（代码支持，未充分测试）
- `aggregate_key`：AGGREGATE KEY 表（代码支持，但暂不实现）

---

#### 7.2.2 `_expr_to_column_tuple()`

**作用**：统一转换 key 表达式为列名元组

**支持格式**：

- `exp.Tuple(expressions=[exp.Column(...), ...])`
- `[exp.Column(...), ...]`（list）
- `exp.Column(...)`（单列）
- `"col_name"`（字符串）

**返回**：`Tuple[str, ...]`

---

#### 7.2.3 `_reorder_columns_for_key()`

**作用**：重排序列，key 列在前

**实现逻辑**：

```python
def _reorder_columns_for_key(
    self,
    target_columns_to_types: t.Dict[str, exp.DataType],
    key_columns: t.Tuple[str, ...],
    key_type: str
):
    # 1. 验证 key 列存在
    missing = set(key_columns) - set(target_columns_to_types.keys())
    if missing:
        raise SQLMeshError(f"{key_type} columns {missing} not found")

    # 2. 构建新字典：key 列在前 + 其他列
    reordered = {}
    for key_col in key_columns:
        reordered[key_col] = target_columns_to_types[key_col]
    for col_name, col_type in target_columns_to_types.items():
        if col_name not in key_columns:
            reordered[col_name] = col_type

    return reordered
```

**适用范围**：PRIMARY KEY, DUPLICATE KEY, UNIQUE KEY（代码支持）

---

### 7.3 可能需要重载的方法

#### 7.3.1 `_build_partitioned_by_exp()`

**条件**：如需支持多列表达式分区

**当前状态**：基类可能已支持，待验证

**实现要点**：

- 处理 `exp.Anonymous`（函数调用）
- 处理多列组合
- 参考 Doris 实现

---

#### 7.3.2 `_create_view()`

**条件**：如需支持 SECURITY 子句

**当前状态**：基本语法兼容，暂不需要

**未来扩展**：

```python
def _create_view(...):
    # 从 table_properties 提取 security 参数
    # 生成 SECURITY {NONE | INVOKER} 子句
```

---

#### 7.3.3 `_create_materialized_view()`

**条件**：如需支持异步 MV 的 REFRESH 子句

**实现要点**：

```python
def _create_materialized_view(...):
    # 1. 提取 refresh_mode, refresh_interval
    # 2. 生成 REFRESH 子句
    # 3. 处理 PARTITION BY（MV 专用）
    # 4. 调用基类或自行生成 DDL
```

**优先级**：中（取决于 MV 需求）

---

### 7.4 不需要重载的方法

以下方法基类实现已足够，**无需重载**：

| 方法 | 原因 |
|------|------|
| `columns()` | 标准 DESCRIBE TABLE 查询 |
| `table_exists()` | 标准 information_schema 查询 |
| `insert_append()` | 标准 INSERT INTO ... SELECT |
| `create_schema()` | 标准 CREATE DATABASE，StarRocks支持 CREATE SCHEMA |
| `drop_schema()` | 标准 DROP DATABASE |
| `fetchall()` / `fetchone()` | 标准查询执行 |
| `_build_table_properties_exp()` | 基类处理通用属性 |

**重要原则**：

- ✅ 优先使用基类实现
- ✅ 只重载有 StarRocks 特殊需求的方法

---

### 7.5 方法重载清单（快速参考）

| 方法 | 状态 | 原因 |
|------|------|------|
| **必须重载** | | |
| `_create_table_from_columns()` | ✅ 必须 | 列重排序 + PRIMARY KEY |
| `delete_from()` | ✅ 必须 | WHERE TRUE → TRUNCATE |
| `create_index()` | ✅ 必须 | 跳过执行 |
| `execute()` | ✅ 必须 | 移除 FOR UPDATE |
| **辅助方法** | | |
| `_extract_and_validate_key_columns()` | ✅ 必须 | 提取 key 定义 |
| `_expr_to_column_tuple()` | ✅ 必须 | 统一格式转换 |
| `_reorder_columns_for_key()` | ✅ 必须 | 列重排序逻辑 |
| **可选重载** | | |
| `_build_partitioned_by_exp()` | ⏳ 可选 | 表达式分区（可能已支持） |
| `_create_view()` | ⏳ 可选 | SECURITY 子句（未来） |
| `_create_materialized_view()` | ⏳ 可选 | 异步 MV REFRESH 子句 |
| **不需要重载** | | |
| `columns()` | ❌ 否 | 基类足够 |
| `table_exists()` | ❌ 否 | 基类足够 |
| `insert_append()` | ❌ 否 | 基类足够 |
| 其他标准方法 | ❌ 否 | 基类足够 |

---

## 8. Doris 实现参考

StarRocks 借鉴 Doris 实现，但不继承 `DorisEngineAdapter`。以下列出可参考的Doris 实现要点。

### 8.1 可参考的 Doris 实现

#### 8.1.1 `_create_table_from_columns()` 结构

**参考价值**：

- `_extract_and_validate_key_columns()` 的设计思路
- 列重排序的整体流程
- table_properties 的处理方式

**差异**：

```python
# Doris: 转换 primary_key → unique_key
table_properties["unique_key"] = exp.Tuple(...)
primary_key = None

# StarRocks: 直接使用 primary_key
primary_key = key_columns  # 不转换
```

#### 8.1.2 `delete_from()` WHERE TRUE 处理

**参考价值**：完全相同的实现

```python
# Doris 和 StarRocks 相同
if not where or where == exp.true():
    self.execute(f"TRUNCATE TABLE {table_name}")
```

#### 8.1.3 `_build_table_properties_exp()`

**参考价值**：

- 属性提取和验证的整体结构
- distributed_by, partitioned_by 的处理

**差异**：具体属性名可能不同

#### 8.1.4 `_build_partitioned_by_exp()`

**参考价值**：

- RANGE/LIST 分区的处理逻辑
- 分区边界的表达式构建

**差异**：StarRocks 支持表达式分区（多列/函数）

---

### 8.2 不能参考的 Doris 实现

#### 8.2.1 PRIMARY KEY 处理

**Doris**：转换为 UNIQUE KEY（不支持 PRIMARY KEY）
**StarRocks**：原生支持，不需要转换

#### 8.2.2 Expression Partitioning

**Doris**：不支持
**StarRocks**：支持多列和函数表达式

#### 8.2.3 CREATE INDEX

**Doris**：有 bug，会尝试执行
**StarRocks**：正确跳过

---

## 9. SQLGlot StarRocks Dialect

### 9.1 使用 StarRocks Dialect

**关键决策**：使用 SQLGlot 内置的 StarRocks dialect，**不使用** Doris dialect

```python
from sqlglot.dialects.starrocks import StarRocks

class StarRocksEngineAdapter(EngineAdapter):
    DIALECT = "starrocks"  # 不是 "doris"
```

### 9.2 已支持的特性

SQLGlot StarRocks dialect 已支持：

- ✅ 基本 DDL 语法（CREATE TABLE, VIEW）
- ✅ PRIMARY KEY 子句
- ✅ DUPLICATE KEY, UNIQUE KEY
- ✅ DISTRIBUTED BY HASH
- ✅ PARTITION BY RANGE/LIST
- ✅ table properties

### 9.3 可能需要扩展的特性

以下特性可能需要在 SQLGlot 中扩展：

- ⏳ 复杂的（多列、函数）表达式分区
- ⏳ VIEW 的 SECURITY 子句
- ⏳ 异步 MV 的 REFRESH 子句
- ⏳ MV 的 PARTITION BY（与表的不同）

**扩展方式**：

1. 贡献到 SQLGlot 项目
2. 临时在 adapter 中处理（字符串拼接）

---

## 10. 测试策略

### 10.1 测试目标

- ✅ 验证 PRIMARY KEY 和 DUPLICATE KEY 表创建
- ✅ 验证列重排序（key 列在前）
- ✅ 验证 DELETE 操作（TRUNCATE 转换）
- ✅ 验证多列表达式分区
- ✅ 验证 CREATE INDEX 跳过
- ⏳ 验证 VIEW 和 MV 创建

### 10.2 测试模型设计

**模型1**：PRIMARY KEY + 表达式分区

```sql
MODEL (
  partitioned_by (date_trunc('day', event_time), region),
  physical_properties (
    primary_key = (order_id, event_date),
    distributed_by = (HASH(order_id), 10)
  )
)
```

**模型2**：DUPLICATE KEY + RANGE 分区

```sql
MODEL (
  partitioned_by (event_date BETWEEN '2024-01-01' AND '2024-12-31'),
  physical_properties (
    duplicate_key = (event_date, region),
    distributed_by = (HASH(region), AUTO)
  )
)
```

### 10.3 验证要点

#### 10.3.1 列顺序验证

```sql
-- 验证生成的 DDL
SHOW CREATE TABLE test_table;

-- 预期：key 列在前
CREATE TABLE test_table (
  order_id BIGINT,      -- key column 1
  event_date DATE,      -- key column 2
  customer_id INT,      -- regular column
  ...
) PRIMARY KEY(order_id, event_date);
```

#### 10.3.2 PRIMARY KEY 生成验证

```python
# 验证 PRIMARY KEY 子句存在
assert "PRIMARY KEY" in ddl
assert "order_id" in ddl
assert "event_date" in ddl
```

#### 10.3.3 DELETE 操作验证

```python
# 验证 TRUNCATE 转换
adapter.delete_from("test_table", exp.true())
# 预期执行：TRUNCATE TABLE test_table
```

### 10.4 测试文件位置

- 目录：`mytest/test_model_covery/`
- 模型文件：`models/sr_*.sql`
- 测试README：`README.md`

---

## 附录 A. 核心概念详解

### A.1 表类型实现状态

StarRocks 支持 4 种表类型，当前实现状态：

| 表类型 | 实现状态 | 说明 |
|--------|----------|------|
| PRIMARY KEY | ✅ 完整实现 | 原生支持，直接传递 primary_key 给基类 |
| DUPLICATE KEY | ✅ 完整实现 | 通过 table_properties 传递 |
| UNIQUE KEY | ⚠️ 代码已有 | 未充分测试，暂不推荐 |
| AGGREGATE KEY | ❌ 暂不实现 | 需修改 model 支持 value 聚合类型 |

**实现要点**：

- 所有 key 类型都需要列重排序（key 列在前）
- `_reorder_columns_for_key()` 已支持所有 key 类型
- PRIMARY KEY 与 Doris 的主要区别：StarRocks 原生支持，不需转换

---

### A.2 分区与分桶

**分区类型**：

- 表达式分区：StarRocks 特有，支持多列/函数
- RANGE 分区：与 Doris 相同
- LIST 分区：与 Doris 相同

**分桶类型**：

- HASH 分桶：与 Doris 相同
- RANDOM 分桶：与 Doris 相同

**注意**：StarRocks 不支持 AUTO 分桶设置。

**实现要点**：

- `distributed_by` 采用嵌套 Tuple 结构，复用 Doris 实现
- `partitioned_by` 支持表达式列表，复用 `PartitionedByProperty`

---

## 附录 B. SQLMesh 关键设计模式

### B.1 Expression-Based Property

SQLMesh 要求所有 table_properties 的值必须是 Expression 对象：

```python
# ❌ 错误
properties = {"buckets": 10}

# ✅ 正确
properties = {"buckets": exp.Literal.number(10)}
```

### B.2 两级 AST 转换

```plain text
MODEL 语法 → SQLGlot Expression → 数据库 SQL
    ↓              ↓                  ↓
  解析器      EngineAdapter        Generator
```

### B.3 Dialect-Specific Generation

EngineAdapter 通过 `self.dialect` 控制SQL生成：

```python
sql = expression.sql(dialect=self.dialect, identify=True)
```

---

## 附录 C. 常见问题与解决方案

### C.1 PRIMARY KEY 子句被忽略

**问题**：生成的 DDL 中没有 PRIMARY KEY

**原因**：`SUPPORTS_INDEXES = False`

**解决方案**：

```python
# ✅ 正确
SUPPORTS_INDEXES = True  # 支持 PRIMARY KEY
def create_index(...):
    return  # 跳过独立索引
```

---

### C.2 列顺序错误

**问题**：`ERROR 1105: key columns must be the first n columns`

**原因**：未调用 `_reorder_columns_for_key()`

**解决方案**：在 `_create_table_from_columns()` 中重排序

---

### C.3 DELETE WHERE TRUE 失败

**问题**：`ERROR 1064: You have an error in your SQL syntax`

**原因**：StarRocks 不支持 `WHERE TRUE`

**解决方案**：转换为 `TRUNCATE TABLE`

---

### C.4 Expression 类型错误

**问题**：`TypeError: 'dict' object is not a sqlglot Expression`

**原因**：table_properties 使用了普通 Python dict

**解决方案**：转换为 Expression 对象

---

## 附录 D. 参考资源

### D.1 StarRocks 文档

- 官方文档：https://docs.starrocks.io/
- CREATE TABLE：https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/
- CREATE VIEW：https://docs.starrocks.io/docs/sql-reference/sql-statements/View/CREATE_VIEW/
- CREATE MATERIALIZED VIEW：https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW/

### D.2 SQLMesh 文档

- EngineAdapter：https://sqlmesh.readthedocs.io/
- Model 语法：https://sqlmesh.readthedocs.io/en/stable/concepts/models/overview/

### D.3 SQLGlot 文档

- 官方文档：https://sqlglot.com/
- StarRocks Dialect：https://github.com/tobymao/sqlglot/blob/main/sqlglot/dialects/starrocks.py

### D.4 代码位置

- StarRocks Adapter：`sqlmesh/core/engine_adapter/starrocks.py`
- Doris Adapter（参考）：`sqlmesh/core/engine_adapter/doris.py`
- 测试模型：`mytest/test_model_covery/models/`
- 设计文档：`starrocks_design.md`

---

## 总结

本文档提供了 StarRocks Engine Adapter 的完整实现参考：

1. ✅ **核心实现要点**：列重排序、PRIMARY KEY、DELETE、CREATE INDEX
2. ✅ **完整方法清单**：必须重载、可选重载、不需重载
3. ✅ **Doris 参考要点**：可参考和不可参考的实现
4. ✅ **View/MV 支持**：语法差异分析和实现建议
5. ✅ **测试策略**：验证要点和测试模型设计

**实现优先级**：

1. Phase 1：PRIMARY KEY + DUPLICATE KEY 表（已完成）
2. Phase 2：多列表达式分区（已完成）
3. Phase 3：VIEW 和同步 MV（待实现）
4. Phase 4：异步 MV 支持（未来）

---

**文档版本**：v2.0
**最后更新**：2025-11-03
**维护者**：StarRocks Adapter Team
