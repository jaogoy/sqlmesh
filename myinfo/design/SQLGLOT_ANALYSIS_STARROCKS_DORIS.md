# SQLGlot StarRocks/Doris 表属性 Generator 分析

## 概述

本文档分析 SQLGlot 在处理 StarRocks 和 Doris 的特殊建表语法（如 PARTITION BY、DISTRIBUTED BY、UNIQUE KEY 等）时的工作机制，重点关注 **Generator 部分如何要求特定的 Expression 结构才能生成正确的 SQL**。

## 1. 架构概览

### 1.1 SQLMesh + SQLGlot 的分层架构

```
┌─────────────────────────────────────────────────────────┐
│  SQLMesh Layer (Python API)                             │
│  - DorisEngineAdapter / StarRocksEngineAdapter          │
│  - 提供高级 API: create_table(), create_view()          │
│  - 处理业务逻辑: primary_key → unique_key 转换          │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│  SQLGlot Expression Layer (AST)                         │
│  - exp.Create, exp.Properties, exp.UniqueKeyProperty    │
│  - exp.PartitionByRangeProperty                         │
│  - exp.DistributedByProperty                            │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│  SQLGlot Generator Layer (SQL 生成)                     │
│  - Doris.Generator                                      │
│  - StarRocks.Generator                                  │
│  - 方法: uniquekeyproperty_sql(), partition_by_range()  │
└─────────────────────────────────────────────────────────┘
                         ↓
                   SQL 字符串输出
```

## 2. 核心机制：Expression → SQL 生成

### 2.1 SQLGlot Expression 结构基础

SQLGlot 使用 AST (Abstract Syntax Tree) 表示 SQL，每个节点都是 `exp.Expression` 的子类。理解 Expression 的内部结构对于正确构建 SQL 至关重要。

#### Expression 的通用属性

每个 Expression 对象有以下标准属性：

| 属性 | 用途 | 示例 |
|------|------|------|
| `this` | 主操作对象/左操作数 | 二元操作的左侧，函数的第一参数 |
| `expression` | 次要对象/右操作数 | 二元操作的右侧 |
| `expressions` | 多个子表达式列表 | SELECT 的列，函数的剩余参数 |
| 自定义字段 | 特定类型专用 | `buckets`, `unit`, `kind` 等 |

**重要**：不同 Expression 类型对这些属性的使用方式**不一致**，必须通过调试或查看源码才能确定具体结构！

#### 常见 Expression 类型的结构模式

**1. 二元操作（Binary Operations）**

```python
# exp.EQ, exp.GT, exp.LT, exp.And, exp.Or 等
exp.EQ(
    this=exp.Column("key"),           # 左操作数
    expression=exp.Literal("value")   # 右操作数
)
# SQL: key = 'value'

# 别名访问
eq.left   # 等同于 eq.this
eq.right  # 等同于 eq.expression
```

**2. 函数调用（Functions）**

```python
# 普通函数：this = 第一参数
exp.Coalesce(
    this=exp.Column("a"),                    # 第一参数
    expressions=[exp.Column("b"), exp.Column("c")]  # 其余参数
)
# SQL: COALESCE(a, b, c)

# 匿名函数：this = 函数名（字符串！）
exp.Anonymous(
    this="RANGE",                            # 函数名
    expressions=[exp.Column("dt")]          # 所有参数
)
# SQL: RANGE(dt)
```

**3. 容器类型（Containers）**

```python
# Tuple/Array：无 this，只有 expressions
exp.Tuple(
    expressions=[exp.Column("a"), exp.Column("b")]
)
# SQL: (a, b)

# Paren：this = 包裹的表达式
exp.Paren(
    this=exp.Add(this=a, expression=b)
)
# SQL: (a + b)
```

**4. DDL 属性（Table Properties）**

```python
# 直接列表型：无 this，只有 expressions
exp.UniqueKeyProperty(
    expressions=[exp.Column("id"), exp.Column("name")]
)
# SQL: UNIQUE KEY(id, name)

# Schema 包裹型：this = Schema wrapper
exp.PartitionedByProperty(
    this=exp.Schema(
        expressions=[exp.Column("dt")]
    )
)
# SQL: PARTITIONED BY (dt)
```

**5. 查询语句（特殊结构）**

```python
# exp.Select：this = FROM 子句（注意不是 SELECT 列！）
exp.Select(
    this=exp.From(this=exp.Table("table")),  # FROM 是 this!
    expressions=[exp.Column("a"), exp.Column("b")]  # SELECT 列是 expressions
)
# SQL: SELECT a, b FROM table

# exp.Create：this = 表结构，expression = 查询（CTAS）
exp.Create(
    this=exp.Schema(...),      # 表定义
    expression=exp.Select(...), # AS SELECT ...
    properties=exp.Properties(...)
)
```

#### 为什么结构不一致？

**历史原因**和**语义差异**导致不同 Expression 使用不同模式：

1. **语义适配**：
   - `PARTITION BY` 支持表达式 → 用 `Schema` 包裹以支持类型标注
   - `PRIMARY KEY` 只允许列名 → 直接用 `expressions` 列表

2. **演化过程**：
   - SQLGlot 不同版本添加的功能使用了不同设计模式
   - 为了向后兼容，无法统一结构

3. **调试必要性**：
   - **无法通过命名推断结构**
   - 必须通过 `print(expr)` 或查看 `arg_types` 确定实际结构
   - 查看 Generator 源码了解预期结构

**调试技巧**：
```python
# 查看 Expression 的参数定义
print(exp.PartitionedByProperty.arg_types)
# {'this': True}  # 需要 this，通常是 Schema

print(exp.UniqueKeyProperty.arg_types)
# {'expressions': True}  # 需要 expressions 列表

# 打印实际结构
parsed = parse_one("PARTITIONED BY (col1, col2)", into=exp.PartitionedByProperty)
print(parsed)
# PartitionedByProperty(this=Schema(expressions=[Column(...)]))
```

### 2.2 Generator 的工作原理

SQLGlot Generator 通过**双派发模式**（Double Dispatch）将 Expression 对象转换为 SQL 字符串：

```python
# 伪代码示例
class Generator:
    def sql(self, expression: exp.Expression) -> str:
        # 根据 expression 的类型调用对应的 xxx_sql() 方法
        method_name = f"{expression.key}_sql"
        return getattr(self, method_name)(expression)

    # 每个 Expression 类型都有对应的生成方法
    def uniquekeyproperty_sql(self, expression: exp.UniqueKeyProperty) -> str:
        columns = self.expressions(expression, flat=True)
        return f"UNIQUE KEY({columns})"

    def partitionbyrangeproperty_sql(self, expression: exp.PartitionByRangeProperty) -> str:
        partition_cols = self.expressions(expression.partition_expressions)
        partition_defs = self.expressions(expression.create_expressions)
        return f"PARTITION BY RANGE({partition_cols}) ({partition_defs})"
```

**关键点**：
- Generator 不关心 Expression 从哪里来
- **只关心 Expression 的结构是否符合预期**
- 结构正确 → 生成正确的 SQL
- 结构错误 → 生成错误的 SQL 或报错
- **必须准确了解每种 Expression 的内部结构要求**

## 3. 表属性的 Expression 结构要求

### 3.1 UNIQUE KEY / PRIMARY KEY

#### Doris: UNIQUE KEY

**要求的 Expression 结构**：
```python
exp.UniqueKeyProperty(
    expressions=[
        exp.Column("id"),
        exp.Column("created_at")
    ]
)
```

**生成的 SQL**：
```sql
UNIQUE KEY(`id`, `created_at`)
```

**在 SQLMesh 中如何构建**：
```python
# Doris Adapter 中的转换逻辑
if primary_key:
    table_properties["unique_key"] = exp.Tuple(
        expressions=[exp.to_column(col) for col in primary_key]
    )

# 在 _build_table_properties_exp() 中处理
unique_key = table_properties_copy.pop("unique_key", None)
if isinstance(unique_key, exp.Tuple):
    column_names = []
    for expr in unique_key.expressions:
        if isinstance(expr, exp.Column) and hasattr(expr, "this"):
            column_names.append(str(expr.this.this))

    properties.append(
        exp.UniqueKeyProperty(
            expressions=[exp.to_column(k) for k in column_names]
        )
    )
```

#### StarRocks: PRIMARY KEY

**要求的 Expression 结构**：
```python
exp.PrimaryKeyColumnConstraint(
    expressions=[
        exp.Column("id"),
        exp.Column("created_at")
    ]
)
```

**生成的 SQL**：
```sql
PRIMARY KEY(`id`, `created_at`)
```

**在 SQLMesh 中如何构建**：
```python
# StarRocks Adapter 直接传递给 base class
# base class 会自动构建 PrimaryKeyColumnConstraint
super()._create_table_from_columns(
    primary_key=("id", "created_at"),  # 传递元组
    ...
)
```

### 3.2 PARTITION BY

#### PARTITION BY RANGE

**要求的 Expression 结构**：
```python
exp.PartitionByRangeProperty(
    partition_expressions=[
        exp.Column("dt")  # PARTITION BY RANGE(dt)
    ],
    create_expressions=[
        exp.Var(this="PARTITION p1 VALUES LESS THAN ('2024-01-01')")
    ]
)
```

**生成的 SQL**：
```sql
PARTITION BY RANGE(`dt`) (
    PARTITION p1 VALUES LESS THAN ('2024-01-01')
)
```

**在 SQLMesh 中如何构建**：
```python
def _build_partitioned_by_exp(
    self,
    partitioned_by: t.List[exp.Expression],
    **kwargs
) -> exp.PartitionByRangeProperty:
    # 解析 partitioned_by 表达式
    partitioned_by, partition_kind = self._parse_partition_expressions(partitioned_by)

    # 处理 partition 定义字符串
    partitions = kwargs.get("partitions")
    create_expressions = None
    if partitions:
        if isinstance(partitions, exp.Tuple):
            create_expressions = [
                exp.Var(this=e.this, quoted=False)
                for e in partitions.expressions
            ]

    # 构建 RANGE 分区
    return exp.PartitionByRangeProperty(
        partition_expressions=partitioned_by,
        create_expressions=create_expressions,
    )
```

#### PARTITION BY LIST

**要求的 Expression 结构**：
```python
exp.PartitionByListProperty(
    partition_expressions=[
        exp.Column("region")  # PARTITION BY LIST(region)
    ],
    create_expressions=[
        exp.Var(this="PARTITION p_asia VALUES IN ('China', 'Japan')")
    ]
)
```

**生成的 SQL**：
```sql
PARTITION BY LIST(`region`) (
    PARTITION p_asia VALUES IN ('China', 'Japan')
)
```

#### 表达式分区（StarRocks 特有）

**要求的 Expression 结构**：
```python
exp.PartitionedByProperty(
    this=exp.Schema(
        expressions=[
            exp.Anonymous(
                this="DATE_TRUNC",
                expressions=[
                    exp.Literal.string("day"),
                    exp.Column("order_time")
                ]
            )
        ]
    )
)
```

**生成的 SQL**：
```sql
PARTITION BY (DATE_TRUNC('day', `order_time`))
```

### 3.3 DISTRIBUTED BY

#### DISTRIBUTED BY HASH

**要求的 Expression 结构**：
```python
exp.DistributedByProperty(
    expressions=[
        exp.Column("id")
    ],
    buckets=exp.Literal.number(10)
)
```

**生成的 SQL**：
```sql
DISTRIBUTED BY HASH(`id`) BUCKETS 10
```

**在 SQLMesh 中如何构建**：

Doris/StarRocks Adapter 使用了一种**扁平化的属性表示**（基于项目中的 memory 经验）：

```python
# 输入格式（扁平化）
table_properties = {
    "distributed_by": exp.Tuple(
        expressions=[
            exp.EQ(
                this=exp.Column("kind"),
                expression=exp.Literal.string("HASH")
            ),
            exp.EQ(
                this=exp.Column("expressions"),
                expression=exp.Column("orderkey")  # 或 exp.Array([...])
            ),
            exp.EQ(
                this=exp.Column("buckets"),
                expression=exp.Literal.number(10)
            )
        ]
    )
}

# 在 _build_table_properties_exp() 中解析
distributed_by = table_properties_copy.pop("distributed_by", None)
if isinstance(distributed_by, exp.Tuple):
    distributed_info = {}
    for expr in distributed_by.expressions:
        if isinstance(expr, exp.EQ):
            key = str(expr.this.this).strip('"')
            if isinstance(expr.expression, exp.Literal):
                distributed_info[key] = expr.expression.this
            elif isinstance(expr.expression, exp.Array):
                distributed_info[key] = [
                    str(e.this) for e in expr.expression.expressions
                ]

    # 构建 DistributedByProperty
    if distributed_info.get("kind") == "HASH":
        expressions = distributed_info.get("expressions")
        if isinstance(expressions, str):
            expressions = [expressions]

        properties.append(
            exp.DistributedByProperty(
                expressions=[exp.to_column(e) for e in expressions],
                buckets=exp.Literal.number(distributed_info["buckets"])
            )
        )
```

**为什么使用扁平化格式？**

根据项目 memory：
> "Use flattened properties like distributed_by_kind and distributed_by_columns instead of complex expressions when defining StarRocks table properties in SQLMesh to ensure proper parsing and compatibility."

这是因为：
1. **避免嵌套结构解析问题**：SQLGlot 的 Parser 对某些嵌套结构支持不完善
2. **单向限制**：SQLGlot 可以生成 `DISTRIBUTED BY` SQL，但无法解析回来
3. **显式控制**：扁平化格式让开发者显式控制每个属性的值

## 4. 完整的建表流程示例

### 4.1 Doris 建表

**Python 代码**：
```python
adapter.create_table(
    table_name="db.sales",
    target_columns_to_types={
        "id": exp.DataType.build("INT"),
        "amount": exp.DataType.build("DECIMAL(18,2)"),
        "dt": exp.DataType.build("DATE")
    },
    primary_key=("id",),  # 会被转换为 unique_key
    partitioned_by=[
        exp.Anonymous(this="RANGE", expressions=[exp.Column("dt")])
    ],
    table_properties={
        "distributed_by": exp.Tuple(
            expressions=[
                exp.EQ(this=exp.Column("kind"), expression=exp.Literal.string("HASH")),
                exp.EQ(this=exp.Column("expressions"), expression=exp.Column("id")),
                exp.EQ(this=exp.Column("buckets"), expression=exp.Literal.number(10))
            ]
        ),
        "partitions": exp.Tuple(
            expressions=[
                exp.Literal.string("PARTITION p1 VALUES LESS THAN ('2024-01-01')")
            ]
        )
    }
)
```

**内部转换流程**：

```
Step 1: _create_table_from_columns()
  - primary_key=("id",) → table_properties["unique_key"] = exp.Tuple([exp.Column("id")])
  - primary_key=None (传递给 base class)

Step 2: _build_table_properties_exp()
  - unique_key → exp.UniqueKeyProperty(expressions=[exp.Column("id")])
  - partitioned_by → exp.PartitionByRangeProperty(...)
  - distributed_by → exp.DistributedByProperty(...)

Step 3: 构建 CREATE 表达式
  exp.Create(
      this=exp.Schema(...),
      kind="TABLE",
      properties=exp.Properties(
          expressions=[
              exp.UniqueKeyProperty(...),
              exp.PartitionByRangeProperty(...),
              exp.DistributedByProperty(...)
          ]
      )
  )

Step 4: Generator 生成 SQL
  Doris.Generator.sql(create_exp)
  → "CREATE TABLE db.sales (...) UNIQUE KEY(id) PARTITION BY RANGE(dt) (...) DISTRIBUTED BY HASH(id) BUCKETS 10"
```

**生成的 SQL**：
```sql
CREATE TABLE IF NOT EXISTS `db`.`sales` (
    `id` INT,
    `amount` DECIMAL(18,2),
    `dt` DATE
)
UNIQUE KEY(`id`)
PARTITION BY RANGE(`dt`) (
    PARTITION p1 VALUES LESS THAN ('2024-01-01')
)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
```

### 4.2 StarRocks 建表

**Python 代码**：
```python
adapter.create_table(
    table_name="db.sales",
    target_columns_to_types={...},
    primary_key=("id",),  # 直接传递，不转换
    partitioned_by=[exp.Column("dt")],
    table_properties={
        "distributed_by": {...}  # 同 Doris
    }
)
```

**内部转换流程**：

```
Step 1: _create_table_from_columns()
  - primary_key=("id",) → 直接传递给 base class（不转换）

Step 2: base._build_table_properties_exp() 或 StarRocks._build_table_properties_exp()
  - primary_key → exp.PrimaryKeyColumnConstraint(expressions=[exp.Column("id")])
  - partitioned_by → exp.PartitionByRangeProperty(...) 或 exp.PartitionedByProperty(...)
  - distributed_by → exp.DistributedByProperty(...)

Step 3: Generator 生成 SQL
  StarRocks.Generator.sql(create_exp)
  → "CREATE TABLE db.sales (...) PRIMARY KEY(id) PARTITION BY RANGE(dt) (...) DISTRIBUTED BY HASH(id) BUCKETS 10"
```

**生成的 SQL**：
```sql
CREATE TABLE IF NOT EXISTS `db`.`sales` (
    `id` INT,
    `amount` DECIMAL(18,2),
    `dt` DATE
)
PRIMARY KEY(`id`)
PARTITION BY RANGE(`dt`) (...)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
```

## 5. 关键发现与设计模式

### 5.1 单向生成限制

根据项目 memory：
> "SQLGlot defines DistributedByProperty but no dialect implements a parser for 'DISTRIBUTED BY' clause, meaning it can generate SQL but cannot parse it back, creating a one-way limitation in SQLMesh model definitions."

这意味着：
- **生成方向**：Expression → SQL ✅ 支持
- **解析方向**：SQL → Expression ❌ 不支持

**影响**：
- SQLMesh 必须手动构建 Expression 树
- 不能依赖 `parse_one("CREATE TABLE ... DISTRIBUTED BY ...")` 来获取结构
- 所有表属性必须通过 Python API 显式指定

### 5.2 扁平化属性模式

**为什么使用 `exp.Tuple([exp.EQ(...), exp.EQ(...)])` 而不是直接传递字典？**

```python
# ❌ 不能直接传递字典
table_properties = {
    "distributed_by": {
        "kind": "HASH",
        "expressions": ["id"],
        "buckets": 10
    }
}

# ✅ 必须转换为 Expression
table_properties = {
    "distributed_by": exp.Tuple(
        expressions=[
            exp.EQ(this=exp.Column("kind"), expression=exp.Literal.string("HASH")),
            exp.EQ(this=exp.Column("expressions"), expression=exp.Column("id")),
            exp.EQ(this=exp.Column("buckets"), expression=exp.Literal.number(10))
        ]
    )
}
```

**原因**：
1. **类型安全**：Expression 对象有明确的类型信息
2. **SQL 生成一致性**：Generator 只处理 Expression 对象
3. **避免运行时错误**：编译时检查表达式结构

### 5.3 Adapter 的职责分层

| 层级 | 职责 | 示例 |
|------|------|------|
| **SQLMesh Adapter** | 业务逻辑转换 | `primary_key → unique_key`（仅 Doris）<br>扁平化属性 → Expression 树 |
| **SQLGlot Expression** | 表示 SQL 结构 | `exp.UniqueKeyProperty`<br>`exp.PartitionByRangeProperty` |
| **SQLGlot Generator** | Expression → SQL | `uniquekeyproperty_sql()`<br>`partitionbyrangeproperty_sql()` |

## 6. 实际使用建议

### 6.1 定义 Doris/StarRocks 表时

```python
# 1. 定义列类型
columns_to_types = {
    "id": exp.DataType.build("INT"),
    "name": exp.DataType.build("VARCHAR(100)"),
    "dt": exp.DataType.build("DATE")
}

# 2. 定义分区（使用 Anonymous 函数指定 RANGE/LIST）
partitioned_by = [
    exp.Anonymous(this="RANGE", expressions=[exp.Column("dt")])
]

# 3. 定义分布（使用扁平化格式）
distributed_by = exp.Tuple(
    expressions=[
        exp.EQ(this=exp.Column("kind"), expression=exp.Literal.string("HASH")),
        exp.EQ(this=exp.Column("expressions"), expression=exp.Column("id")),
        exp.EQ(this=exp.Column("buckets"), expression=exp.Literal.number(10))
    ]
)

# 4. 调用 create_table
adapter.create_table(
    table_name="db.table_name",
    target_columns_to_types=columns_to_types,
    primary_key=("id",),  # Doris → unique_key, StarRocks → primary_key
    partitioned_by=partitioned_by,
    table_properties={
        "distributed_by": distributed_by,
        "partitions": exp.Tuple(
            expressions=[
                exp.Literal.string("PARTITION p1 VALUES LESS THAN ('2024-01-01')")
            ]
        )
    }
)
```

### 6.2 检查生成的 SQL

```python
# 启用调试日志
import logging
logging.getLogger("sqlmesh").setLevel(logging.DEBUG)

# 或者手动调用 _to_sql 查看生成的 SQL
properties_exp = adapter._build_table_properties_exp(
    partitioned_by=partitioned_by,
    table_properties={"distributed_by": distributed_by}
)
print(properties_exp.sql(dialect="doris"))
```

## 7. 总结

### 核心要点

1. **Expression 结构决定一切**
   - Generator 根据 Expression 的**类型**和**结构**生成 SQL
   - 正确的结构 = 正确的 SQL
   - 错误的结构 = 错误的 SQL 或运行时错误

2. **单向生成模式**
   - SQLGlot 可以生成 DISTRIBUTED BY 等语法
   - 但无法解析这些语法回 Expression
   - 必须手动构建 Expression 树

3. **扁平化属性模式**
   - 使用 `exp.Tuple([exp.EQ(...)])` 表示复杂属性
   - 避免嵌套字典，提高类型安全性
   - Adapter 负责解析扁平化格式 → 构建最终 Expression

4. **Doris vs StarRocks 的关键差异**
   - Doris: `primary_key → unique_key` 转换
   - StarRocks: 直接使用 `primary_key`
   - 其他属性（PARTITION BY, DISTRIBUTED BY）大部分相同

### Expression 结构速查表

| SQL 语法 | Expression 类型 | 必需字段 |
|----------|----------------|---------|
| `UNIQUE KEY(id)` | `exp.UniqueKeyProperty` | `expressions=[exp.Column("id")]` |
| `PRIMARY KEY(id)` | `exp.PrimaryKeyColumnConstraint` | `expressions=[exp.Column("id")]` |
| `PARTITION BY RANGE(dt)` | `exp.PartitionByRangeProperty` | `partition_expressions=[...]`<br>`create_expressions=[...]` |
| `PARTITION BY LIST(region)` | `exp.PartitionByListProperty` | `partition_expressions=[...]`<br>`create_expressions=[...]` |
| `PARTITIONED BY (dt)` | `exp.PartitionedByProperty` | `this=exp.Schema(expressions=[...])` |
| `DISTRIBUTED BY HASH(id) BUCKETS 10` | `exp.DistributedByProperty` | `expressions=[...]`<br>`buckets=exp.Literal.number(10)` |
| `COMMENT 'table comment'` | `exp.SchemaCommentProperty` | `this=exp.Literal.string("...")` |

### 常见 Expression 类型的 this/expression/expressions 使用方式

| Expression 类型 | `this` | `expression` | `expressions` | 示例/注释 |
|----------------|--------|--------------|---------------|----------|
| **二元操作** | | | | |
| `EQ`, `GT`, `LT`, `GTE`, `LTE` | 左操作数 | 右操作数 | - | `a = b` → `EQ(this=a, expression=b)` |
| `And`, `Or` | 左操作数 | 右操作数 | - | `a AND b` → `And(this=a, expression=b)` |
| `Add`, `Sub`, `Mul`, `Div` | 左操作数 | 右操作数 | - | `a + b` → `Add(this=a, expression=b)` |
| **一元操作** | | | | |
| `Not` | 被否定的表达式 | - | - | `NOT a` → `Not(this=a)` |
| `Paren` | 被括号包裹的表达式 | - | - | `(a + b)` → `Paren(this=Add(...))` |
| **函数** | | | | |
| 普通函数（`Coalesce` 等） | 第一参数 | - | 剩余参数 | `COALESCE(a, b)` → `Coalesce(this=a, expressions=[b])` |
| `Anonymous` | **函数名（字符串！）** | - | 所有参数 | `RANGE(dt)` → `Anonymous(this='RANGE', expressions=[dt])` |
| **容器** | | | | |
| `Tuple` | - | - | 元素列表 | `(a, b)` → `Tuple(expressions=[a, b])` |
| `Array` | - | - | 元素列表 | `[a, b]` → `Array(expressions=[a, b])` |
| `Schema` | 表（可选） | - | 列定义列表 | `(col1 INT, col2 STRING)` → `Schema(expressions=[...])` |
| **DDL 属性** | | | | |
| `UniqueKeyProperty` | - | - | 列列表 | `UNIQUE KEY(a, b)` → `UniqueKeyProperty(expressions=[a, b])` |
| `PrimaryKeyColumnConstraint` | - | - | 列列表 | `PRIMARY KEY(a, b)` → `PrimaryKeyColumnConstraint(expressions=[a, b])` |
| `PartitionedByProperty` | **Schema 包裹！** | - | - | `PARTITIONED BY (dt)` → `PartitionedByProperty(this=Schema(...))` |
| `DistributedByProperty` | - | - | 列列表 | 还有 `buckets` 自定义字段 |
| `Properties` | - | - | Property 列表 | 容器，包含多个 Property |
| `Property` | 属性名（字符串） | 属性值 | - | `key = value` → `Property(this='key', expression=value)` |
| **查询语句** | | | | |
| `Select` | **FROM 子句！** | - | SELECT 列列表 | `SELECT a FROM t` → `Select(this=From(...), expressions=[a])` |
| `From` | 表/子查询 | - | - | `FROM table` → `From(this=Table(...))` |
| `Create` | Schema/Table | CTAS 查询 | - | `CREATE TABLE t AS SELECT...` → `Create(this=Schema, expression=Select)` |
| `Insert` | 表 | VALUES/SELECT | - | `INSERT INTO t SELECT...` → `Insert(this=Table, expression=Select)` |

**关键发现**：
1. **`exp.Anonymous`**：`this` 是函数名（字符串），不是参数！
2. **`exp.PartitionedByProperty`**：`this` 是 `Schema` wrapper，需要 `.this.expressions` 解包
3. **`exp.Select`**：`this` 是 `FROM` 子句，`expressions` 才是 SELECT 列
4. **`exp.Property`**：`this` 是属性名（字符串），`expression` 是属性值
5. **容器类型**（`Tuple`/`Array`/`Properties`）：只有 `expressions`，没有 `this`
6. **调试优先**：结构不统一，必须通过 `print(expr)` 或查看 `arg_types` 确认实际结构

### 关键文件参考

- **SQLMesh Adapters**:
  - `/sqlmesh/core/engine_adapter/doris.py`
  - `/sqlmesh/core/engine_adapter/starrocks.py`
  - `/sqlmesh/core/engine_adapter/base.py`

- **SQLGlot（外部依赖）**:
  - `sqlglot/expressions.py` - Expression 定义
  - `sqlglot/dialects/doris.py` - Doris Generator
  - `sqlglot/dialects/starrocks.py` - StarRocks Generator

- **测试用例**:
  - `/tests/core/engine_adapter/test_doris.py` - 查看 Expression 构建示例
