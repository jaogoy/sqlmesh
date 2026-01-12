# SQLMesh 模型配置解析流程完整分析

本文档详细分析 SQLMesh 如何解析一个 model 文件（.sql 文件）的完整过程，包括 MODEL 定义部分和 SELECT 查询部分。

## 目录

1. [概述](#概述)
2. [整体架构](#整体架构)
3. [解析流程详解](#解析流程详解)
4. [配置参数解析机制](#配置参数解析机制)
5. [关键文件说明](#关键文件说明)
6. [示例分析](#示例分析)
7. [总结](#总结)

---

## 概述

### SQLMesh MODEL 文件结构

一个典型的 SQLMesh model 文件包含两部分：

```sql
MODEL (
    name db.table,
    kind FULL,
    partitioned_by (col1, col2),
    clustered_by col3,
    grain (id, ds),
    physical_properties (
        distributed_by = (kind = 'HASH', expressions = 'col1', buckets = 10)
    )
);

SELECT col1, col2, col3, id, ds FROM source_table;
```

- **MODEL 定义部分**：包含模型的元数据配置（name, kind, partitioned_by 等）
- **SELECT 查询部分**：定义模型的数据逻辑

### 解析语法

**关键问题**：这些配置参数是否使用 SQLGlot 解析？

**答案**：**是的，但使用的是 SQLMesh 自定义的 Dialect 扩展**

SQLMesh 基于 SQLGlot 构建了自己的 DSL（Domain Specific Language）来解析 MODEL 配置：

- SQLMesh 扩展了 SQLGlot 的 Parser 和 Tokenizer
- 添加了专门的 MODEL、AUDIT、METRIC 等解析器
- 复用 SQLGlot 的表达式解析能力（如 `partitioned_by`、`columns` 等）

---

## 整体架构

### 解析流程图

```
┌─────────────────────────────────────────────────────────────┐
│                     .sql 文件                                │
│  MODEL (...);                                                │
│  SELECT ...;                                                 │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  1. 文件读取 (loader.py)                                      │
│     - load_sql_models()                                      │
│     - 读取文件内容为字符串                                    │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  2. SQL 解析 (dialect.py)                                    │
│     - parse(sql, default_dialect)                            │
│     - 分词 (Tokenizer)                                        │
│     - 识别 Jinja 模板块                                       │
│     - 分块处理 (ChunkType)                                    │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  3. MODEL 块解析 (dialect.py)                                │
│     - _create_parser() 创建 MODEL 解析器                      │
│     - 解析每个配置属性 (name, kind, partitioned_by, ...)     │
│     - 生成 exp.Property 表达式树                              │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  4. 配置渲染 (definition.py)                                  │
│     - load_sql_based_model()                                 │
│     - 渲染宏和变量                                            │
│     - 提取 meta_fields                                        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  5. 模型对象创建 (definition.py + meta.py)                    │
│     - create_sql_model() 或 create_seed_model()              │
│     - Pydantic 验证器处理配置                                 │
│     - 生成 SqlModel 对象                                      │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  6. 查询解析 (definition.py)                                  │
│     - 解析 SELECT 语句                                        │
│     - 提取 pre/post statements                                │
│     - 生成 QueryRenderer                                      │
└─────────────────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  7. Plan 创建与调度 (context.py + scheduler.py)              │
│     - Context.plan() 生成执行计划                             │
│     - Scheduler 调度各个 Snapshot                            │
│     - SnapshotEvaluator 执行具体操作                         │
└─────────────────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  8. EvaluationStrategy 执行 (snapshot/evaluator.py)        │
│     - _evaluation_strategy() 选择策略类                    │
│     - MaterializableStrategy.create() 创建表               │
│     - 调用 EngineAdapter 方法                                │
└─────────────────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  9. EngineAdapter 处理 (engine_adapter/base.py)            │
│     - create_table() → _create_table_from_columns()        │
│     - _build_table_properties_exp() 构建属性                │
│     - _create_table() → _build_create_table_exp()          │
└─────────────────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  10. SQL 生成与执行 (SQLGlot Generator)                      │
│     - execute() 调用 expression.sql(dialect=...)            │
│     - Generator 将 AST 转换为 SQL 字符串                    │
│     - 发送 SQL 到数据库执行                                   │
└─────────────────────────────────────────────────────────────┘
```

### 下游执行流程（Model 解析后）

上面的流程图展示了从 .sql 文件到 SqlModel 对象的解析过程。解析完成后，还有以下执行流程：

```
SqlModel 对象
    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│ Plan 创建                                                             │
│ context.py: Context.plan()                                          │
│   └─ 创建 Plan 对象，包含要执行的 Snapshots                           │
└──────────────────────────────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│ Plan 应用                                                             │
│ context.py: Context._apply(plan)                                    │
│   └─ PlanEvaluator.evaluate(plan)                                   │
└──────────────────────────────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│ Scheduler 调度                                                        │
│ scheduler.py: Scheduler.run_merged_intervals()                       │
│   ├─ 构建执行 DAG (CreateNode / EvaluateNode)                          │
│   └─ 对每个 Snapshot 调用 evaluate()                                  │
└──────────────────────────────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│ Snapshot 评估                                                         │
│ snapshot/evaluator.py: SnapshotEvaluator.evaluate()                  │
│   ├─ _evaluation_strategy(snapshot, adapter) 选择策略                 │
│   │   • FullRefreshStrategy      (全量刷新)                           │
│   │   • IncrementalByTimeRange   (增量时间范围)                        │
│   │   • ViewStrategy            (VIEW)                               │
│   │   • SeedStrategy            (Seed 数据)                           │
│   └─ 调用 strategy.create() 或 strategy.insert()                     │
└──────────────────────────────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│ EvaluationStrategy 执行                                               │
│ snapshot/evaluator.py: MaterializableStrategy.create()               │
│                                                                      │
│   if model.annotated:  # 有明确列定义                                  │
│       adapter.create_table(                                          │
│           table_name,                                                │
│           target_columns_to_types=model.columns_to_types_or_raise,   │
│           partitioned_by=model.partitioned_by,                       │
│           clustered_by=model.clustered_by,                           │
│           table_properties=physical_properties,  # ← 包含 primary_key │
│       )                                                              │
│   else:                                                              │
│       adapter.ctas(table_name, ctas_query, ...)                      │
└──────────────────────────────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│ EngineAdapter 处理                                                    │
│ engine_adapter/base.py + 具体 adapter (starrocks.py 等)              │
│                                                                      │
│   create_table() [base.py:619]                                       │
│   └─ _create_table_from_columns() [可被子类重载]                    │
│       ├─ _build_schema_exp()        # 构建列定义 + PRIMARY KEY        │
│       └─ _create_table()            # 构建并执行 CREATE TABLE         │
│           ├─ _build_create_table_exp()                              │
│           │   └─ _build_table_properties_exp() [可被子类重载]       │
│           └─ execute(create_exp)     # 执行 SQL                      │
└──────────────────────────────────────────────────────────────────────────┘
    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│ SQL 生成与执行                                                         │
│ engine_adapter/base.py: execute()                                    │
│                                                                      │
│   sql = expression.sql(dialect=self.dialect, ...)                    │
│   # SQLGlot Generator 将 AST 转换为 SQL 字符串                       │
│   # 例如: exp.Create → "CREATE TABLE IF NOT EXISTS ..."             │
│                                                                      │
│   self.cursor.execute(sql)                                           │
│   # 发送到数据库执行                                                    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 解析流程详解

### 第一步：文件读取

**文件**：`sqlmesh/core/loader.py`

```python
def load_sql_models(path: Path) -> t.List[Model]:
    # 读取 .sql 文件
    with open(path, "r", encoding="utf-8") as file:
        # 调用 dialect.parse() 解析 SQL
        expressions = parse(file.read(), default_dialect=_defaults["dialect"])

    # 调用 load_sql_based_models 创建模型对象
    models = load_sql_based_models(expressions, path=Path(path).absolute(), **_defaults)
    return models
```

### 第二步：SQL 分词和分块

**文件**：`sqlmesh/core/dialect.py`

```python
def parse(
    sql: str,
    default_dialect: t.Optional[str] = None,
    match_dialect: bool = True,
    into: t.Optional[exp.IntoType] = None,
) -> t.List[exp.Expression]:
    """解析 SQL 字符串，支持 MODEL 定义和 Jinja 模板"""

    # 1. 检测 dialect（从 MODEL 块的 dialect 参数）
    match = match_dialect and DIALECT_PATTERN.search(sql[:MAX_MODEL_DEFINITION_SIZE])
    dialect = Dialect.get_or_raise(match.group(2) if match else default_dialect)

    # 2. 分词
    tokens = dialect.tokenize(sql)

    # 3. 分块（识别 SQL、Jinja、Virtual Statement）
    chunks: t.List[t.Tuple[t.List[Token], ChunkType]] = [([], ChunkType.SQL)]

    # 遍历 tokens，按类型分块
    while pos < total:
        token = tokens[pos]
        if _is_jinja_query_begin(tokens, pos):
            chunks.append(([token], ChunkType.JINJA_QUERY))
        elif _is_jinja_statement_begin(tokens, pos):
            chunks.append(([token], ChunkType.JINJA_STATEMENT))
        else:
            chunks[-1][0].append(token)
        pos += 1

    # 4. 解析每个块
    parser = dialect.parser()
    for chunk, chunk_type in chunks:
        if chunk_type == ChunkType.SQL:
            expressions.extend(parser.parse(chunk, sql))
        else:  # Jinja
            expressions.append(parse_jinja_chunk(chunk))

    return expressions
```

**关键点**：

- 使用 SQLGlot 的 `Tokenizer` 进行分词
- 识别 Jinja 模板块（`{{ ... }}`, `{% ... %}`）
- 将 SQL 和 Jinja 分别处理

### 第三步：MODEL 块解析

**文件**：`sqlmesh/core/dialect.py`

#### 3.1 创建 MODEL 解析器

```python
def _create_parser(expression_type: t.Type[exp.Expression], table_keys: t.List[str]) -> t.Callable:
    """创建自定义解析器（MODEL、AUDIT、METRIC）"""

    def parse(self: Parser) -> t.Optional[exp.Expression]:
        expressions: t.List[exp.Expression] = []

        # 循环解析每个配置属性
        while True:
            # 匹配逗号
            if not self._match(TokenType.COMMA, expression=prev_property) and expressions:
                break

            # 解析键（name, kind, partitioned_by, ...）
            key_expression = self._parse_id_var(any_token=True)
            if not key_expression:
                break

            key = key_expression.name.lower()

            # 根据键类型选择解析方法
            if key in table_keys:  # name
                value = self._parse_table_parts()
            elif key == "columns":
                value = self._parse_schema()
            elif key == "kind":
                value = self._parse_kind()
            elif key == "partitioned_by":
                value = self._parse_partitioned_by()
            else:  # 默认
                value = self._parse_bracket(self._parse_field(any_token=True))

            # 生成 Property 表达式
            expressions.append(self.expression(exp.Property, this=key, value=value))

        # 返回 Model/Audit/Metric 表达式
        return self.expression(expression_type, expressions=expressions)

    return parse

# 注册解析器
PARSERS = {
    "MODEL": _create_parser(Model, ["name"]),
    "AUDIT": _create_parser(Audit, ["model"]),
    "METRIC": _create_parser(Metric, ["name"]),
}
```

#### 3.2 各配置参数的解析方法

| 配置参数 | 解析方法 | 说明 | 文件位置 |
|---------|---------|------|---------|
| `name` | `_parse_table_parts()` | 解析表名（支持 catalog.db.table） | SQLGlot Parser |
| `columns` | `_parse_schema()` | 解析列定义 `(col1 INT, col2 STRING)` | SQLGlot Parser |
| `kind` | 自定义解析 | 解析 ModelKind（FULL、INCREMENTAL 等） | dialect.py:603 |
| `partitioned_by` | `_parse_partitioned_by()` | 解析分区字段 | SQLGlot Parser |
| `clustered_by` | `_parse_bracket(_parse_field())` | 解析聚簇字段 | SQLGlot Parser |
| `grain` | `_parse_bracket(_parse_field())` | 解析粒度字段 | SQLGlot Parser |
| `physical_properties` | `_parse_bracket(_parse_field())` | 解析物理属性 | SQLGlot Parser |
| `virtual_properties` | `_parse_bracket(_parse_field())` | 解析虚拟属性 | SQLGlot Parser |
| 其他 | `_parse_bracket(_parse_field())` | 通用解析方法 | SQLGlot Parser |

#### 3.3 特殊参数解析示例

##### `partitioned_by` 解析

```python
# 在 dialect.py:635-640
elif key == "partitioned_by":
    partitioned_by = self._parse_partitioned_by()  # 调用 SQLGlot 的方法
    if isinstance(partitioned_by.this, exp.Schema):
        # 如果是 (a, b, c) 形式，转换为 Tuple
        value = exp.tuple_(*partitioned_by.this.expressions)
    else:
        # 如果是单个字段 a，直接取值
        value = partitioned_by.this
```

SQLGlot 的 `_parse_partitioned_by()` 实现（在 `sqlglot/parser.py`）：

```python
def _parse_partitioned_by(self) -> exp.PartitionedByProperty:
    self._match(TokenType.EQ)
    return self.expression(
        exp.PartitionedByProperty,
        # 解析 SCHEMA (a, b, c) 或单个字段
        this=self._parse_schema() or self._parse_bracket(self._parse_field()),
    )
```

**解析示例**：

```sql
partitioned_by (col1, col2)
```

解析结果：

```python
exp.PartitionedByProperty(
    this=exp.Schema(
        expressions=[
            exp.Column(this="col1"),
            exp.Column(this="col2")
        ]
    )
)
```

##### `kind` 解析

```python
# 在 dialect.py:603-632
elif key == "kind":
    field = _parse_macro_or_clause(self, lambda: self._parse_id_var(any_token=True))

    if isinstance(field, (MacroVar, MacroFunc)):
        value = field  # 宏变量，不解析
    else:
        # 检查是否是有效的 ModelKind
        kind = ModelKindName[field.name.upper()]

        # 如果有参数，解析参数
        if self._match(TokenType.L_PAREN, advance=False):
            props = self._parse_wrapped_csv(functools.partial(_parse_props, self))
        else:
            props = None

        # 生成 ModelKind 表达式
        value = self.expression(ModelKind, this=kind.value, expressions=props)
```

**解析示例**：

```sql
kind INCREMENTAL_BY_TIME_RANGE(time_column a, batch_size 10)
```

解析结果：

```python
exp.ModelKind(
    this="INCREMENTAL_BY_TIME_RANGE",
    expressions=[
        exp.Property(this="time_column", value=exp.Column("a")),
        exp.Property(this="batch_size", value=exp.Literal(10))
    ]
)
```

##### `physical_properties` 解析

```python
# 在 dialect.py:642
else:  # 默认解析方法
    value = self._parse_bracket(self._parse_field(any_token=True))
```

这会调用 SQLGlot 的通用解析方法，支持：

- `(key = value, key2 = value2)`：解析为 `exp.Tuple`
- `[key = value, key2 = value2]`：解析为 `exp.Array`

**解析示例**：

```sql
physical_properties (
    distributed_by = (kind = 'HASH', expressions = 'col1', buckets = 10),
    primary_key = (order_id, event_date)
)
```

解析结果：

```python
exp.Tuple(
    expressions=[
        exp.EQ(
            this=exp.Column("distributed_by"),
            expression=exp.Tuple(
                expressions=[
                    exp.EQ(this=exp.Column("kind"), expression=exp.Literal("HASH")),
                    exp.EQ(this=exp.Column("expressions"), expression=exp.Literal("col1")),
                    exp.EQ(this=exp.Column("buckets"), expression=exp.Literal(10))
                ]
            )
        ),
        exp.EQ(
            this=exp.Column("primary_key"),
            expression=exp.Tuple(
                expressions=[
                    exp.Column("order_id"),
                    exp.Column("event_date")
                ]
            )
        )
    ]
)
```

### 第四步：配置渲染和提取

**文件**：`sqlmesh/core/model/definition.py`

```python
def load_sql_based_model(
    expressions: t.List[exp.Expression],
    ...
) -> Model:
    """从解析的表达式创建模型对象"""

    # 1. 获取 MODEL 表达式
    meta = expressions[0]
    if not isinstance(meta, d.Model):
        raise_config_error("Missing MODEL block")

    # 2. 渲染宏和变量
    rendered_meta = render_expression(
        expression=meta,
        macros=macros,
        jinja_macros=jinja_macros,
        variables=variables,
        ...
    )

    # 3. 提取配置字段
    meta_fields: t.Dict[str, t.Any] = {
        "dialect": dialect,
        **{prop.name.lower(): prop.args.get("value") for prop in rendered_meta.expressions},
        **kwargs,
    }

    # 4. 提取 name
    name = meta_fields.pop("name", "")
    if not name:
        raise_config_error("Missing 'name' field")

    # 5. 根据 kind 创建模型
    kind = meta_fields.pop("kind", ViewKind())

    if kind.name == ModelKindName.SEED:
        return create_seed_model(name, kind, **meta_fields)
    else:
        return create_sql_model(name, query, kind=kind, **meta_fields)
```

### 第五步：模型对象创建和验证

**文件**：`sqlmesh/core/model/definition.py` + `sqlmesh/core/model/meta.py`

```python
def create_sql_model(
    name: TableName,
    query: t.Optional[exp.Expression],
    **kwargs: t.Any,
) -> Model:
    """创建 SQL 模型"""
    return _create_model(SqlModel, name, query=query, **kwargs)

def _create_model(
    klass: t.Type[Model],
    name: TableName,
    **kwargs: t.Any,
) -> Model:
    """创建模型对象（使用 Pydantic 验证）"""

    # Pydantic 会调用所有的 field_validator
    model = klass(name=name, **kwargs)

    return model
```

#### Pydantic 验证器

**文件**：`sqlmesh/core/model/meta.py`

```python
class ModelMeta(_Node):
    """模型元数据基类"""

    # 字段定义
    name: str
    kind: ModelKind = ViewKind()
    partitioned_by_: t.List[exp.Expression] = Field(default=[], alias="partitioned_by")
    clustered_by: t.List[exp.Expression] = []
    physical_properties_: t.Optional[exp.Tuple] = Field(default=None, alias="physical_properties")
    ...

    # 验证器：partitioned_by 和 clustered_by
    @field_validator("partitioned_by_", "clustered_by", mode="before")
    def _partition_and_cluster_validator(
        cls, v: t.Any, info: ValidationInfo
    ) -> t.List[exp.Expression]:
        """验证分区和聚簇字段"""

        # 如果是 List[str]（从 JSON 反序列化）
        if isinstance(v, list) and all(isinstance(i, str) for i in v):
            # 重新解析为表达式
            string_to_parse = f"({','.join(v)})"
            parsed = parse_one(string_to_parse, into=exp.PartitionedByProperty, ...)
            v = parsed.this.expressions

        # 验证每个表达式只包含一个列
        expressions = list_of_fields_validator(v, info.data)
        for expression in expressions:
            num_cols = len(list(expression.find_all(exp.Column)))
            if num_cols == 0:
                raise ConfigError("does not contain a column")
            elif num_cols > 1:
                raise ConfigError("contains multiple columns")

        return expressions

    # 验证器：columns
    @field_validator("columns_to_types_", mode="before")
    def _columns_validator(cls, v: t.Any, info: ValidationInfo) -> t.Dict[str, exp.DataType]:
        """验证列定义"""

        if isinstance(v, exp.Schema):
            # 从 Schema 表达式提取列定义
            for column in v.expressions:
                expr = column.args.get("kind")
                if not isinstance(expr, exp.DataType):
                    raise ConfigError(f"Missing data type for column '{column.name}'")
                columns_to_types[column.name] = expr

        return columns_to_types

    # 验证器：physical_properties
    @field_validator("physical_properties_", mode="before")
    def _properties_validator(cls, v: t.Any, info: ValidationInfo) -> exp.Tuple:
        """验证物理属性"""
        return parse_properties(cls, v, info)

    # 属性访问器
    @cached_property
    def physical_properties(self) -> t.Dict[str, exp.Expression]:
        """将 Tuple 转换为字典"""
        if self.physical_properties_:
            return {e.this.name: e.expression for e in self.physical_properties_.expressions}
        return {}
```

**parse_properties 实现**（在 `common.py`）：

```python
def parse_properties(
    cls: t.Type, v: t.Any, info: t.Optional[ValidationInfo]
) -> t.Optional[exp.Tuple]:
    """解析属性（physical_properties、virtual_properties 等）"""

    if isinstance(v, str):
        v = d.parse_one(v, dialect=dialect)

    if isinstance(v, (exp.Array, exp.Paren, exp.Tuple)):
        eq_expressions = [v.unnest()] if isinstance(v, exp.Paren) else v.expressions

        # 验证每个表达式都是 EQ
        for eq_expr in eq_expressions:
            if not isinstance(eq_expr, exp.EQ):
                raise ConfigError(f"Invalid property '{eq_expr.sql()}'")

        properties = exp.Tuple(expressions=eq_expressions)
    elif isinstance(v, dict):
        # 支持字典定义
        properties = exp.Tuple(
            expressions=[exp.Literal.string(key).eq(value) for key, value in v.items()]
        )

    return properties
```

### 第六步：查询解析

**文件**：`sqlmesh/core/model/definition.py`

```python
# 在 load_sql_based_model() 中
query_or_seed_insert, pre_statements, post_statements, on_virtual_update, inline_audits = (
    _split_sql_model_statements(expressions[1:], path, dialect=dialect)
)

def _split_sql_model_statements(
    expressions: t.List[exp.Expression],
    path: t.Optional[Path],
    dialect: str = "",
) -> t.Tuple[...]:
    """分离查询、pre/post 语句"""

    pre_statements = []
    post_statements = []
    on_virtual_update = []
    inline_audits = []
    query = None

    for i, expression in enumerate(expressions):
        if isinstance(expression, d.VirtualUpdateStatement):
            on_virtual_update.extend(expression.expressions)
        elif _is_audit_def(expression):
            inline_audits.append(expression)
        elif _is_query(expression):
            query = expression
            # 之后的都是 post_statements
            post_statements = [e for e in expressions[i + 1:] if not _is_audit_def(e)]
            break
        else:
            pre_statements.append(expression)

    return query, pre_statements, post_statements, on_virtual_update, inline_audits
```

---

## 配置参数解析机制

### 解析策略总结

| 参数类型 | 解析方法 | SQLGlot 使用 | 验证器 |
|---------|---------|-------------|--------|
| `name` | `_parse_table_parts()` | ✅ 完全使用 | ❌ 无特殊验证 |
| `kind` | 自定义解析 | ✅ 部分使用（参数解析） | `model_kind_validator` |
| `partitioned_by` | `_parse_partitioned_by()` | ✅ 完全使用 | `_partition_and_cluster_validator` |
| `clustered_by` | `_parse_bracket(_parse_field())` | ✅ 完全使用 | `_partition_and_cluster_validator` |
| `columns` | `_parse_schema()` | ✅ 完全使用 | `_columns_validator` |
| `grain` | `_parse_bracket(_parse_field())` | ✅ 完全使用 | `_refs_validator` |
| `physical_properties` | `_parse_bracket(_parse_field())` | ✅ 完全使用 | `properties_validator` |
| `audits` | `_parse_bracket(_parse_field())` | ✅ 完全使用 | `_func_call_validator` |
| `signals` | `_parse_bracket(_parse_field())` | ✅ 完全使用 | `_func_call_validator` |

### 解析流程对比

#### 1. `partitioned_by` 解析流程

```
partitioned_by (col1, col2)
    ↓
SQLMesh Dialect Parser (dialect.py:635)
    ↓
SQLGlot._parse_partitioned_by()
    ↓
SQLGlot._parse_schema()  # 解析 (col1, col2)
    ↓
exp.PartitionedByProperty(
    this=exp.Schema(
        expressions=[Column("col1"), Column("col2")]
    )
)
    ↓
SQLMesh 转换为 exp.Tuple (dialect.py:638)
    ↓
Pydantic 验证器 (meta.py:190)
    ↓
model.partitioned_by_ = [Column("col1"), Column("col2")]
```

#### 2. `kind` 解析流程

```
kind INCREMENTAL_BY_TIME_RANGE(time_column a, batch_size 10)
    ↓
SQLMesh Dialect Parser (dialect.py:603)
    ↓
_parse_id_var() 识别 INCREMENTAL_BY_TIME_RANGE
    ↓
检查是否是有效的 ModelKindName
    ↓
_parse_wrapped_csv() 解析参数
    ↓
exp.ModelKind(
    this="INCREMENTAL_BY_TIME_RANGE",
    expressions=[
        Property(this="time_column", value=Column("a")),
        Property(this="batch_size", value=Literal(10))
    ]
)
    ↓
Pydantic 验证器 (kind.py)
    ↓
model.kind = IncrementalByTimeRangeKind(time_column="a", batch_size=10)
```

#### 3. `physical_properties` 解析流程

```
physical_properties (
    distributed_by = (kind = 'HASH', expressions = 'col1', buckets = 10)
)
    ↓
SQLMesh Dialect Parser (dialect.py:642)
    ↓
_parse_bracket(_parse_field())
    ↓
SQLGlot 递归解析嵌套结构
    ↓
exp.Tuple(
    expressions=[
        exp.EQ(
            this=Column("distributed_by"),
            expression=Tuple([
                EQ(Column("kind"), Literal("HASH")),
                EQ(Column("expressions"), Literal("col1")),
                EQ(Column("buckets"), Literal(10))
            ])
        )
    ]
)
    ↓
Pydantic 验证器 (meta.py:487)
    ↓
model.physical_properties = {
    "distributed_by": Tuple([...])
}
```

---

## 关键文件说明

### 文件职责表

| 文件路径 | 主要职责 | 关键函数/类 |
|---------|---------|-----------|
| `sqlmesh/core/loader.py` | 文件加载和模型管理 | `load_sql_models()`, `Loader` |
| `sqlmesh/core/dialect.py` | SQL 解析和 DSL 定义 | `parse()`, `_create_parser()`, `PARSERS` |
| `sqlmesh/core/model/definition.py` | 模型对象创建 | `load_sql_based_model()`, `create_sql_model()`, `_split_sql_model_statements()` |
| `sqlmesh/core/model/meta.py` | 模型元数据和验证 | `ModelMeta`, 各种 `field_validator` |
| `sqlmesh/core/model/kind.py` | 模型类型定义 | `ModelKind`, `IncrementalByTimeRangeKind`, etc. |
| `sqlmesh/core/model/common.py` | 通用解析和验证函数 | `parse_properties()`, `parse_bool()`, `parse_expression()` |

### 核心类和函数详解

#### 1. `parse()` (dialect.py:878)

```python
def parse(sql: str, default_dialect: str = None) -> t.List[exp.Expression]:
    """
    解析 SQL 字符串，返回表达式列表

    功能：
    1. 分词（Tokenizer）
    2. 识别 Jinja 模板块
    3. 分块处理（SQL、Jinja Query、Jinja Statement、Virtual Statement）
    4. 调用 Parser 解析每个块

    返回：
    [Model, *Statements, Query, *Statements]
    """
```

#### 2. `_create_parser()` (dialect.py:569)

```python
def _create_parser(expression_type, table_keys) -> Callable:
    """
    创建自定义解析器（MODEL、AUDIT、METRIC）

    功能：
    1. 循环解析每个配置属性
    2. 根据键名选择解析方法
    3. 生成 exp.Property 表达式
    4. 返回 Model/Audit/Metric 表达式
    """
```

#### 3. `load_sql_based_model()` (definition.py:2153)

```python
def load_sql_based_model(expressions, ...) -> Model:
    """
    从解析的表达式创建模型对象

    功能：
    1. 渲染宏和变量
    2. 提取 meta_fields
    3. 分离查询和语句
    4. 创建 SqlModel 或 SeedModel 对象
    """
```

#### 4. `ModelMeta` (meta.py:86)

```python
class ModelMeta(_Node):
    """
    模型元数据基类

    功能：
    1. 定义所有配置字段
    2. 提供 Pydantic 验证器
    3. 提供属性访问器（physical_properties、virtual_properties 等）
    """
```

---

## 示例分析

### 完整示例

```sql
MODEL (
    name mydb.mytable,
    kind INCREMENTAL_BY_TIME_RANGE(
        time_column ds,
        batch_size 100
    ),
    dialect starrocks,
    owner 'data_team',
    partitioned_by (ds, hour),
    clustered_by customer_id,
    grain (order_id, ds),
    columns (
        order_id INT,
        customer_id INT,
        amount DECIMAL(10, 2),
        ds DATE,
        hour INT
    ),
    physical_properties (
        distributed_by = (kind = 'HASH', expressions = 'customer_id', buckets = 10),
        primary_key = (order_id, ds)
    )
);

SELECT
    order_id,
    customer_id,
    amount,
    ds,
    hour
FROM source_orders
WHERE ds >= @start_ds AND ds < @end_ds;
```

### 解析步骤详解

#### 步骤 1：分词

```
[MODEL, (, name, mydb.mytable, ,, kind, INCREMENTAL_BY_TIME_RANGE, (, ...]
```

#### 步骤 2：MODEL 块解析

生成 `exp.Model` 表达式：

```python
exp.Model(
    expressions=[
        exp.Property(this="name", value=exp.Table(this="mytable", db="mydb")),
        exp.Property(this="kind", value=exp.ModelKind(
            this="INCREMENTAL_BY_TIME_RANGE",
            expressions=[
                exp.Property(this="time_column", value=exp.Column("ds")),
                exp.Property(this="batch_size", value=exp.Literal(100))
            ]
        )),
        exp.Property(this="dialect", value=exp.Identifier("starrocks")),
        exp.Property(this="owner", value=exp.Literal("data_team")),
        exp.Property(this="partitioned_by", value=exp.Tuple([
            exp.Column("ds"),
            exp.Column("hour")
        ])),
        exp.Property(this="clustered_by", value=exp.Column("customer_id")),
        exp.Property(this="grain", value=exp.Tuple([
            exp.Column("order_id"),
            exp.Column("ds")
        ])),
        exp.Property(this="columns", value=exp.Schema([
            exp.ColumnDef(this="order_id", kind=exp.DataType(this="INT")),
            exp.ColumnDef(this="customer_id", kind=exp.DataType(this="INT")),
            exp.ColumnDef(this="amount", kind=exp.DataType(this="DECIMAL", expressions=[10, 2])),
            exp.ColumnDef(this="ds", kind=exp.DataType(this="DATE")),
            exp.ColumnDef(this="hour", kind=exp.DataType(this="INT"))
        ])),
        exp.Property(this="physical_properties", value=exp.Tuple([
            exp.EQ(
                this=exp.Column("distributed_by"),
                expression=exp.Tuple([
                    exp.EQ(this=exp.Column("kind"), expression=exp.Literal("HASH")),
                    exp.EQ(this=exp.Column("expressions"), expression=exp.Literal("customer_id")),
                    exp.EQ(this=exp.Column("buckets"), expression=exp.Literal(10))
                ])
            ),
            exp.EQ(
                this=exp.Column("primary_key"),
                expression=exp.Tuple([
                    exp.Column("order_id"),
                    exp.Column("ds")
                ])
            )
        ]))
    ]
)
```

#### 步骤 3：提取 meta_fields

```python
meta_fields = {
    "name": exp.Table(this="mytable", db="mydb"),
    "kind": exp.ModelKind(...),
    "dialect": "starrocks",
    "owner": "data_team",
    "partitioned_by": exp.Tuple([...]),
    "clustered_by": exp.Column("customer_id"),
    "grain": exp.Tuple([...]),
    "columns": exp.Schema([...]),
    "physical_properties": exp.Tuple([...])
}
```

#### 步骤 4：Pydantic 验证

```python
model = SqlModel(
    name="mydb.mytable",
    kind=IncrementalByTimeRangeKind(
        time_column=TimeColumn(column=exp.Column("ds")),
        batch_size=100
    ),
    dialect="starrocks",
    owner="data_team",
    partitioned_by_=[exp.Column("ds"), exp.Column("hour")],
    clustered_by=[exp.Column("customer_id")],
    grains=[exp.Column("order_id"), exp.Column("ds")],
    columns_to_types_={
        "order_id": exp.DataType(this="INT"),
        "customer_id": exp.DataType(this="INT"),
        "amount": exp.DataType(this="DECIMAL", expressions=[10, 2]),
        "ds": exp.DataType(this="DATE"),
        "hour": exp.DataType(this="INT")
    },
    physical_properties_=exp.Tuple([...]),
    query_=ParsableSql(sql="SELECT ...")
)
```

#### 步骤 5：属性访问

```python
# 通过 @cached_property 访问
model.physical_properties
# 返回:
{
    "distributed_by": exp.Tuple([
        exp.EQ(this=exp.Column("kind"), expression=exp.Literal("HASH")),
        exp.EQ(this=exp.Column("expressions"), expression=exp.Literal("customer_id")),
        exp.EQ(this=exp.Column("buckets"), expression=exp.Literal(10))
    ]),
    "primary_key": exp.Tuple([
        exp.Column("order_id"),
        exp.Column("ds")
    ])
}
```

---

## 总结

### 核心要点

1. **SQLMesh 使用 SQLGlot 作为解析引擎**：
   - 复用 SQLGlot 的 Tokenizer 和 Parser
   - 扩展了自定义的 MODEL、AUDIT、METRIC 解析器
   - 支持 Jinja 模板的混合解析

2. **解析流程分层清晰**：
   - 词法分析（Tokenizer）
   - 语法分析（Parser）
   - 配置提取（load_sql_based_model）
   - 对象创建（Pydantic 验证）

3. **配置参数解析策略**：
   - `name`、`partitioned_by`、`columns` 等：完全使用 SQLGlot 的解析方法
   - `kind`：自定义解析逻辑 + SQLGlot 参数解析
   - `physical_properties`、`virtual_properties`：通用解析 + Pydantic 验证

4. **Pydantic 验证器的作用**：
   - 类型转换（Expression → Python 对象）
   - 数据验证（列是否存在、类型是否正确）
   - 提供便捷的属性访问器

5. **查询部分的处理**：
   - 与 MODEL 块分离解析
   - 支持 pre_statements、post_statements、on_virtual_update
   - 延迟渲染（lazy rendering）

### 与 SQLGlot 的关系

| 组件 | SQLMesh | SQLGlot |
|------|---------|---------|
| Tokenizer | ✅ 使用（扩展了 Jinja 识别） | ✅ 提供 |
| Parser | ✅ 使用（扩展了 MODEL、AUDIT、METRIC） | ✅ 提供 |
| Expression | ✅ 使用（添加了 Model、ModelKind 等） | ✅ 提供 |
| Generator | ✅ 使用（扩展了 MODEL 的 SQL 生成） | ✅ 提供 |

### 设计优势

1. **灵活性**：支持宏、Jinja 模板、变量替换
2. **可扩展性**：可以轻松添加新的配置参数
3. **类型安全**：Pydantic 提供强类型检查
4. **性能**：缓存机制（ModelCache、cached_property）
5. **兼容性**：基于 SQLGlot，支持多种 SQL 方言

### 常见问题

**Q1：为什么 `partitioned_by` 存储为 List，而 `physical_properties` 存储为 Dict？**

A：

- `partitioned_by` 需要保持顺序，使用 List
- `physical_properties` 是键值对配置，使用 Dict 方便访问

**Q2：为什么有些配置参数有下划线后缀（如 `partitioned_by_`）？**

A：

- Pydantic 的 `alias` 机制，允许使用更简洁的外部名称（`partitioned_by`）
- 内部字段名加下划线避免与 Python 关键字冲突

**Q3：为什么需要 `render_expression()` 步骤？**

A：

- 支持宏和变量替换（如 `@VAR('my_var')`）
- 某些配置参数可能包含需要在运行时才能解析的表达式

**Q4：如何添加新的配置参数？**

A：

1. 在 `ModelMeta` 中添加字段定义
2. 在 `_create_parser()` 中添加解析逻辑（如果需要特殊解析）
3. 添加 Pydantic 验证器（如果需要验证）
4. 在 `render_definition()` 中添加序列化逻辑（如果需要）

---

## 深度分析：解析流程和 Validator 框架

### 问题 1：解析时机和流程

**Q：在进行 parse 时，dialect 是直接解析还是先通用解析？**

**A：SQLMesh 采用**两阶段解析**策略**：

#### 阶段 1：通用 SQL 解析（dialect.py）

```python
# dialect.py:878
def parse(sql: str, default_dialect: str = None) -> t.List[exp.Expression]:
    # 1. 使用 SQLGlot dialect tokenizer 分词
    tokens = dialect.tokenize(sql)

    # 2. 使用 SQLGlot dialect parser 解析
    parser = dialect.parser()
    expressions = parser.parse(chunk, sql)

    # 返回 [Model, *Statements, Query, *Statements]
```

在这个阶段，`partitioned_by (col1, col2)` 已经被解析为 SQLGlot 表达式树：

```python
exp.Property(
    this="partitioned_by",
    value=exp.Tuple([exp.Column("col1"), exp.Column("col2")])
)
```

#### 阶段 2：Pydantic 模型创建和验证（definition.py + meta.py）

```python
# definition.py:2653
model = klass(
    name=name,
    **{
        "partitioned_by": exp.Tuple([...]),  # 已经是表达式，不是字符串
        ...
    }
)
```

**关键点**：

- 传入 Pydantic 模型构造函数时，`partitioned_by` 已经是 `exp.Tuple`，**不是字符串**
- Pydantic 的 `field_validator` 接收的是 SQLGlot 表达式对象
- validator 的作用是**规范化**和**验证**，而不是初次解析

### 问题 2：dialect 默认行为与 Expression 结构

**Q：如果 dialect 中没有重载 `_parse_partitioned_by()`，会直接默认处理并返回 `PartitionedByProperty` 吗？**

**A：是的，SQLGlot 提供默认实现**.

```python
# sqlglot/parser.py:2741
def _parse_partitioned_by(self) -> exp.PartitionedByProperty:
    self._match(TokenType.EQ)
    return self.expression(
        exp.PartitionedByProperty,
        this=self._parse_schema() or self._parse_bracket(self._parse_field()),
    )
```

**默认行为**：

1. 尝试解析为 `exp.Schema`（列定义格式）
2. 如果失败，尝试解析为括号包裹的字段
3. 返回 `exp.PartitionedByProperty(this=...)`

**Dialect 重载示例**（ClickHouse）：

```python
# sqlglot/dialects/clickhouse.py:984
def _parse_partitioned_by(self) -> exp.PartitionedByProperty:
    # ClickHouse 允许自定义表达式作为分区键
    return self.expression(
        exp.PartitionedByProperty,
        this=self._parse_assignment(),  # 不同的解析方法
    )
```

#### 关于 `PartitionedByProperty` 的特殊结构

**为什么 `PartitionedByProperty` 使用 `this=Schema()` 而不是直接用 `expressions`？**

这是 SQLGlot 的历史设计选择：

```python
# PartitionedByProperty 的 arg_types
arg_types = {"this": True}  # 需要 this 字段，通常是 Schema

# 而 PrimaryKey/UniqueKey 的 arg_types
arg_types = {"expressions": True}  # 直接用 expressions 列表
```

**设计理由**：

1. **支持类型标注**：

   ```sql
   -- PARTITION BY 可以包含类型信息（虽然很少用）
   PARTITION BY (col DATE, region STRING)
   ```

   Schema 可以处理 `ColumnDef`（带类型的列定义）

2. **复用现有解析器**：
   SQLGlot 已有 `_parse_schema()` 处理 `(col1 TYPE, col2 TYPE)` 格式，直接复用

3. **支持复杂表达式**：

   ```sql
   PARTITION BY (DATE_TRUNC('day', event_time))
   ```

   Schema wrapper 更灵活

**不一致性的问题**：

| 属性类型 | Expression | 结构 | 原因 |
|---------|-----------|------|------|
| `partitioned_by` | `PartitionedByProperty` | `this=Schema(expressions=[...])` | 支持表达式和类型 |
| `primary_key` | `PrimaryKeyColumnConstraint` | `expressions=[...]` | 只允许列名 |
| `unique_key` | `UniqueKeyProperty` | `expressions=[...]` | 只允许列名 |
| `clustered_by` | 直接 `List[Column]` | 无 wrapper | SQLMesh 内部处理 |

**这导致**：

- **需要解包**：`partitioned_by.this.expressions`（两层嵌套）
- **不直观**：无法通过类型名推断结构，必须调试或查看源码
- **历史包袱**：不同时期添加的功能使用了不同模式

### 问题 3：SQLMesh 解析时机和 Validator 调用

**Q：SQLMesh 在解析 model 时，就已经把 `partitioned_by` 解析成 `PartitionedByProperty` 了吗？Validator 为什么还要重新解析？**

**A：详细解析流程**：

#### 1. 初次解析（dialect.py:635-640）

```python
elif key == "partitioned_by":
    # 调用 SQLGlot 的 _parse_partitioned_by()
    partitioned_by = self._parse_partitioned_by()

    # 提取内部表达式
    if isinstance(partitioned_by.this, exp.Schema):
        # (col1, col2) -> Tuple
        value = exp.tuple_(*partitioned_by.this.expressions)
    else:
        # col1 -> Column
        value = partitioned_by.this
```

**关键转换**：`PartitionedByProperty` → `Tuple` 或 `Column`

#### 2. Pydantic Validator 调用时机

```python
# meta.py:190
@field_validator("partitioned_by_", "clustered_by", mode="before")
def _partition_and_cluster_validator(
    cls, v: t.Any, info: ValidationInfo
) -> t.List[exp.Expression]:
```

**调用时机**：在 `klass(**kwargs)` 构造函数调用时，Pydantic 自动触发

```python
# definition.py:2653
model = klass(  # 此时触发所有 field_validator
    name=name,
    partitioned_by=exp.Tuple([...]),  # 传入的是表达式
    ...
)
```

#### 3. Validator 的三个职责

**职责 1：处理 JSON 反序列化**:

```python
if (
    isinstance(v, list)
    and all(isinstance(i, str) for i in v)
    and info.field_name == "partitioned_by_"
):
    # 从 JSON 反序列化时：["col1", "col2"] -> List[str]
    # 需要重新解析为表达式
    string_to_parse = f"({','.join(v)})"
    parsed = parse_one(string_to_parse, into=exp.PartitionedByProperty, ...)
    v = parsed.this.expressions
```

**职责 2：规范化表达式**:

```python
expressions = list_of_fields_validator(v, info.data)
# 调用 _get_fields() -> _get_field()
# _get_field() 会：
# 1. 将 Identifier 转换为 Column
# 2. 规范化标识符（normalize_identifiers）
# 3. 添加引号（quote_identifiers）
# 4. 设置 meta["dialect"]
```

**职责 3：验证语法**:

```python
for expression in expressions:
    num_cols = len(list(expression.find_all(exp.Column)))

    if num_cols == 0:
        raise ConfigError("does not contain a column")
    elif num_cols > 1:
        raise ConfigError("contains multiple columns")
```

#### 4. 关于 `RANGE(col1, col2)` 的问题

**Q：如果我写 `partitioned_by RANGE(col1, col2)`，会通过检查吗？**

**A：**不会！**这是当前的设计限制**

```python
# RANGE(col1, col2) 被解析为：
exp.Anonymous(
    this="RANGE",
    expressions=[exp.Column("col1"), exp.Column("col2")]
)

# expression.find_all(exp.Column) 会找到 2 个 Column
# num_cols = 2
# 触发错误："contains multiple columns"
```

**解决方案**：

1. **方案 1**：修改 validator，允许特定函数

```python
for expression in expressions:
    # 如果是 RANGE/HASH 等分区函数，跳过检查
    if isinstance(expression, exp.Anonymous) and expression.name in ("RANGE", "HASH"):
        continue

    num_cols = len(list(expression.find_all(exp.Column)))
    if num_cols == 0 or num_cols > 1:
        raise ConfigError(...)
```

2. **方案 2**：使用 `physical_properties`

```sql
MODEL (
    name db.table,
    physical_properties (
        partition_by = RANGE(col1, col2)
    )
);
```

### 问题 4：通用 Validator 框架设计

**设计目标**：创建通用语法验证器，支持规则配置和归一化

#### 4.1 Validator 框架分析

**Pydantic Validator 执行流程**：

```
用户调用: model = ModelMeta(partitioned_by=value)
    ↓
Pydantic 拦截: __init__(**kwargs)
    ↓
遍历所有字段，查找 @field_validator 装饰的方法
    ↓
mode="before": 在类型转换前执行（接收原始值）
    ↓
执行 validator 函数: _partition_and_cluster_validator(cls, v, info)
    ↓
mode="after": 在类型转换后执行（接收 Pydantic 类型）
    ↓
执行 @model_validator(mode="after"): _root_validator(self)
    ↓
返回模型实例
```

**关键装饰器**：

```python
# pydantic.py:31
def field_validator(*args, **kwargs):
    return pydantic.field_validator(*args, **kwargs)

# 使用示例
@field_validator("field_name", mode="before")
def my_validator(cls, v: Any, info: ValidationInfo) -> Any:
    # v: 传入的原始值
    # info.data: 其他字段的值
    # info.field_name: 当前字段名
    return transformed_value
```

#### 4.2 通用 Validator 设计

```python
from typing import Any, Callable, List, Union, Type
from sqlglot import exp
from pydantic import field_validator, ValidationInfo
from dataclasses import dataclass

@dataclass
class ValidationRule:
    """验证规则配置"""

    # 允许的类型
    allowed_types: List[Type[exp.Expression]]

    # 是否允许多列
    allow_multiple_columns: bool = False

    # 是否必须包含列
    require_column: bool = True

    # 允许的函数名（用于 Anonymous）
    allowed_functions: List[str] = None

    # 规范化函数
    normalizer: Callable[[Any], Any] = None

    # 输出类型（single, list, tuple）
    output_type: str = "list"

class GenericValidator:
    """通用验证器"""

    @staticmethod
    def create_validator(
        field_names: Union[str, List[str]],
        rule: ValidationRule
    ) -> Callable:
        """创建字段验证器"""

        def validator(cls, v: Any, info: ValidationInfo) -> Any:
            # 1. 处理 JSON 反序列化
            if isinstance(v, list) and all(isinstance(i, str) for i in v):
                v = GenericValidator._deserialize_from_json(v, info, rule)

            # 2. 规范化为表达式列表
            expressions = GenericValidator._normalize_to_expressions(v, info, rule)

            # 3. 验证语法
            for expr in expressions:
                GenericValidator._validate_expression(expr, rule)

            # 4. 应用自定义规范化
            if rule.normalizer:
                expressions = [rule.normalizer(e) for e in expressions]

            # 5. 返回指定格式
            return GenericValidator._format_output(expressions, rule)

        return field_validator(*field_names, mode="before")(validator)

    @staticmethod
    def _deserialize_from_json(
        v: List[str],
        info: ValidationInfo,
        rule: ValidationRule
    ) -> Any:
        """从 JSON 字符串列表反序列化"""
        from sqlglot import parse_one

        if rule.output_type == "single":
            return parse_one(v[0], dialect=info.data.get("dialect"))
        else:
            string_to_parse = f"({','.join(v)})"
            parsed = parse_one(
                string_to_parse,
                into=exp.PartitionedByProperty,
                dialect=info.data.get("dialect")
            )
            return parsed.this.expressions if isinstance(parsed.this, exp.Schema) else [parsed.this]

    @staticmethod
    def _normalize_to_expressions(
        v: Any,
        info: ValidationInfo,
        rule: ValidationRule
    ) -> List[exp.Expression]:
        """规范化为表达式列表"""
        from sqlglot import parse_one
        from sqlglot.helper import ensure_list

        dialect = info.data.get("dialect")

        # 处理不同输入类型
        if isinstance(v, (exp.Tuple, exp.Array)):
            expressions = v.expressions
        elif isinstance(v, exp.Expression):
            expressions = [v]
        else:
            expressions = [
                parse_one(entry, dialect=dialect) if isinstance(entry, str) else entry
                for entry in ensure_list(v)
            ]

        # 规范化每个表达式
        results = []
        for expr in expressions:
            # 转换 Identifier -> Column
            if isinstance(expr, exp.Identifier):
                expr = exp.column(expr)

            # 规范化和引用
            from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
            from sqlglot.optimizer.qualify_columns import quote_identifiers

            expr = quote_identifiers(
                normalize_identifiers(expr, dialect=dialect),
                dialect=dialect
            )
            expr.meta["dialect"] = dialect
            results.append(expr)

        return results

    @staticmethod
    def _validate_expression(expr: exp.Expression, rule: ValidationRule) -> None:
        """验证单个表达式"""
        # 检查类型
        if rule.allowed_types and not isinstance(expr, tuple(rule.allowed_types)):
            # 检查是否是允许的函数
            if isinstance(expr, exp.Anonymous):
                if not rule.allowed_functions or expr.name not in rule.allowed_functions:
                    raise ValueError(
                        f"Expression type {type(expr)} not in allowed types {rule.allowed_types}"
                    )
            else:
                raise ValueError(
                    f"Expression type {type(expr)} not in allowed types {rule.allowed_types}"
                )

        # 检查列数量
        columns = list(expr.find_all(exp.Column))
        num_cols = len(columns)

        # 特殊处理：允许的函数跳过列检查
        if isinstance(expr, exp.Anonymous) and rule.allowed_functions and expr.name in rule.allowed_functions:
            return

        if rule.require_column and num_cols == 0:
            raise ValueError(f"Expression '{expr}' does not contain a column")

        if not rule.allow_multiple_columns and num_cols > 1:
            raise ValueError(f"Expression '{expr}' contains multiple columns")

    @staticmethod
    def _format_output(expressions: List[exp.Expression], rule: ValidationRule) -> Any:
        """格式化输出"""
        if rule.output_type == "single":
            return expressions[0] if expressions else None
        elif rule.output_type == "tuple":
            return exp.Tuple(expressions=expressions)
        else:  # list
            return expressions

# 使用示例
class ModelMeta(PydanticModel):
    """模型元数据"""

    # 定义规则
    PARTITIONED_BY_RULE = ValidationRule(
        allowed_types=[exp.Column, exp.Anonymous],
        allow_multiple_columns=False,
        require_column=True,
        allowed_functions=["RANGE", "HASH", "TRUNCATE"],  # StarRocks 分区函数
        output_type="list"
    )

    GRAIN_RULE = ValidationRule(
        allowed_types=[exp.Column],
        allow_multiple_columns=False,
        require_column=True,
        output_type="list"
    )

    # 应用验证器
    _partition_validator = GenericValidator.create_validator(
        ["partitioned_by_", "clustered_by"],
        PARTITIONED_BY_RULE
    )

    _grain_validator = GenericValidator.create_validator(
        "grains",
        GRAIN_RULE
    )
```

#### 4.3 高级用法：条件验证

```python
class ConditionalValidationRule(ValidationRule):
    """条件验证规则"""

    # 条件函数
    condition: Callable[[Any, ValidationInfo], bool] = None

    # 条件为真时的规则
    true_rule: ValidationRule = None

    # 条件为假时的规则
    false_rule: ValidationRule = None

# 示例：根据 dialect 选择不同规则
STARROCKS_PARTITION_RULE = ValidationRule(
    allowed_functions=["RANGE", "HASH"],
    allow_multiple_columns=True  # RANGE(col1, col2) 允许多列
)

DEFAULT_PARTITION_RULE = ValidationRule(
    allowed_types=[exp.Column],
    allow_multiple_columns=False
)

rule = ConditionalValidationRule(
    condition=lambda v, info: info.data.get("dialect") == "starrocks",
    true_rule=STARROCKS_PARTITION_RULE,
    false_rule=DEFAULT_PARTITION_RULE
)
```

#### 4.4 实际应用：支持 StarRocks 的 `RANGE(col1, col2)`

```python
# 修改 ModelMeta
class ModelMeta(_Node):

    @field_validator("partitioned_by_", mode="before")
    def _partition_validator(cls, v: Any, info: ValidationInfo) -> List[exp.Expression]:
        # 获取 dialect
        dialect = info.data.get("dialect", "")

        # StarRocks 特殊处理
        if dialect == "starrocks":
            rule = ValidationRule(
                allowed_types=[exp.Column, exp.Anonymous],
                allowed_functions=["RANGE", "HASH"],
                allow_multiple_columns=True,  # RANGE 内允许多列
                output_type="list"
            )
        else:
            rule = ValidationRule(
                allowed_types=[exp.Column],
                allow_multiple_columns=False,
                output_type="list"
            )

        validator = GenericValidator.create_validator(["partitioned_by_"], rule)
        return validator(cls, v, info)
```

### 总结：Validator 框架的价值

1. **统一验证逻辑**：避免重复代码
2. **灵活配置**：通过规则对象配置验证行为
3. **可扩展**：支持自定义规范化函数
4. **类型安全**：利用 Pydantic 的类型系统
5. **方言感知**：根据 SQL 方言调整验证规则

---

## 附录：SQLGlot Expression 结构速查

### SQLGlot Expression 通用属性

每个 Expression 对象有以下标准属性：

| 属性 | 用途 | 示例 |
|------|------|------|
| `this` | 主操作对象/左操作数 | 二元操作的左侧，函数的第一参数 |
| `expression` | 次要对象/右操作数 | 二元操作的右侧 |
| `expressions` | 多个子表达式列表 | SELECT 的列，函数的剩余参数 |
| 自定义字段 | 特定类型专用 | `buckets`, `unit`, `kind` 等 |

**重要**：不同 Expression 类型对这些属性的使用方式**不一致**，必须通过调试或查看源码才能确定具体结构！

### 常用 Expression 类型的 this/expression/expressions 使用方式

| Expression 类型 | `this` | `expression` | `expressions` | 示例/注释 |
|----------------|--------|--------------|---------------|----------|
| **二元操作** | | | | |
| `EQ`, `GT`, `LT` | 左操作数 | 右操作数 | - | `a = b` → `EQ(this=a, expression=b)` |
| `And`, `Or` | 左操作数 | 右操作数 | - | `a AND b` → `And(this=a, expression=b)` |
| **函数** | | | | |
| 普通函数 | 第一参数 | - | 剩余参数 | `COALESCE(a, b)` → `this=a, expressions=[b]` |
| `Anonymous` | **函数名（字符串！）** | - | 所有参数 | `RANGE(dt)` → `Anonymous(this='RANGE', expressions=[dt])` |
| **容器** | | | | |
| `Tuple` | - | - | 元素列表 | `(a, b)` → `Tuple(expressions=[a, b])` |
| `Schema` | 表（可选） | - | 列定义 | `(col1 INT, col2 STRING)` |
| **DDL 属性** | | | | |
| `UniqueKeyProperty` | - | - | 列列表 | `UNIQUE KEY(a, b)` |
| `PartitionedByProperty` | **Schema 包裹！** | - | - | `this=Schema(expressions=[...])` |
| `Property` | 属性名（字符串） | 属性值 | - | `key = value` |
| **查询语句** | | | | |
| `Select` | **FROM 子句！** | - | SELECT 列 | `this` 是 FROM，不是 SELECT 列！ |
| `Create` | Schema/Table | CTAS 查询 | - | `CREATE TABLE ... AS SELECT` |

### 关键发现

1. **`exp.Anonymous`**：`this` 是函数名（字符串），不是参数！

   ```python
   exp.Anonymous(this="RANGE", expressions=[exp.Column("dt")])
   # SQL: RANGE(dt)
   ```

2. **`exp.PartitionedByProperty`**：`this` 是 `Schema` wrapper，需要解包

   ```python
   exp.PartitionedByProperty(
       this=exp.Schema(expressions=[exp.Column("dt")])
   )
   # 访问列：partitioned_by.this.expressions
   ```

3. **`exp.Select`**：`this` 是 `FROM` 子句，`expressions` 是 SELECT 列

   ```python
   exp.Select(
       this=exp.From(this=exp.Table("t")),  # FROM
       expressions=[exp.Column("a")]        # SELECT
   )
   # SQL: SELECT a FROM t
   ```

4. **`exp.Property`**：`this` 是属性名（字符串），`expression` 是值

   ```python
   exp.Property(this="key", expression=exp.Literal("value"))
   # 用于 MODEL 定义中的 key-value 对
   ```

5. **容器类型**：只有 `expressions`，没有 `this`

   ```python
   exp.Tuple(expressions=[...])
   exp.Array(expressions=[...])
   exp.Properties(expressions=[...])  # 包含多个 Property
   ```

### 调试技巧

```python
# 1. 查看 Expression 的参数定义
print(exp.PartitionedByProperty.arg_types)
# {'this': True}

print(exp.UniqueKeyProperty.arg_types)
# {'expressions': True}

# 2. 打印实际结构
from sqlglot import parse_one
parsed = parse_one("PARTITIONED BY (col1, col2)", into=exp.PartitionedByProperty)
print(parsed)
# PartitionedByProperty(this=Schema(expressions=[Column(...)]))

# 3. 查看树结构
for node in parsed.walk():
    print(f"{type(node).__name__}: {node}")

# 4. 查看 Generator 源码
from sqlglot.dialects import starrocks
gen = starrocks.StarRocks.Generator()
print(gen.partitionedbyproperty_sql.__doc__)
```

---

## 参考资料

- [SQLGlot 文档](https://github.com/tobymao/sqlglot)
- [SQLMesh 文档](https://sqlmesh.readthedocs.io/)
- [Pydantic 文档](https://docs.pydantic.dev/)

---

**文档版本**：1.0
**生成时间**：2025-01-25
**适用 SQLMesh 版本**：0.2.17+
