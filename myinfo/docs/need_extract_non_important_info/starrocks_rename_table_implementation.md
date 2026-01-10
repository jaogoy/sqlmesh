# StarRocks rename_table Implementation

## 问题背景

StarRocks 的 `ALTER TABLE RENAME` 语法有两个关键特性：

1. **不使用 `TO` 关键字**：
   - **标准 SQL**: `ALTER TABLE old_name RENAME TO new_name`
   - **StarRocks**: `ALTER TABLE old_name RENAME new_name`
   - **Doris**: `ALTER TABLE old_name RENAME new_name`

2. **RENAME 子句只接受标识符**（⚠️ 关键约束）：
   - **错误**: `ALTER TABLE db.old_table RENAME db.new_table`
   - **正确**: `ALTER TABLE db.old_table RENAME new_table`
   - 新表名**不能**包含数据库限定符
   - 新表继承旧表的数据库

## 问题分析

1. **基类实现** ([base.py#L3021-L3026](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/sqlmesh/core/engine_adapter/base.py#L3021-L3026)):
   ```python
   def _rename_table(self, old_table_name: TableName, new_table_name: TableName) -> None:
       self.execute(exp.rename_table(old_table_name, new_table_name))
   ```
   - 使用 SQLGlot 的 `exp.rename_table()` 生成 SQL

2. **SQLGlot 问题**:
   - SQLGlot StarRocks dialect 生成的是: `ALTER TABLE ... RENAME TO ...` (错误 - 多了 `TO`)
   - SQLGlot Doris dialect 生成的是: `ALTER TABLE ... RENAME ...` (正确)
   - **关键约束**: SQLGlot 要求 RENAME 子句只接受标识符，不接受限定名

3. **解决方案**:
   - 在 StarRocks adapter 中重写 `_rename_table()` 方法
   - 手动生成正确的 SQL 语句
   - **从新表名中提取纯表名**（去除数据库限定符）

## 实现方案

### 1. 代码实现

在 `starrocks.py` 中添加：

```python
def _rename_table(
    self,
    old_table_name: TableName,
    new_table_name: TableName,
) -> None:
    """
    Rename a table.

    StarRocks syntax: ALTER TABLE [db.]old_name RENAME new_name

    Key constraint: The RENAME clause only accepts a table identifier,
    NOT a qualified name (db.table). The database is inferred from the
    old table name.

    Examples:
        - ALTER TABLE `old_table` RENAME `new_table`
        - ALTER TABLE `db`.`old_table` RENAME `new_table`  (NOT `db`.`new_table`)

    Args:
        old_table_name: Current table name (may be qualified with database)
        new_table_name: New table name (may be qualified, but only name part is used)
    """
    old_table = exp.to_table(old_table_name)
    new_table = exp.to_table(new_table_name)

    # Generate SQL for old table (with database qualifier if present)
    old_table_sql = old_table.sql(dialect=self.dialect, identify=True)

    # Extract ONLY the table name from new_table (no database qualifier)
    # StarRocks RENAME clause requires just an identifier, not a qualified name
    new_table_name_only = exp.to_identifier(new_table.name).sql(
        dialect=self.dialect, identify=True
    )

    # StarRocks uses RENAME without TO, and new name must be unqualified
    self.execute(f"ALTER TABLE {old_table_sql} RENAME {new_table_name_only}")
```

**关键点**：
- 使用 `new_table.name` 提取纯表名（去除数据库限定符）
- 即使 `new_table_name` 是 `db.table` 格式，也只使用 `table` 部分
- 新表会继承旧表的数据库

### 2. 测试更新

在 `test_starrocks.py` 中更新测试：

```python
def test_rename_table(self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]):
    """Test RENAME TABLE statement."""
    adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

    # Test 1: Simple table names (no database qualifier)
    adapter.rename_table("old_table", "new_table")
    adapter.cursor.execute.assert_called_with(
        "ALTER TABLE `old_table` RENAME `new_table`"
    )

    # Test 2: Database-qualified names - RENAME only uses table name
    adapter.cursor.execute.reset_mock()
    adapter.rename_table("db.old_table", "db.new_table")
    # StarRocks RENAME clause requires unqualified table name
    adapter.cursor.execute.assert_called_with(
        "ALTER TABLE `db`.`old_table` RENAME `new_table`"
    )
```

**关键测试场景**：
- 简单表名：`old_table` → `new_table`
- 限定表名：`db.old_table` → `db.new_table`
  - **注意**: SQL 生成为 `ALTER TABLE \`db\`.\`old_table\` RENAME \`new_table\``
  - `new_table` **没有** `db.` 前缀

## 测试验证

### 单元测试
```bash
pytest tests/core/engine_adapter/test_starrocks.py::TestStarRocksAdapter::test_rename_table -v
```

### 集成测试
```bash
pytest tests/core/engine_adapter/integration/test_integration_starrocks.py::TestIntegrationStarRocks::test_rename_table -v
```

## 生成的 SQL 示例

### 简单表名
```sql
ALTER TABLE `old_table` RENAME `new_table`
```

### 带 Schema 的表名
```sql
-- 正确: RENAME 子句只使用表名，不包含数据库
ALTER TABLE `db`.`old_table` RENAME `new_table`

-- 错误: RENAME 子句不能包含数据库限定符 (会报错!)
ALTER TABLE `db`.`old_table` RENAME `db`.`new_table`
```

### 重要行为
```sql
-- 即使传入 "db.new_table"，也会自动提取为 "new_table"
rename_table("db.old_table", "db.new_table")
↓
ALTER TABLE `db`.`old_table` RENAME `new_table`
(新表位于 db 数据库，继承自 old_table)

-- 跨数据库重命名不支持！
rename_table("db1.old_table", "db2.new_table")
↓
ALTER TABLE `db1`.`old_table` RENAME `new_table`
(仍然在 db1 中，不是 db2！)
```

## 参考实现

1. **Doris Adapter**: 不需要重写，因为 SQLGlot Doris dialect 已正确处理
2. **ClickHouse Adapter** ([clickhouse.py#L588-L596](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/sqlmesh/core/engine_adapter/clickhouse.py#L588-L596)):
   ```python
   def _rename_table(self, old_table_name: TableName, new_table_name: TableName) -> None:
       old_table_sql = exp.to_table(old_table_name).sql(dialect=self.dialect, identify=True)
       new_table_sql = exp.to_table(new_table_name).sql(dialect=self.dialect, identify=True)
       self.execute(f"RENAME TABLE {old_table_sql} TO {new_table_sql}{self._on_cluster_sql()}")
   ```

## 关键点总结

1. ✅ **需要重写 `_rename_table()`**: 是的，因为 SQLGlot StarRocks dialect 生成的语法不正确
2. ✅ **语法差异**: StarRocks 和 Doris 都使用 `RENAME` 而非 `RENAME TO`
3. ⚠️ **关键约束**: RENAME 子句**只接受标识符**，不接受限定名
4. ✅ **实现方式**:
   - 手动拼接 SQL 字符串，而不是依赖 SQLGlot
   - 使用 `new_table.name` 提取纯表名
5. ✅ **标识符引用**: 使用 `identify=True` 确保表名被正确引用（反引号）
6. ✅ **Schema 支持**:
   - 自动处理带 schema 的表名（如 `db.table`）
   - 但 RENAME 部分只使用表名，不包含数据库
7. ⚠️ **不支持跨库重命名**: 新表会继承旧表的数据库

## 未来改进

如果需要长期支持，可以考虑：
1. 向 SQLGlot 提交 PR，修复 StarRocks dialect 的 `ALTER TABLE RENAME` 语法
2. 在 SQLGlot StarRocks Generator 中添加 `altertable_sql()` 方法来自定义 RENAME 语法

## 相关文件

- **实现**: [`sqlmesh/core/engine_adapter/starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/sqlmesh/core/engine_adapter/starrocks.py#L2897-L2918)
- **单元测试**: [`tests/core/engine_adapter/test_starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/tests/core/engine_adapter/test_starrocks.py#L114-L121)
- **集成测试**: [`tests/core/engine_adapter/integration/test_integration_starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/tests/core/engine_adapter/integration/test_integration_starrocks.py#L260-L302)
