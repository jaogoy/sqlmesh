# StarRocks RENAME TABLE 关键约束修复

## 问题描述

在 SQLGlot 中为 StarRocks 支持 `ALTER TABLE RENAME` 时发现：

**StarRocks RENAME 子句只接受标识符（表名），不能接受限定名（database.table）**

### 错误语法 ❌
```sql
ALTER TABLE `db`.`old_table` RENAME `db`.`new_table`
-- 错误: RENAME 后面不能有 database qualifier
```

### 正确语法 ✅
```sql
ALTER TABLE `db`.`old_table` RENAME `new_table`
-- 正确: RENAME 只使用表名，新表继承旧表的数据库
```

## 根本原因

这是 **StarRocks 的语法限制**，而非 SQLMesh 的问题：

1. **StarRocks 设计**：`RENAME` 子句设计为只接受简单标识符
2. **继承机制**：新表自动继承 `ALTER TABLE` 语句中指定的数据库
3. **不支持跨库重命名**：无法通过 RENAME 将表移动到不同数据库

## 解决方案

### 代码修改

在 [`starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/sqlmesh/core/engine_adapter/starrocks.py#L2897-L2933) 中：

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
    NOT a qualified name (db.table).
    """
    old_table = exp.to_table(old_table_name)
    new_table = exp.to_table(new_table_name)

    # Old table: keep database qualifier if present
    old_table_sql = old_table.sql(dialect=self.dialect, identify=True)

    # New table: ONLY extract the table name (strip database)
    new_table_name_only = exp.to_identifier(new_table.name).sql(
        dialect=self.dialect, identify=True
    )

    # Generate: ALTER TABLE [db.]old_name RENAME new_name
    self.execute(f"ALTER TABLE {old_table_sql} RENAME {new_table_name_only}")
```

### 关键实现点

1. **旧表名**：保留完整的数据库限定符
   - `db.old_table` → `\`db\`.\`old_table\``

2. **新表名**：只提取表名部分
   - `db.new_table` → `\`new_table\``（去除 `db.`）
   - 使用 `new_table.name` 属性获取纯表名

3. **标识符引用**：使用 `identify=True` 确保反引号正确

## 测试覆盖

### 单元测试场景

在 [`test_starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/tests/core/engine_adapter/test_starrocks.py#L114-L130) 中：

```python
def test_rename_table(self, make_mocked_engine_adapter):
    adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

    # Test 1: Simple names
    adapter.rename_table("old_table", "new_table")
    # → ALTER TABLE `old_table` RENAME `new_table`

    # Test 2: Qualified names (key test!)
    adapter.rename_table("db.old_table", "db.new_table")
    # → ALTER TABLE `db`.`old_table` RENAME `new_table`
    #   注意: new_table 没有 `db.` 前缀
```

### 集成测试

集成测试在 [`test_integration_starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/tests/core/engine_adapter/integration/test_integration_starrocks.py#L260-L302) 中验证实际数据库行为。

## 行为示例

### 场景 1：简单表名
```python
adapter.rename_table("old_table", "new_table")
```
生成 SQL：
```sql
ALTER TABLE `old_table` RENAME `new_table`
```

### 场景 2：限定表名（同一数据库）
```python
adapter.rename_table("mydb.old_table", "mydb.new_table")
```
生成 SQL：
```sql
ALTER TABLE `mydb`.`old_table` RENAME `new_table`
```
结果：表被重命名为 `mydb.new_table` ✅

### 场景 3：限定表名（不同数据库）⚠️
```python
adapter.rename_table("db1.old_table", "db2.new_table")
```
生成 SQL：
```sql
ALTER TABLE `db1`.`old_table` RENAME `new_table`
```
结果：表被重命名为 `db1.new_table`（**不是** `db2.new_table`）❌

**警告**：这会导致意外行为！新表仍在 `db1` 中，不会移动到 `db2`。

## 最佳实践建议

### 推荐做法 ✅

1. **同库重命名**：确保新旧表在同一数据库
   ```python
   adapter.rename_table("mydb.old_table", "mydb.new_table")  # ✅ 安全
   adapter.rename_table("old_table", "new_table")             # ✅ 安全
   ```

2. **跨库移动**：使用两步操作
   ```python
   # 不要这样做：
   adapter.rename_table("db1.table", "db2.table")  # ❌ 不会移动到 db2

   # 应该这样做：
   # Step 1: CREATE TABLE db2.table AS SELECT * FROM db1.table
   # Step 2: DROP TABLE db1.table
   ```

### 避免的陷阱 ❌

1. **假设跨库重命名**：
   ```python
   # 错误假设：这会把表移到 db2
   adapter.rename_table("db1.old_table", "db2.new_table")
   # 实际结果：表仍在 db1 中！
   ```

2. **混合使用限定名**：
   ```python
   # 避免混淆，保持一致
   adapter.rename_table("db.old_table", "new_table")  # ⚠️ 混淆
   adapter.rename_table("db.old_table", "db.new_table")  # ✅ 清晰
   ```

## 与其他数据库对比

| 数据库 | RENAME 语法 | 支持跨库重命名 |
|--------|-------------|----------------|
| **StarRocks** | `ALTER TABLE [db.]old RENAME new` | ❌ 否 |
| **Doris** | `ALTER TABLE [db.]old RENAME new` | ❌ 否 |
| **MySQL** | `ALTER TABLE old RENAME TO new`<br>`RENAME TABLE old TO new` | ❌ 否（需要两步） |
| **PostgreSQL** | `ALTER TABLE old RENAME TO new`<br>`ALTER TABLE old SET SCHEMA new_schema` | ✅ 是（SET SCHEMA） |
| **ClickHouse** | `RENAME TABLE old TO new` | ❌ 否 |

## 相关文档

- **实现**: [`sqlmesh/core/engine_adapter/starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/sqlmesh/core/engine_adapter/starrocks.py#L2897-L2933)
- **单元测试**: [`tests/core/engine_adapter/test_starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/tests/core/engine_adapter/test_starrocks.py#L114-L130)
- **集成测试**: [`tests/core/engine_adapter/integration/test_integration_starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/tests/core/engine_adapter/integration/test_integration_starrocks.py#L260-L302)
- **详细文档**: [`docs/starrocks_rename_table_implementation.md`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/docs/starrocks_rename_table_implementation.md)

## 总结

✅ **问题已解决**：通过从新表名中提取纯表名，符合 StarRocks 的语法要求

⚠️ **使用注意**：
- StarRocks RENAME 不支持跨数据库重命名
- 新表总是继承旧表的数据库
- 建议在应用层检查新旧表的数据库是否一致

🔧 **实现亮点**：
- 自动处理限定名和非限定名
- 保持 API 简洁（用户无需关心底层细节）
- 充分测试覆盖（单元测试 + 集成测试）
