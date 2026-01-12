#!/usr/bin/env python3
"""
测试 SQLMesh MODEL 解析流程：grain 如何处理，以及何时生成 exp.PrimaryKey()
"""

import sys
from pathlib import Path

# 添加 sqlmesh 到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlmesh.core import dialect as d
from sqlmesh.core.model.definition import load_sql_based_model
from sqlglot import exp


def test_grain_parsing():
    """测试 grain 属性如何被解析"""
    print("=" * 80)
    print("1. 测试 grain 属性解析")
    print("=" * 80)

    model_sql = """
    MODEL (
        name mytest.test_grain,
        kind FULL,
        grain (order_id, event_date)
    );

    SELECT order_id, event_date FROM source_table
    """

    # Step 1: 解析 MODEL 语句（生成 AST）
    expressions = d.parse(model_sql, default_dialect="starrocks")
    model_ast = expressions[0]

    print(f"\n📋 MODEL AST 类型: {type(model_ast)}")
    print(f"   AST 节点: {model_ast}")

    # 检查 grain 属性
    for prop in model_ast.expressions:
        if hasattr(prop, 'name') and prop.name == 'grain':
            print(f"\n✅ 找到 grain 属性:")
            print(f"   属性值: {prop.args.get('value')}")
            print(f"   值类型: {type(prop.args.get('value'))}")

            # 如果是 Tuple，打印其中的表达式
            grain_value = prop.args.get('value')
            if isinstance(grain_value, exp.Tuple):
                print(f"   Tuple 内容: {[e.sql() for e in grain_value.expressions]}")

    # Step 2: 加载为 Model 对象
    model = load_sql_based_model(expressions)

    print(f"\n📦 Model 对象信息:")
    print(f"   名称: {model.name}")
    print(f"   grain 字段: {getattr(model, 'grain', 'NOT_FOUND')}")
    print(f"   grains 字段: {model.grains}")
    print(f"   grains 类型: {[type(g) for g in model.grains]}")
    print(f"   grains SQL: {[g.sql(dialect='starrocks') for g in model.grains]}")

    return model


def test_physical_properties_primary_key():
    """测试在 physical_properties 中设置 primary_key"""
    print("\n" + "=" * 80)
    print("2. 测试 physical_properties 中的 primary_key")
    print("=" * 80)

    model_sql = """
    MODEL (
        name mytest.test_physical_pk,
        kind FULL,
        grain (order_id, event_date),
        physical_properties (
            primary_key (order_id, event_date),
            distributed_by (kind = 'HASH', expressions = 'order_id', buckets = 10)
        )
    );

    SELECT order_id, event_date FROM source_table
    """

    expressions = d.parse(model_sql, default_dialect="starrocks")
    model = load_sql_based_model(expressions)

    print(f"\n📦 Model 物理属性:")
    print(f"   physical_properties: {model.physical_properties}")

    # 检查是否包含 primary_key
    if model.physical_properties:
        for eq_expr in model.physical_properties.expressions:
            prop_name = eq_expr.left.name if hasattr(eq_expr.left, 'name') else str(eq_expr.left)
            print(f"\n   属性: {prop_name}")
            print(f"   值: {eq_expr.expression}")
            print(f"   值类型: {type(eq_expr.expression)}")

    return model


def test_create_table_sql_generation():
    """测试生成的 CREATE TABLE SQL"""
    print("\n" + "=" * 80)
    print("3. 测试 CREATE TABLE SQL 生成")
    print("=" * 80)

    from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
    from sqlmesh.core.config import DuckDBConnectionConfig
    from unittest.mock import MagicMock

    # 创建 mock adapter（不连接真实数据库）
    connection_mock = MagicMock()
    adapter = StarRocksEngineAdapter(lambda: connection_mock, "starrocks")

    # 测试 1: 只有 grain（预期：DUPLICATE KEY）
    print("\n🔧 场景 1: 只有 grain，无 primary_key")
    print("-" * 60)

    model_sql_1 = """
    MODEL (
        name mytest.test_only_grain,
        kind FULL,
        grain (order_id, event_date),
        physical_properties (
            distributed_by (kind = 'HASH', expressions = 'order_id', buckets = 10)
        )
    );

    SELECT order_id, event_date FROM source_table
    """

    expressions_1 = d.parse(model_sql_1, default_dialect="starrocks")
    model_1 = load_sql_based_model(expressions_1)

    # 模拟 create_table 调用
    print(f"   调用参数:")
    print(f"     - grains: {[g.sql() for g in model_1.grains]}")
    print(f"     - physical_properties: {model_1.physical_properties}")
    print(f"   ❌ 没有 primary_key 参数被传递！")

    # 测试 2: grain + physical_properties.primary_key
    print("\n🔧 场景 2: grain + physical_properties.primary_key")
    print("-" * 60)

    model_sql_2 = """
    MODEL (
        name mytest.test_with_pk,
        kind FULL,
        grain (order_id, event_date),
        physical_properties (
            primary_key (order_id, event_date),
            distributed_by (kind = 'HASH', expressions = 'order_id', buckets = 10)
        )
    );

    SELECT order_id, event_date FROM source_table
    """

    expressions_2 = d.parse(model_sql_2, default_dialect="starrocks")
    model_2 = load_sql_based_model(expressions_2)

    print(f"   调用参数:")
    print(f"     - grains: {[g.sql() for g in model_2.grains]}")
    print(f"     - physical_properties:")

    if model_2.physical_properties:
        for eq_expr in model_2.physical_properties.expressions:
            prop_name = eq_expr.left.name if hasattr(eq_expr.left, 'name') else str(eq_expr.left)
            print(f"       * {prop_name}: {eq_expr.expression}")

    print(f"\n   ✅ primary_key 在 physical_properties 中!")

    # 测试生成的表属性表达式
    print("\n🔍 StarRocks Adapter 如何处理 physical_properties:")
    print("-" * 60)

    # 这里需要查看 _build_table_properties_exp 的处理
    if model_2.physical_properties:
        props_exp = adapter._build_table_properties_exp(
            table_properties=model_2.physical_properties
        )
        if props_exp:
            print(f"   生成的 Properties 表达式:")
            print(f"   {props_exp}")
            print(f"\n   SQL 输出:")
            print(f"   {props_exp.sql(dialect='starrocks')}")


def main():
    """主测试函数"""
    print("\n" + "🔬 SQLMesh MODEL 解析流程测试" + "\n")

    # 测试 1: grain 解析
    model1 = test_grain_parsing()

    # 测试 2: physical_properties.primary_key
    model2 = test_physical_properties_primary_key()

    # 测试 3: CREATE TABLE SQL 生成
    test_create_table_sql_generation()

    # 总结
    print("\n" + "=" * 80)
    print("📊 总结")
    print("=" * 80)
    print("""
✅ grain 属性解析流程:
   1. d.parse() 解析 MODEL 语句 → exp.Property(this='grain', value=exp.Tuple(...))
   2. load_sql_based_model() → ModelMeta 对象
   3. _pre_root_validator() 将 grain 转换为 grains 列表
   4. grains 仅用于 SQLMesh 内部逻辑（table_diff、metrics join等）

❌ grain 不会自动生成 PRIMARY KEY:
   - snapshot/evaluator.py 调用 adapter.create_table() 时
   - 只传递了 partitioned_by、clustered_by、table_properties
   - 没有传递 primary_key 参数
   - 所以 base adapter 不会生成 exp.PrimaryKey() 节点

✅ 生成 PRIMARY KEY 的正确方法:
   METHOD 1: 在 physical_properties 中显式设置
   ```sql
   physical_properties (
       primary_key (order_id, event_date),
       distributed_by (...)
   )
   ```

   METHOD 2: 修改 StarRocks Adapter（需要改代码）
   - 在 _build_table_properties_exp() 中
   - 从 model.grains 提取并生成 exp.PrimaryKey()
   - 或者修改 snapshot/evaluator.py 传递 primary_key 参数

📝 相关文件:
   - MODEL 解析: sqlmesh/core/dialect.py:L654 (_create_parser)
   - Model 加载: sqlmesh/core/model/definition.py:L2152 (load_sql_based_model)
   - grain→grains: sqlmesh/core/model/meta.py:L377 (_pre_root_validator)
   - CREATE TABLE: sqlmesh/core/snapshot/evaluator.py:L2062
   - PrimaryKey 生成: sqlmesh/core/engine_adapter/base.py:L771
""")


if __name__ == "__main__":
    main()
