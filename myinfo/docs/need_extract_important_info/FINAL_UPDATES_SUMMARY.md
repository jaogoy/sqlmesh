# Final Updates Summary

## ✅ 完成总结

根据您的要求完成了所有修改：

## 1. ✅ normalized_type 参数默认为 None

### 修改的类型

#### LiteralType
```python
def __init__(self, normalized_type: t.Optional[str] = None):
    """
    Args:
        normalized_type: Target type for normalization.
            - None: Keep as exp.Literal (default)
            - "literal": Keep as exp.Literal
            - "str": Convert to Python string
    """
```

#### IdentifierType
```python
def __init__(self, normalized_type: t.Optional[str] = None):
    """
    Args:
        normalized_type: Target type for normalization.
            - None: Keep as exp.Identifier (default)
            - "identifier": Keep as exp.Identifier
            - "column": Convert to exp.Column
            - "literal": Convert to exp.Literal.string()
            - "str": Convert to Python string
    """
```

#### ColumnType
```python
def __init__(self, normalized_type: t.Optional[str] = None):
    """
    Args:
        normalized_type: Target type for normalization.
            - None: Keep as exp.Column (default)
            - "column": Keep as exp.Column
            - "identifier": Convert to exp.Identifier
            - "literal": Convert to exp.Literal.string()
            - "str": Convert to Python string
    """
```

#### EnumType
```python
def __init__(
    self, 
    valid_values: t.Sequence[str], 
    normalized_type: t.Optional[str] = None,  # Changed from "str"
    case_sensitive: bool = False
):
    """
    Args:
        normalized_type: Target type for normalization.
            - None: Keep as Python string (default)
            - "str": Python string
            - "identifier": exp.Identifier
            - "literal": exp.Literal.string()
            - "column": exp.Column
    """
```

### 设计理念

**None 表示不转换** - 保持原有类型，只有明确指定时才进行类型转换：
- `None` → 保持验证后的类型（默认行为）
- `"str"` → 转换为 Python 字符串
- `"column"` → 转换为 exp.Column
- `"identifier"` → 转换为 exp.Identifier
- `"literal"` → 转换为 exp.Literal

## 2. ✅ DistributionSpec → DistributedBySpec

**重命名原因**: 保持与属性名 `distributed_by` 和命名模式 `PartitionedBySpec` 一致。

```python
# Before
DistributionSpec = AnyOf(
    DistributionTupleType(),
    EnumType(["RANDOM"]),
    FuncType(),
)
PROPERTY_INPUT_SPEC = {
    "distributed_by": DistributionSpec,
}

# After
DistributedBySpec = AnyOf(
    DistributionTupleType(),
    EnumType(["RANDOM"]),
    FuncType(),
)
PROPERTY_INPUT_SPEC = {
    "distributed_by": DistributedBySpec,
}
```

## 3. ✅ PROPERTY_OUTPUT_SPEC 重新设计

### Before (文档性质的字符串描述)

```python
PROPERTY_OUTPUT_SPEC: t.Dict[str, str] = {
    "partitioned_by": "list_of_exprs",
    "distributed_by": "expr_or_list",
    "order_by": "list_of_exprs",
    # ... more string descriptions
}
```

**问题**: 
- 只是文档，无法用于实际验证
- 无法确保 normalize 后的输出符合预期类型

### After (实际的类型验证器)

```python
PROPERTY_OUTPUT_SPEC: t.Dict[str, DeclarativeType] = {
    # Partition properties -> list of exp.Expression
    "partitioned_by": ListOfExpressionsType(),
    "partition_by": ListOfExpressionsType(),
    "partitions": ListOfStringsType(),

    # Distribution -> dict | exp.Expression | List[exp.Expression]
    "distributed_by": DictOrExpressionOrListType(),

    # Ordering -> list of exp.Expression
    "order_by": ListOfExpressionsType(),

    # Table keys -> list of exp.Expression
    "primary_key": ListOfExpressionsType(),
    "duplicate_key": ListOfExpressionsType(),
    "unique_key": ListOfExpressionsType(),
    "aggregate_key": ListOfExpressionsType(),
}
```

### 新增的 OUTPUT 类型验证器

#### ListOfExpressionsType
```python
class ListOfExpressionsType(DeclarativeType):
    """验证值是否为 List[exp.Expression]"""
    
    def validate(self, value: t.Any) -> t.Optional[t.List[exp.Expression]]:
        if not isinstance(value, list):
            return None
        for item in value:
            if not isinstance(item, exp.Expression):
                return None
        return value
```

#### ListOfStringsType
```python
class ListOfStringsType(DeclarativeType):
    """验证值是否为 List[str]"""
    
    def validate(self, value: t.Any) -> t.Optional[t.List[str]]:
        if not isinstance(value, list):
            return None
        for item in value:
            if not isinstance(item, str):
                return None
        return value
```

#### DictOrExpressionOrListType
```python
class DictOrExpressionOrListType(DeclarativeType):
    """验证值是否为 Dict | exp.Expression | List[exp.Expression]"""
    
    def validate(self, value: t.Any) -> t.Optional[t.Union[t.Dict, exp.Expression, t.List[exp.Expression]]]:
        # Dict from DistributionTupleType
        if isinstance(value, dict):
            return value
        # Single expression
        if isinstance(value, exp.Expression):
            return value
        # List of expressions
        if isinstance(value, list):
            for item in value:
                if not isinstance(item, exp.Expression):
                    return None
            return value
        return None
```

#### ExpressionOutputType
```python
class ExpressionOutputType(DeclarativeType):
    """验证值是否为 str | exp.Expression (用于 generic properties)"""
    
    def validate(self, value: t.Any) -> t.Optional[t.Union[str, exp.Expression]]:
        if isinstance(value, (str, exp.Expression)):
            return value
        return None
```

### 新增辅助函数

#### get_output_property_type()
```python
def get_output_property_type(property_name: str) -> DeclarativeType:
    """
    Get the OUTPUT type validator for a property.
    
    Returns the specific type from PROPERTY_OUTPUT_SPEC if defined,
    otherwise returns ExpressionOutputType for unknown properties.
    """
    return PROPERTY_OUTPUT_SPEC.get(property_name, ExpressionOutputType())
```

#### validate_and_normalize_property()
```python
def validate_and_normalize_property(property_name: str, value: t.Any) -> t.Any:
    """
    Complete property processing pipeline:
    1. Get INPUT type validator
    2. Validate and normalize input value
    3. Get OUTPUT type validator
    4. Verify normalized output conforms to expected type
    5. Return verified output
    
    Raises:
        ValueError: If input validation fails
        TypeError: If output validation fails
    """
    # Step 1-2: Validate and normalize input
    input_type = get_property_type(property_name)
    normalized = input_type(value)
    
    # Step 3-4: Verify output type
    output_type = get_output_property_type(property_name)
    verified = output_type.validate(normalized)
    
    if verified is None:
        raise TypeError(
            f"Normalized value for property '{property_name}' does not match expected output type."
        )
    
    return verified
```

### 使用场景

#### 场景 1: 简单 INPUT 验证和 normalize

```python
# Only INPUT validation
input_type = PROPERTY_INPUT_SPEC["partitioned_by"]
normalized = input_type("(col1, col2)")
# normalized = [exp.Column("col1"), exp.Column("col2")]
```

#### 场景 2: 完整的 INPUT → OUTPUT 流程

```python
# Full pipeline with OUTPUT validation
verified = validate_and_normalize_property("partitioned_by", "(col1, col2)")
# verified = [exp.Column("col1"), exp.Column("col2")]
# Guaranteed to be List[exp.Expression]
```

#### 场景 3: 手动两阶段验证

```python
# Manual two-phase validation
input_type = get_property_type("distributed_by")
normalized = input_type("(kind='HASH', columns=(id))")
# normalized = {"kind": "HASH", "columns": [exp.Column("id")]}

output_type = get_output_property_type("distributed_by")
verified = output_type.validate(normalized)
if verified is None:
    raise TypeError("Output type mismatch")
```

#### 场景 4: 在使用处只需关注 OUTPUT 类型

```python
# In downstream code, only care about OUTPUT types
def process_partition(prop_value: t.Any):
    """Process partitioned_by property"""
    # Type check using OUTPUT spec
    output_type = PROPERTY_OUTPUT_SPEC["partitioned_by"]
    if output_type.validate(prop_value) is None:
        raise TypeError("Invalid partition value")
    
    # Now safe to use as List[exp.Expression]
    for expr in prop_value:
        # Process each expression...
        pass
```

## 4. ✅ error_on_unknown_field 和 error_on_invalid_field

### 添加到 StructuredTupleType

```python
class StructuredTupleType(DeclarativeType):
    """
    Base class for validating tuples with typed fields.
    
    Args:
        error_on_unknown_field: If True, raise error when encountering unknown fields.
                                If False, silently skip unknown fields (default: False)
        error_on_invalid_field: If True, raise error when field value validation fails.
                                If False, return None for entire validation (default: True)
    """
    
    def __init__(
        self, 
        error_on_unknown_field: bool = False, 
        error_on_invalid_field: bool = True
    ):
        self.error_on_unknown_field = error_on_unknown_field
        self.error_on_invalid_field = error_on_invalid_field
```

### 行为对比

#### error_on_unknown_field

**False (默认)**: 跳过未知字段
```python
dist = DistributionTupleType(error_on_unknown_field=False)
result = dist.validate("(kind='HASH', unknown_field='value')")
# result = {"kind": "HASH"}  ✅ 跳过 unknown_field
```

**True**: 抛出错误
```python
dist = DistributionTupleType(error_on_unknown_field=True)
result = dist.validate("(kind='HASH', unknown_field='value')")
# ValueError: Unknown field 'unknown_field' in DistributionTupleType ❌
```

#### error_on_invalid_field

**True (默认)**: 抛出详细错误
```python
dist = DistributionTupleType(error_on_invalid_field=True)
result = dist.validate("(kind='INVALID')")
# ValueError: Invalid value for field 'kind': 'INVALID'. Expected type: EnumType ❌
```

**False**: 返回 None
```python
dist = DistributionTupleType(error_on_invalid_field=False)
result = dist.validate("(kind='INVALID')")
# result = None  ✅ 静默失败
```

#### Required Field 处理

**True (默认)**: 抛出错误
```python
dist = DistributionTupleType(error_on_invalid_field=True)
result = dist.validate("(columns=(id))")  # Missing required 'kind'
# ValueError: Required field 'kind' is missing in DistributionTupleType ❌
```

**False**: 返回 None
```python
dist = DistributionTupleType(error_on_invalid_field=False)
result = dist.validate("(columns=(id))")
# result = None  ✅
```

### 使用场景

#### 严格模式 (生产环境)
```python
# Fail fast with detailed error messages
dist = DistributionTupleType(
    error_on_unknown_field=True,
    error_on_invalid_field=True
)
```

#### 宽松模式 (探索/开发)
```python
# Ignore unknown fields, return None for invalid
dist = DistributionTupleType(
    error_on_unknown_field=False,
    error_on_invalid_field=False
)
```

#### 混合模式 (向后兼容)
```python
# Skip unknown fields but validate known ones strictly
dist = DistributionTupleType(
    error_on_unknown_field=False,  # For backward compatibility
    error_on_invalid_field=True     # For data quality
)
```

## 测试结果

运行 `python mytest/test_output_spec_and_error_handling.py`:

```
✓ PROPERTY_OUTPUT_SPEC: Type validators instead of string descriptions
✓ validate_and_normalize_property(): Complete INPUT→OUTPUT pipeline
✓ error_on_unknown_field: Control unknown field handling
✓ error_on_invalid_field: Control invalid field handling
✓ Output type validation: Verify normalized values conform to expected types
```

## 文件修改总结

### sqlmesh/core/engine_adapter/starrocks.py

**新增内容** (~142 行):
1. **ListOfExpressionsType** (26 行) - OUTPUT 验证器
2. **ListOfStringsType** (20 行) - OUTPUT 验证器
3. **DictOrExpressionOrListType** (30 行) - OUTPUT 验证器
4. **ExpressionOutputType** (14 行) - OUTPUT 验证器
5. **get_output_property_type()** (12 行) - 辅助函数
6. **validate_and_normalize_property()** (24 行) - 完整流程
7. **StructuredTupleType error handling** (16 行) - 错误处理增强

**修改内容**:
1. LiteralType.__init__ - normalized_type 默认 None
2. IdentifierType.__init__ - normalized_type 默认 None，增加 "str"
3. ColumnType.__init__ - normalized_type 默认 None，增加 "str"
4. EnumType.__init__ - normalized_type 默认 None
5. StructuredTupleType.__init__ - 增加 error 参数
6. StructuredTupleType.validate - 实现 error 处理
7. PROPERTY_OUTPUT_SPEC - 从 Dict[str, str] 改为 Dict[str, DeclarativeType]
8. DistributionSpec → DistributedBySpec 重命名

### 测试文件

**新增**: `mytest/test_output_spec_and_error_handling.py` (289 行)

## 架构改进

### Before: 单向流程

```
INPUT → validate → normalize → OUTPUT (未验证)
```

### After: 双重验证流程

```
INPUT → validate → normalize → OUTPUT validate → 确保类型正确
  ↑                                    ↑
  PROPERTY_INPUT_SPEC             PROPERTY_OUTPUT_SPEC
```

### 优势

1. **类型安全**: normalize 后的值必须符合预期的输出类型
2. **可组合**: 可以单独使用 INPUT 或 OUTPUT 验证
3. **错误定位**: 能区分是输入错误还是输出类型错误
4. **文档即代码**: PROPERTY_OUTPUT_SPEC 既是文档也是验证器
5. **灵活的错误处理**: 通过 error_on_* 参数控制行为

## 总结

✅ **完成的修改**:
1. ✅ normalized_type 默认 None (LiteralType, IdentifierType, ColumnType, EnumType)
2. ✅ IdentifierType、ColumnType 支持 normalize 到 "str"
3. ✅ DistributionSpec → DistributedBySpec 重命名
4. ✅ PROPERTY_OUTPUT_SPEC 从字符串描述改为实际类型验证器
5. ✅ error_on_unknown_field 和 error_on_invalid_field 参数
6. ✅ 完整的 validate_and_normalize_property() 流程

✅ **测试状态**: 100% 通过

✅ **代码质量**:
- 无语法错误
- 完整的类型标注
- 详细的文档字符串
- 全面的测试覆盖

All requirements completed! 🎉
