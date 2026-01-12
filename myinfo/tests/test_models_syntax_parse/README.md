# SQLMesh Model Syntax Parsing Tests

This directory contains tests for understanding how SQLMesh parses different model syntax configurations, particularly `partitioned_by`, `distributed_by`, and other physical properties.

## Design Principles

1. **Use SQLMesh Context**: Models are loaded through SQLMesh Context, NOT parsed with regex
2. **One syntax per model**: Each model file contains ONE partition syntax (SQLMesh only uses the last one)
3. **Shared utilities**: Common code for model initialization and expression analysis is extracted to reusable modules

## Directory Structure

```
test_models_syntax_parse/
├── models/                    # Test model SQL files
│   ├── test_range_partition.sql       # RANGE(dt)
│   ├── test_list_partition.sql        # (col1, col2)
│   ├── test_expression_partition.sql  # date_trunc('day', created_at)
│   └── test_simple_partition.sql      # dt
├── config.yaml                # SQLMesh configuration
├── test_parse_partitions.py   # Analyze partition syntax via Context
├── test_parse_model_syntax.py # Detailed model property inspection
└── README.md                  # This file
```

## Shared Utilities

Common functions are extracted to two shared modules:

### `/myinfo/tests/test_utils.py` - Expression Analysis

- `analyze_expression()` - Recursively analyze SQLGlot expression structure
- `print_structure()` - Pretty print expression structure
- `print_expression_detailed()` - Detailed analysis with type, SQL, args
- `compare_expressions()` - Side-by-side comparison of multiple expressions

### `/myinfo/tests/test_init_models.py` - Model Initialization

- `create_context()` - Create SQLMesh Context from project directory
- `list_models()` - List all models in context
- `select_models()` - Select models by pattern
- `get_model_by_name()` - Get model with fuzzy matching
- `print_model_summary()` - Print model summary
- `analyze_models()` - Apply custom analysis function to models

## Test Scripts

### 1. test_parse_partitions.py

**Purpose**: Analyze partition expressions from SQLMesh parsed models.

**Features**:
- Uses SQLMesh Context to load and parse models (NOT regex)
- Extracts `partitioned_by_` from parsed model objects
- Uses shared utilities for detailed structure analysis
- Compares all partition syntaxes found across models

**Usage**:
```bash
cd myinfo/tests/test_models_syntax_parse
python test_parse_partitions.py
```

**Output**:
- Creates SQLMesh Context and lists all models
- For each model with partitions:
  - Shows partition expression count
  - Detailed structure analysis of each expression
- Comparison table of all partitions
- Summary with expression type distribution

### 2. test_parse_model_syntax.py

**Purpose**: Load models through SQLMesh Context and analyze how they are parsed.

**Features**:
- Creates SQLMesh Context
- Loads models by name
- Shows model attributes (partitioned_by_, clustered_by, etc.)
- Uses shared utilities for detailed partition analysis
- Compares multiple partitions if present

**Usage**:
```bash
cd myinfo/tests/test_models_syntax_parse

# Test default model (test_range_partition)
python test_parse_model_syntax.py

# Test specific model
python test_parse_model_syntax.py -m range
python test_parse_model_syntax.py -m list
python test_parse_model_syntax.py -m expr
python test_parse_model_syntax.py -m simple
```

**Options**:
- `-m, --model`: Model to test (range, list, expr, simple, or full model name)

## Test Model Files

Each model file tests ONE specific partition syntax:

1. **test_range_partition.sql**: `partitioned_by RANGE(dt)`
   - Expression Type: `Anonymous` (function call)

2. **test_list_partition.sql**: `partitioned_by (col1, col2)`
   - Expression Type: `Column` (expands to multiple Column objects)

3. **test_expression_partition.sql**: `partitioned_by date_trunc('day', created_at)`
   - Expression Type: `DateTrunc` (known function)

4. **test_simple_partition.sql**: `partitioned_by dt`
   - Expression Type: `Column` (simple column reference)

**Note**: SQLMesh only uses the LAST `partitioned_by` if multiple are defined, so each test model contains only ONE partition definition.

## Expression Types Found

From our tests, different partition syntaxes result in different SQLGlot expression types:

| Syntax | Expression Type | Description |
|--------|----------------|-------------|
| `RANGE(dt)` | Anonymous | Function call with unknown function name |
| `(col1, col2)` | Tuple | Multiple columns as tuple |
| `date_trunc('day', col)` | TimestampTrunc | Known function, specific expression type |
| `dt` | Column | Simple column reference |

## Creating New Syntax Tests

To test other model properties (e.g., `distributed_by`, `clustered_by`):

1. **Create new model files** in `models/` directory
2. **Use shared utilities** from `test_utils.py` and `test_init_models.py`
3. **Create a new test script** (e.g., `test_distribution_syntax.py`)

Example:
```python
from myinfo.tests.test_utils import print_expression_detailed
from myinfo.tests.test_init_models import (
    create_context,
    list_models,
    select_models
)

# Create context
test_dir = Path(__file__).parent
context = create_context(test_dir, verbose=True)

# Select and analyze models
for full_name, model in select_models(context, pattern="distribution").items():
    if hasattr(model, 'distributed_by') and model.distributed_by:
        for expr in model.distributed_by:
            print_expression_detailed(expr, f"{full_name} - Distribution")
```

## Key Findings

1. **SQLMesh Context parsing**: Models are loaded through SQLMesh Context which properly parses all syntax

2. **One partition per model**: Each model should have only ONE `partitioned_by` definition (SQLMesh uses the last one)

3. **Expression structure**: Each partition definition is parsed as a SQLGlot Expression with:
   - `type`: Expression class name
   - `sql()`: SQL representation
   - `key`: Expression key
   - `args`: Child expressions and attributes

4. **Shared utilities**: Reusable modules make it easy to:
   - Initialize contexts and load models
   - Analyze expression structures in detail
   - Compare expressions across models

## Future Extensions

This testing framework can be extended to test:
- `physical_properties` parsing
- `distributed_by` syntax (for StarRocks/Doris)
- `clustered_by` syntax
- `table_properties` syntax
- Other dialect-specific properties

The shared utilities (`test_utils.py` and `test_init_models.py`) make it easy to add new tests without duplicating code.
