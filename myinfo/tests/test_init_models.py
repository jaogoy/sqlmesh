"""
Shared utilities for initializing and working with SQLMesh test models

This module provides common functions for:
- Creating SQLMesh Context
- Discovering and loading models
- Selecting models for testing
- Printing model information
"""

import sys
from pathlib import Path
from typing import List, Dict, Optional, Callable

# Add paths
base_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(base_dir))

try:
    from sqlmesh import Context
    from sqlmesh.core.model import Model
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)


def create_context(project_dir: Path, verbose: bool = False, load_models: bool = True) -> Optional[Context]:
    """
    Create a SQLMesh Context for the given project directory

    Args:
        project_dir: Path to the project directory (where config.yaml is located)
        verbose: Whether to print detailed logging
        load_models: Whether to load all models (False = lazy loading for faster startup)

    Returns:
        Context instance or None if creation fails
    """
    if verbose:
        print(f"\nCreating SQLMesh Context...")
        print(f"  Project directory: {project_dir}")
        if not load_models:
            print(f"  Mode: Lazy loading (faster)")

    try:
        # Note: SQLMesh always loads models at Context creation
        # For faster testing, consider using select_models() to filter
        context = Context(paths=[str(project_dir)])

        if verbose:
            print(f"  ✓ Context created successfully")
            print(f"  - Default dialect: {context.config.dialect}")
            print(f"  - Models found: {len(context.models)}")

        return context
    except Exception as e:
        print(f"  ✗ Error creating context: {e}")
        return None


def list_models(context: Context, verbose: bool = True) -> List[str]:
    """
    List all models in the context

    Args:
        context: SQLMesh Context
        verbose: Whether to print the list

    Returns:
        List of model names (fully qualified)
    """
    model_names = sorted(context.models.keys())

    if verbose:
        print(f"\nFound {len(model_names)} model(s):")
        for name in model_names:
            # Extract just the table name for display
            short_name = name.split('.')[-1].strip('"')
            print(f"  - {short_name:30} ({name})")

    return model_names


def select_models(
    context: Context,
    pattern: Optional[str] = None,
    include_catalog: bool = False
) -> Dict[str, Model]:
    """
    Select models matching a pattern

    Args:
        context: SQLMesh Context
        pattern: Pattern to match (None = all models)
        include_catalog: Whether to include catalog in matching

    Returns:
        Dictionary of {model_name: model_object}
    """
    selected = {}

    for full_name, model in context.models.items():
        # Get short name without catalog/schema
        short_name = full_name.split('.')[-1].strip('"')

        # Determine matching name
        match_name = full_name if include_catalog else short_name

        # Check pattern
        if pattern is None or pattern.lower() in match_name.lower():
            selected[full_name] = model

    return selected


def get_model_by_name(
    context: Context,
    name: str,
    fuzzy: bool = True
) -> Optional[tuple[str, Model]]:
    """
    Get a single model by name (supports fuzzy matching)

    Args:
        context: SQLMesh Context
        name: Model name to search for
        fuzzy: Whether to allow fuzzy matching

    Returns:
        Tuple of (full_name, model) or None if not found
    """
    # Try exact match first
    if name in context.models:
        return name, context.models[name]

    # Try fuzzy match
    if fuzzy:
        name_lower = name.lower()
        for full_name, model in context.models.items():
            short_name = full_name.split('.')[-1].strip('"').lower()
            if name_lower == short_name or name_lower in short_name:
                return full_name, model

    return None


def load_single_model(project_dir: Path, model_name: str, verbose: bool = False) -> Optional[tuple[str, Model]]:
    """
    Load a single model from the project

    NOTE: This function does NOT provide any performance benefit.
    SQLMesh Context always loads ALL models in the models/ directory.
    There is no way to load only a single model file.

    This function is just a convenience wrapper that:
    1. Creates a Context (loads all models)
    2. Returns only the requested model

    Args:
        project_dir: Path to the project directory
        model_name: Short name or pattern of the model to load
        verbose: Whether to print progress

    Returns:
        Tuple of (full_name, model) or None if not found

    Example:
        >>> result = load_single_model(test_dir, "test_range_partition")
        >>> if result:
        >>>     full_name, model = result
    """
    if verbose:
        print(f"\nLoading model: {model_name}...")
        print(f"  (Note: All models are loaded, cannot load only one)")

    # SQLMesh Context always loads all models - no way to avoid this
    context = create_context(project_dir, verbose=False)
    if not context:
        return None

    result = get_model_by_name(context, model_name, fuzzy=True)

    if verbose:
        if result:
            full_name, model = result
            short = full_name.split('.')[-1].strip('"')
            print(f"  ✓ Found: {short}")
            print(f"  - Full name: {full_name}")
            print(f"  - Type: {type(model).__name__}")
            print(f"  - Total models loaded: {len(context.models)}")
        else:
            print(f"  ✗ Model not found: {model_name}")

    return result


def print_model_summary(model: Model, name: str = None) -> None:
    """
    Print a summary of a model

    Args:
        model: Model to summarize
        name: Optional display name
    """
    display_name = name or model.name

    print(f"\n{'=' * 80}")
    print(f"Model: {display_name}")
    print('=' * 80)
    print(f"  Type: {type(model).__name__}")
    print(f"  Kind: {model.kind}")
    print(f"  Dialect: {model.dialect}")

    if hasattr(model, 'partitioned_by_') and model.partitioned_by_:
        print(f"  Partitioned by: {len(model.partitioned_by_)} expression(s)")

    if hasattr(model, 'clustered_by') and model.clustered_by:
        print(f"  Clustered by: {len(model.clustered_by)} column(s)")

    if hasattr(model, 'columns_to_types') and model.columns_to_types:
        print(f"  Columns: {len(model.columns_to_types)}")


def analyze_models(
    context: Context,
    analyzer_func: Callable[[str, Model], None],
    pattern: Optional[str] = None,
    verbose: bool = True
) -> None:
    """
    Analyze selected models using a custom analyzer function

    Args:
        context: SQLMesh Context
        analyzer_func: Function that takes (model_name, model) and analyzes it
        pattern: Optional pattern to filter models
        verbose: Whether to print progress
    """
    selected = select_models(context, pattern)

    if not selected:
        print(f"\n⚠️  No models found matching pattern: {pattern}")
        return

    if verbose:
        print(f"\nAnalyzing {len(selected)} model(s)...")

    for full_name, model in selected.items():
        short_name = full_name.split('.')[-1].strip('"')

        if verbose:
            print(f"\n{'-' * 80}")
            print(f"Analyzing: {short_name}")
            print('-' * 80)

        try:
            analyzer_func(full_name, model)
        except Exception as e:
            print(f"  ✗ Error analyzing {short_name}: {e}")


def create_model_name_map(short_names: Dict[str, str]) -> Dict[str, str]:
    """
    Create a mapping from short names to full model names

    Args:
        short_names: Dictionary of {alias: short_model_name}

    Returns:
        Dictionary that can be used with get_model_by_name

    Example:
        >>> model_map = create_model_name_map({
        ...     'range': 'test_range_partition',
        ...     'list': 'test_list_partition'
        ... })
    """
    return short_names


def find_models_dir(test_file: Path) -> Path:
    """
    Find the models directory relative to a test file

    Args:
        test_file: Path to the test file (usually __file__)

    Returns:
        Path to the models directory
    """
    test_dir = test_file.parent if test_file.is_file() else test_file
    models_dir = test_dir / "models"

    if not models_dir.exists():
        raise FileNotFoundError(f"Models directory not found: {models_dir}")

    return models_dir


# Example usage
def main():
    """Example of using the test initialization utilities"""
    print("=" * 80)
    print("  SQLMesh Test Model Initialization Example")
    print("=" * 80)

    # Find models directory
    test_dir = Path(__file__).parent / "test_models_syntax_parse"

    try:
        models_dir = find_models_dir(test_dir)
    except FileNotFoundError as e:
        print(f"\n✗ {e}")
        return

    # Create context
    context = create_context(models_dir, verbose=True)
    if not context:
        return

    # List all models
    model_names = list_models(context)

    # Select models with pattern
    print("\n" + "=" * 80)
    print("Models matching 'partition':")
    print("=" * 80)
    selected = select_models(context, pattern="partition")
    for name in selected.keys():
        print(f"  - {name}")

    # Get a specific model
    print("\n" + "=" * 80)
    print("Get model by name:")
    print("=" * 80)
    result = get_model_by_name(context, "range", fuzzy=True)
    if result:
        full_name, model = result
        print_model_summary(model, full_name)

    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
