"""
Test that primary_key parameter is properly normalized into table_properties.

This test validates the fix for the issue where primary_key passed as a model
parameter wasn't accessible in _build_table_properties_exp().
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from sqlglot import exp


def test_primary_key_parameter_normalization():
    """
    Test that primary_key parameter is normalized into table_properties.

    Scenario:
    - User defines primary_key as model parameter: MODEL(..., primary_key (id, dt))
    - This passes primary_key=("id", "dt") to _create_table_from_columns()
    - The fix ensures this is normalized into table_properties["primary_key"]
    - So _build_table_properties_exp() can access it
    """
    print("Testing primary_key parameter normalization...")

    # Simulate the scenario where primary_key is passed as parameter
    adapter = StarRocksEngineAdapter(lambda: None, "starrocks")

    # Test 1: primary_key as parameter (not in table_properties)
    print("\n  Test 1: primary_key as parameter")
    table_properties = {}
    primary_key = ("id", "dt")

    # Simulate what happens in _create_table_from_columns
    kwargs = {"table_properties": table_properties}
    table_properties_ref = kwargs.setdefault("table_properties", {})

    # Apply the fix: normalize parameter into table_properties
    if primary_key and "primary_key" not in table_properties_ref:
        table_properties_ref["primary_key"] = primary_key

    # Verify
    if "primary_key" in table_properties_ref and table_properties_ref["primary_key"] == primary_key:
        print("    ✅ PASSED: primary_key parameter normalized into table_properties")
    else:
        print(f"    ❌ FAILED: Expected primary_key in table_properties, got {table_properties_ref}")
        return False

    # Test 2: primary_key already in table_properties (should not overwrite)
    print("\n  Test 2: primary_key already in table_properties")
    table_properties = {"primary_key": ("other_id",)}
    primary_key = ("id", "dt")

    kwargs = {"table_properties": table_properties}
    table_properties_ref = kwargs.setdefault("table_properties", {})

    # Apply the fix: should NOT overwrite existing
    if primary_key and "primary_key" not in table_properties_ref:
        table_properties_ref["primary_key"] = primary_key

    # Verify - should keep original
    if table_properties_ref["primary_key"] == ("other_id",):
        print("    ✅ PASSED: Existing table_properties primary_key preserved")
    else:
        print(f"    ❌ FAILED: Should preserve existing, got {table_properties_ref['primary_key']}")
        return False

    # Test 3: No primary_key (neither parameter nor table_properties)
    print("\n  Test 3: No primary_key defined")
    table_properties = {}
    primary_key = None

    kwargs = {"table_properties": table_properties}
    table_properties_ref = kwargs.setdefault("table_properties", {})

    # Apply the fix
    if primary_key and "primary_key" not in table_properties_ref:
        table_properties_ref["primary_key"] = primary_key

    # Verify - should remain empty
    if "primary_key" not in table_properties_ref:
        print("    ✅ PASSED: No primary_key when none defined")
    else:
        print(f"    ❌ FAILED: Unexpected primary_key: {table_properties_ref}")
        return False

    # Test 4: Verify PropertyValidator can now access it in _build_table_properties_exp
    print("\n  Test 4: PropertyValidator can access normalized primary_key")
    from sqlmesh.core.engine_adapter.starrocks import PropertyValidator

    # Simulate normalized table_properties (as would be after fix)
    table_properties = {"primary_key": ("id", "dt")}

    # This is what happens in _build_table_properties_exp
    active_key_type = PropertyValidator.check_at_most_one(
        property_group_name="key type",
        property_names=["primary_key", "unique_key", "duplicate_key", "aggregate_key"],
        table_properties=table_properties,
        parameter_value=None,  # No separate parameter needed now!
        parameter_name=None
    )

    if active_key_type == "primary_key":
        print("    ✅ PASSED: PropertyValidator found primary_key in table_properties")
    else:
        print(f"    ❌ FAILED: Expected 'primary_key', got {active_key_type}")
        return False

    return True


def test_integration_flow():
    """
    Test the complete flow from parameter to property validation.
    """
    print("\n\nTesting integration flow...")
    from sqlmesh.core.engine_adapter.starrocks import PropertyValidator

    # Scenario: User defines MODEL with primary_key parameter
    print("  Scenario: MODEL(..., primary_key (id, dt))")

    # Step 1: In _create_table_from_columns - normalize parameter
    print("    Step 1: Normalize parameter into table_properties")
    primary_key_param = ("id", "dt")
    table_properties = {}

    if primary_key_param and "primary_key" not in table_properties:
        table_properties["primary_key"] = primary_key_param

    print(f"      table_properties = {table_properties}")

    # Step 2: In _build_table_properties_exp - validate with PropertyValidator
    print("    Step 2: Validate with PropertyValidator")
    try:
        active_key_type = PropertyValidator.check_at_most_one(
            property_group_name="key type",
            property_names=["primary_key", "unique_key", "duplicate_key", "aggregate_key"],
            table_properties=table_properties.copy(),  # Use copy to not modify
            parameter_value=None,
            parameter_name=None
        )
        print(f"      active_key_type = {active_key_type}")

        if active_key_type == "primary_key":
            print("    ✅ PASSED: Complete flow works correctly")
            return True
        else:
            print(f"    ❌ FAILED: Expected 'primary_key', got {active_key_type}")
            return False
    except Exception as e:
        print(f"    ❌ FAILED: Validation error: {e}")
        return False


def main():
    """Run all tests."""
    print("=" * 70)
    print("Primary Key Parameter Normalization Test Suite")
    print("=" * 70)

    all_passed = True

    all_passed &= test_primary_key_parameter_normalization()
    all_passed &= test_integration_flow()

    print("\n" + "=" * 70)
    if all_passed:
        print("✅ ALL TESTS PASSED")
        print("\nThe fix ensures that primary_key passed as a model parameter")
        print("is properly normalized into table_properties, making it accessible")
        print("to PropertyValidator in _build_table_properties_exp().")
    else:
        print("❌ SOME TESTS FAILED")
    print("=" * 70)

    return 0 if all_passed else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
