#!/usr/bin/env python3
"""
Quick Start - StarRocks Engine Adapter Test

This is the simplest possible test to verify the adapter works.
Just run: python quickstart_starrocks.py

Prerequisites:
1. pip install pymysql
2. StarRocks running on localhost:9030
"""

import logging

# Configure logging to see SQL statements
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)

def main():
    print("StarRocks Adapter Quick Start")
    print("=" * 60)

    # Step 1: Import
    print("\n1. Importing modules...")
    try:
        from sqlmesh.core.engine_adapter import create_engine_adapter
        from pymysql import connect
        from functools import partial
        print("   ✓ Imports successful")
    except ImportError as e:
        print(f"   ✗ Import failed: {e}")
        print("   Please install: pip install pymysql")
        return

    # Step 2: Create connection
    print("\n2. Creating connection factory...")
    connection_factory = partial(
        connect,
        host="localhost",
        port=9030,
        user="root",
        password=""
    )
    print("   ✓ Connection factory created")

    # Step 3: Create adapter
    print("\n3. Creating StarRocks adapter...")
    try:
        adapter = create_engine_adapter(
            connection_factory=connection_factory,
            dialect="starrocks",
            # pretty_sql=True,              # Format SQL nicely
            execute_log_level=logging.INFO  # Log all SQL executions
        )
        print(f"   ✓ Adapter created: {type(adapter).__name__}")
        print(f"   ✓ Dialect: {adapter.DIALECT}")
        print(f"   ✓ SQL logging enabled")
    except Exception as e:
        print(f"   ✗ Adapter creation failed: {e}")
        return

    # Step 4: Test connection
    print("\n4. Testing connection...")
    try:
        result = adapter.fetchone("SELECT VERSION()")
        if result:
            version = result[0]
            print(f"   ✓ Connected to StarRocks {version}")
        else:
            print("   ✗ No version returned")
            return
    except Exception as e:
        print(f"   ✗ Connection test failed: {e}")
        print("   Check: StarRocks running? Correct host/port?")
        return

    # Step 5: Test create_schema
    print("\n5. Testing create_schema()...")
    test_db = "quickstart_test"
    try:
        adapter.create_schema(test_db, ignore_if_exists=True)
        print(f"   ✓ Database '{test_db}' created")

        # Verify
        result = adapter.fetchone(
            f"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA "
            f"WHERE SCHEMA_NAME = '{test_db}'"
        )
        if result:
            print(f"   ✓ Verified in information_schema")

        # Cleanup
        adapter.drop_schema(test_db, ignore_if_not_exists=True)
        print(f"   ✓ Database '{test_db}' dropped")

    except Exception as e:
        print(f"   ✗ create_schema test failed: {e}")
        import traceback
        traceback.print_exc()
        return

    # Success!
    print("\n" + "=" * 60)
    print("✓ All tests passed!")
    print("=" * 60)
    print("\nYour StarRocks adapter is working correctly!")
    print("\nNext steps:")
    print("1. Create a SQLMesh project with config.yaml")
    print("2. Define models using StarRocks dialect")
    print("3. Run sqlmesh plan and apply")
    print("\nSee STARROCKS_TESTING.md for detailed examples.")

if __name__ == "__main__":
    main()
