"""
Simple direct test of with_properties and properties functions
"""

import sys
sys.path.insert(0, '/Users/lijiao/resources/git-open-source/etl/sqlglot')

from sqlglot import exp
from sqlglot.dialects.starrocks import StarRocks

print("Starting test...")

# Create generator
generator = StarRocks.Generator()

print(f"\n1. WITH_PROPERTIES_PREFIX = '{generator.WITH_PROPERTIES_PREFIX}'")

# Create test properties
props = exp.Properties(expressions=[
    exp.Property(this=exp.Literal.string("key1"), value=exp.Literal.string("value1")),
    exp.Property(this=exp.Literal.string("key2"), value=exp.Literal.string("value2")),
])

print("\n2. Testing with_properties():")
result = generator.with_properties(props)
print(f"   Result: {result}")

print("\n3. Testing properties() with different parameters:")

r1 = generator.properties(props, prefix="PROPERTIES", wrapped=True)
print(f"   prefix='PROPERTIES', wrapped=True: {r1}")

r2 = generator.properties(props, prefix="", wrapped=True)
print(f"   prefix='', wrapped=True: {r2}")

r3 = generator.properties(props, wrapped=False)
print(f"   wrapped=False: {r3}")

print("\n4. POST_WITH properties in PROPERTIES_LOCATION:")
for cls, loc in generator.PROPERTIES_LOCATION.items():
    if loc == exp.Properties.Location.POST_WITH:
        print(f"   - {cls.__name__}")

print("\nTest complete!")
