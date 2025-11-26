"""
init sys path
"""

import sys
from pathlib import Path
import logging

# Configure logging to see SQL statements
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(name)s - %(message)s'
)

# Add paths for sqlglot and sqlmesh
base_dir = Path(__file__).parent.parent.parent.parent
print("base dir:", base_dir)
sqlmesh_dir = base_dir
sqlglot_dir = base_dir / '../sqlglot'

sys.path.insert(0, str(sqlmesh_dir))
sys.path.insert(0, str(sqlglot_dir))
sys.path.insert(0, str(sqlmesh_dir / 'myinfo'))
# print("sys.path:", sys.path)
