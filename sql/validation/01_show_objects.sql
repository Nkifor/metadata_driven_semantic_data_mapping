-- Validate that repo 2 Unity Catalog objects are visible to the current principal.
USE CATALOG mapping_demo_dev;

SHOW SCHEMAS;
SHOW VOLUMES IN bronze;
SHOW TABLES IN bronze;
SHOW TABLES IN silver;
SHOW TABLES IN gold;
SHOW TABLES IN metadata;
SHOW TABLES IN compliance;
