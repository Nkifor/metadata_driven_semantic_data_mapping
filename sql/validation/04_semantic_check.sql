SELECT *
FROM mapping_demo_dev.bronze.semantic_concepts_raw;

SELECT
  source_system,
  table_name,
  column_name,
  concept_id,
  concept_name,
  canonical_field_name,
  pii_category,
  mapping_score,
  mapping_reason,
  decision
FROM mapping_demo_dev.metadata.mapping_decisions
ORDER BY source_system, table_name, column_name;