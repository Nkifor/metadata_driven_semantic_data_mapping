SELECT
  source_system,
  table_name,
  COUNT(*) AS profiled_columns
FROM mapping_demo_dev.metadata.source_column_profile
GROUP BY source_system, table_name
ORDER BY source_system, table_name;

SELECT
  source_system,
  table_name,
  column_name,
  row_count,
  null_count,
  ROUND(null_rate, 4) AS null_rate,
  distinct_count,
  ROUND(distinct_rate, 4) AS distinct_rate,
  min_length,
  max_length,
  ROUND(avg_length, 2) AS avg_length,
  inferred_type,
  sample_values
FROM mapping_demo_dev.metadata.source_column_profile
ORDER BY source_system, table_name, column_name;