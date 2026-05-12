# Metadata driven semantic data mapping

Application/workload repository for GDPR-aware Semantic Data Mapping & Entity Resolution on Databricks.

This repository does not provision Azure or Databricks infrastructure.

It consumes Unity Catalog objects and external volumes created by:
- data-mapping-bootstrap
- data-mapping-infra

## External contract

Expected catalog:

mapping_demo_dev

Expected volumes:

/Volumes/mapping_demo_dev/bronze/crm_landing_ext/
/Volumes/mapping_demo_dev/bronze/erp_landing_ext/
/Volumes/mapping_demo_dev/bronze/reference_landing_ext/
/Volumes/mapping_demo_dev/bronze/compliance_landing_ext/
/Volumes/mapping_demo_dev/bronze/checkpoints_ext/
/Volumes/mapping_demo_dev/bronze/archive_ext/
/Volumes/mapping_demo_dev/bronze/rejected_ext/