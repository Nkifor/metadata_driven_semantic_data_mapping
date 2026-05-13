import mapping_engine
from mapping_engine.config import AppSettings


def test_import():
    assert mapping_engine is not None



def test_settings_can_be_loaded_from_explicit_values():
    settings = AppSettings(
        crm_landing_path="/Volumes/mapping_demo_dev/bronze/crm_landing_ext/",
        erp_landing_path="/Volumes/mapping_demo_dev/bronze/erp_landing_ext/",
        reference_landing_path="/Volumes/mapping_demo_dev/bronze/reference_landing_ext/",
        compliance_landing_path="/Volumes/mapping_demo_dev/bronze/compliance_landing_ext/",
        checkpoint_path="/Volumes/mapping_demo_dev/bronze/checkpoints_ext/",
        archive_path="/Volumes/mapping_demo_dev/bronze/archive_ext/",
        rejected_path="/Volumes/mapping_demo_dev/bronze/rejected_ext/",
    )

    assert settings.env == "dev"
    assert settings.catalog == "mapping_demo_dev"
    assert settings.crm_landing_path.endswith("/crm_landing_ext/")