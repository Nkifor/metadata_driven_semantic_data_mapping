from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="APP_",
        extra="ignore",
    )

    env: str = "dev"
    catalog: str = "mapping_demo_dev"

    crm_landing_path: str
    erp_landing_path: str
    reference_landing_path: str
    compliance_landing_path: str

    checkpoint_path: str
    archive_path: str
    rejected_path: str


def get_settings() -> AppSettings:
    return AppSettings()