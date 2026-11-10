from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    log_level: str = "INFO"


class ApiSettings(ServiceSettings):
    api_key: SecretStr = Field(min_length=8)


class BrokerSettings(ServiceSettings):
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"


class RelaySettings(BrokerSettings):
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_poll_interval: float = Field(default=0.5, gt=0)


class WorkerSettings(BrokerSettings):
    consumer_prefetch: int = Field(default=20, ge=1)
    retry_max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay: float = Field(default=2.0, gt=0)
    webhook_timeout: float = Field(default=5.0, gt=0)
    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    @model_validator(mode="after")
    def _check_gateway_delays(self) -> Self:
        if self.gateway_min_delay > self.gateway_max_delay:
            raise ValueError("gateway_min_delay must not exceed gateway_max_delay")
        return self
