"""Connector registry. Add a platform by registering one more connector here."""
from .base import PlatformConnector
from .databricks import DatabricksConnector
from .fabric import FabricConnector
from .snowflake import SnowflakeConnector

REGISTRY: dict[str, PlatformConnector] = {
    c.key: c for c in (DatabricksConnector(), SnowflakeConnector(), FabricConnector())
}


def get(key: str) -> PlatformConnector:
    try:
        return REGISTRY[key]
    except KeyError:
        raise ValueError(f"Unknown platform '{key}'. Choose one of: {', '.join(REGISTRY)}") from None


__all__ = ["PlatformConnector", "REGISTRY", "get"]
