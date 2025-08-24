"""Adapters package for database-specific functionality."""

from .snowflake import SnowflakeAdapter

__all__ = ["SnowflakeAdapter"]