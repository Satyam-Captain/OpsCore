"""Abstract script/GMC adapter for config generation and publish."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict


class ScriptsAdapterBase(ABC):
    @abstractmethod
    def generate_config(
        self, config_type: str, cluster: str, context: Dict[str, Any]
    ) -> str:
        """Produce config; return path to generated artifact."""

    @abstractmethod
    def publish_to_request(
        self, config_type: str, cluster: str, request_number: str, generated_path: str
    ) -> str:
        """Copy or register artifact into request workspace; return destination path."""
