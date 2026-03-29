"""
Create the inventory provider from config/settings.json.

Switching environments (e.g. laptop mock → office/GMC SSH):

1. Set ``"provider": "ssh"`` in config/settings.json (was ``"mock"``).
2. Fill config/sources.json with real ``host`` and ``ssh_user`` values (and keep ``type`` /
   ``trusted_reference`` as today).
3. Set domain roots in config/domains.json to real paths on the remote hosts.
4. Ensure SSH keys / ``known_hosts`` work for the target user@host (no password prompts when
   ``BatchMode=yes``).
5. Smoke-test with: ``python test_provider.py --domain <id> --sources a,b`` before starting Flask.

This module is the only place that chooses mock vs SSH; app code asks for a provider here.
"""


from typing import Any, Dict

from providers.base import InventoryProvider
from providers.mock_provider import MockInventoryProvider
from providers.ssh_provider import SshInventoryProvider


def create_inventory_provider(settings: Dict[str, Any]) -> InventoryProvider:
    """
    Build the provider implementation for the current deployment.

    Supported ``settings["provider"]`` values: ``"mock"`` | ``"ssh"`` (matched case-insensitively).
    """
    mode = str(settings.get("provider", "mock")).strip().lower()
    if mode == "mock":
        return MockInventoryProvider()
    if mode == "ssh":
        return SshInventoryProvider(settings)
    raise ValueError(
        f'Unsupported inventory provider {mode!r}; use "mock" or "ssh" in config/settings.json.'
    )
