"""
NeTEx export configuration for the PT-EPIP profile.

Persisted per-project at ``projects/<slug>/netex_config.json``. Captures
the fields PT-EPIP requires but GTFS does not carry: codespace,
Authority + Operator organisation blocks, participant ref, and version
strategy.

Codespace validation is strict because the PNDT validator rejects any
file whose URNs do not use a registered codespace. The placeholder
value ``"FIXME"`` is accepted at the storage layer so a user can save
partial config, but callers must gate export on
:func:`NetexExportConfig.is_placeholder` before serialising.
"""

import json
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field

from optisus.core.storage.layers import PROJECTS_ROOT

NETEX_CONFIG_FILENAME = "netex_config.json"
PLACEHOLDER_CODESPACE = "FIXME"

_CODESPACE_PATTERN = r"^[A-Z0-9]+$"


class NetexAuthority(BaseModel):
    """The Autoridade de Transportes that owns the data (e.g. CIM do Ave)."""

    id: str = Field(..., description="Authority local id — becomes urn suffix")
    name: str = Field(..., description="Legal name, e.g. 'CIM do Ave'")
    short_name: Optional[str] = Field(None, description="Display short name")
    contact_email: Optional[str] = Field(None, description="Data-ownership contact")


class NetexOperator(BaseModel):
    """The Operador running the service (e.g. Guimabus, STCP)."""

    id: str = Field(..., description="Operator local id — becomes urn suffix")
    name: str = Field(..., description="Legal name, e.g. 'Guimabus'")
    short_name: str = Field(
        ...,
        description="Short name used in zip filename (NETEX_PT_{short_name}_...)",
    )
    contact_email: Optional[str] = Field(None, description="Service-ops contact")


class NetexExportConfig(BaseModel):
    """Per-project NeTEx export configuration.

    ``codespace`` should be the operator's NIF or IMT-assigned short name.
    The placeholder ``"FIXME"`` is allowed in storage but blocks export.
    """

    codespace: str = Field(
        PLACEHOLDER_CODESPACE,
        pattern=_CODESPACE_PATTERN,
        description="PT-EPIP codespace (NIF or IMT short name). Uppercase alphanumerics only.",
    )
    authority: NetexAuthority = Field(..., description="Data-ownership entity")
    operator: NetexOperator = Field(..., description="Service-operation entity")
    participant_ref: Optional[str] = Field(
        None, description="NAP-assigned participant ref (PNDT)"
    )
    default_lang: str = Field("pt", description="IETF BCP 47 language tag")
    version_strategy: Literal["timestamp", "manual"] = Field(
        "timestamp",
        description="'timestamp' stamps every object with the export date (phase 1 default)",
    )

    @property
    def codespace_full(self) -> str:
        """The full URN prefix, ``PT:{codespace}``."""
        return f"PT:{self.codespace}"

    def is_placeholder(self) -> bool:
        """True when the codespace is still the placeholder — export must be blocked."""
        return self.codespace == PLACEHOLDER_CODESPACE


# ═══════════════════════════════════════════════════════════════════════════
# Persistence
# ═══════════════════════════════════════════════════════════════════════════

def _config_path(project_slug: str) -> Path:
    return PROJECTS_ROOT / project_slug / NETEX_CONFIG_FILENAME


def load_netex_config(project_slug: str) -> Optional[NetexExportConfig]:
    """Load config from ``projects/<slug>/netex_config.json``.

    Returns ``None`` when the file does not exist so the UI can render a
    blank form on first run. Malformed JSON raises.
    """
    path = _config_path(project_slug)
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fh:
        return NetexExportConfig.model_validate(json.load(fh))


def save_netex_config(project_slug: str, config: NetexExportConfig) -> Path:
    """Persist config to ``projects/<slug>/netex_config.json``. Returns the path."""
    path = _config_path(project_slug)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(config.model_dump(), fh, indent=2, ensure_ascii=False)
    return path
