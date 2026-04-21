"""
NeTEx URN builder for the Portuguese PT-EPIP profile.

Every NeTEx object ID follows the URN form:

    PT:{codespace}:{object_type}:{local_id}

where ``codespace`` is the operator's NIF (Tax ID) or IMT-assigned short
name. Non-conformant characters in ``local_id`` are sanitised to ``_``
to keep IDs XML-safe and PNDT-ingestible.
"""

import re

_COUNTRY_PREFIX = "PT"
_ALLOWED_LOCAL_ID = re.compile(r"[^A-Za-z0-9_\-]")


def sanitise_local_id(local_id: str) -> str:
    """Replace characters outside ``[A-Za-z0-9_-]`` with ``_``.

    Preserves case. Empty input raises ``ValueError``.
    """
    if not local_id:
        raise ValueError("local_id must not be empty")
    return _ALLOWED_LOCAL_ID.sub("_", str(local_id))


def build_urn(codespace: str, object_type: str, local_id: str) -> str:
    """Build a PT-EPIP URN of the form ``PT:{codespace}:{object_type}:{local_id}``.

    ``codespace`` and ``object_type`` are assumed already conformant
    (enforced upstream by ``NetexExportConfig`` and internal constants);
    ``local_id`` is sanitised defensively because it comes from GTFS data.
    """
    if not codespace:
        raise ValueError("codespace must not be empty")
    if not object_type:
        raise ValueError("object_type must not be empty")
    return f"{_COUNTRY_PREFIX}:{codespace}:{object_type}:{sanitise_local_id(local_id)}"
