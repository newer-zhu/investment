"""Utility helpers to initialize and interact with qlib for this project.

This file provides a minimal `init_qlib` wrapper and a small helper to
verify the environment. Edit `provider_uri` to point to your qlib data.
"""

from typing import Optional

def init_qlib(provider_uri: Optional[str] = None, region: str = "cn"):
    """Initialize qlib with an optional provider URI.

    Args:
        provider_uri: local path to qlib data or remote URI (None uses default).
        region: 'cn' or 'us'
    """
    try:
        import qlib
        from qlib.config import REG_CN, REG_US
    except Exception as e:
        raise RuntimeError("Please install qlib (pip install qlib) before initializing.") from e

    reg = REG_CN if region == "cn" else REG_US
    init_kwargs = {"region": reg}
    if provider_uri:
        init_kwargs["provider_uri"] = provider_uri

    qlib.init(**init_kwargs)

def check_qlib_initialized() -> bool:
    try:
        import qlib
        return qlib.get_instance() is not None
    except Exception:
        return False
