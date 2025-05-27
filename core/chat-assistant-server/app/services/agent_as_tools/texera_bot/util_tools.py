"""
Utility tool functions that can be plugged into Agents.
"""

import uuid
from agents import function_tool

__all__ = ["gen_uuid"]


@function_tool(strict_mode=True)
def gen_uuid() -> str:
    """Return a fresh UUID string (v4)."""
    return str(uuid.uuid4())
