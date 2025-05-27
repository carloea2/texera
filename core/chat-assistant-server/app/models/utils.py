import json
from typing import List, Dict, Any, Optional


def parse_links(links: str) -> List[Dict[str, Any]]:
    """
    Parse `links`, which may be an empty string or a JSON array string.
    Returns a list of link dicts or an empty list.
    """
    if not links or not links.strip():
        return []
    try:
        data = json.loads(links)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


def parse_operator_and_position(
    operator_and_position: Optional[str],
) -> Optional[Dict[str, Any]]:
    """
    Parse `operator_and_position`, which may be None or a JSON object string.
    Returns a dict or None on failure.
    """
    if not operator_and_position:
        return None
    try:
        return json.loads(operator_and_position)
    except json.JSONDecodeError:
        return None
