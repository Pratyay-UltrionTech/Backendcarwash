import json
from typing import Any


def dumps_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)


def loads_json_array(raw: str | None) -> list[Any]:
    if not raw or raw.strip() == "":
        return []
    try:
        v = json.loads(raw)
        return v if isinstance(v, list) else []
    except json.JSONDecodeError:
        return []


def loads_json_object(raw: str | None) -> dict[str, Any]:
    if not raw or raw.strip() == "":
        return {}
    try:
        v = json.loads(raw)
        return v if isinstance(v, dict) else {}
    except json.JSONDecodeError:
        return {}
