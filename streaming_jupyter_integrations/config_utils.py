import json
from typing import Any, Dict


def load_config_file(path: str) -> Dict[str, Any]:
    with open(path, "r") as json_file:
        return json.load(json_file)
