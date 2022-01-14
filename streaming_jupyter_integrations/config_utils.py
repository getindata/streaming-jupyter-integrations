import json


def load_config_file(path: str):
    with open(path, "r") as json_file:
        return json.load(json_file)
