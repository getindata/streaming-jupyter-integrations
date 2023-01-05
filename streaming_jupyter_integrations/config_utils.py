import json
import os
from typing import Any, Dict

import yaml


def load_config_file(path: str) -> Dict[str, Any]:
    with open(path, "r") as json_file:
        return json.load(json_file)


def read_flink_config_file() -> Dict[str, Any]:
    if "FLINK_CONF_DIR" in os.environ:
        config_path = os.path.join(os.environ["FLINK_CONF_DIR"], "flink-conf.yaml")
        return __read_flink_conf_yaml(config_path)
    elif "FLINK_HOME" in os.environ:
        config_path = os.path.join(os.environ["FLINK_HOME"], "conf", "flink-conf.yaml")
        return __read_flink_conf_yaml(config_path)
    else:
        print("Neither FLINK_CONF_DIR nor FLINK_HOME environment variable is set. Skipping reading flink-conf.yaml.")
    return {}


def __read_flink_conf_yaml(file_path: str) -> Dict[str, Any]:
    with open(file_path, 'r') as stream:
        try:
            parsed_yaml = yaml.safe_load(stream)
            return parsed_yaml
        except yaml.YAMLError as exc:
            print(exc)
            return {}
