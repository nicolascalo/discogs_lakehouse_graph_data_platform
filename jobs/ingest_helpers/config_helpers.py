import json

def load_config_from_json(path_config_file:str):

    try:
        with open(path_config_file, "r") as config_file:
            CONFIG = json.loads(config_file.read())
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {path_config_file}") from e
    except OSError as e:
        raise RuntimeError(f"Cannot read {path_config_file}") from e

    return CONFIG