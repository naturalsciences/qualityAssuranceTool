import yaml
from cerberus import Validator
from pathlib import Path

schema = {
    "time": {
        "type": "dict",
        "schema": {
            "format": {"type": "string", "regex": r"^%Y-%m-%d %H:%M:%S$"},
            "start": {
                "type": "string",
                "regex": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
            },
            "end": {
                "type": "string",
                "regex": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
            },
            "date": {
                "type": "dict",
                "schema": {"format": {"type": "string", "regex": r"^%Y-%m-%d$"}},
            },
        },
    },
    "hydra": {
        "type": "dict",
        "schema": {
            "verbose": {"type": "string"},
            "run": {"type": "dict", "schema": {"dir": {"type": "string"}}},
        },
    },
    "data_api": {
        "type": "dict",
        "schema": {
            "base_url": {"type": "string", "regex": r"^https?://[^\s/$.?#].[^\s]*$"},
            "auth": {
                "type": "dict",
                "schema": {
                    "username": {"type": "string"},
                    "passphrase": {"type": "string"},
                },
            },
            "things": {"type": "dict", "schema": {"id": {"type": "integer", "min": 1}}},
            "filter": {
                "type": "dict",
                "schema": {
                    "phenomenonTime": {
                        "type": "dict",
                        "schema": {
                            "format": {"type": "string"},
                            "range": {"type": "list", "schema": {"type": "string"}},
                        },
                    }
                },
            },
        },
    },
    "reset": {
        "type": "dict",
        "schema": {
            "overwrite_flags": {"type": "boolean"},
            "observation_flags": {"type": "boolean"},
            "feature_flags": {"type": "boolean"},
            "exit": {"type": "boolean"},
        },
    },
    "location": {
        "type": "dict",
        "schema": {
            "connection": {
                "type": "dict",
                "schema": {
                    "database": {"type": "string"},
                    "user": {"type": "string"},
                    "host": {"type": "string"},
                    "port": {"type": "integer", "min": 1},
                    "passphrase": {"type": "string"},
                },
            },
            "crs": {"type": "string"},
            "time_window": {"type": "string", "regex": r"^\d+[a-zA-Z]+$"},
            "max_dx_dt": {"type": "float"},
            "max_ddx_dtdt": {"type": "float"},
        },
    },
    "QC_dependent": {
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": {
                "independent": {"type": "integer", "min": 1},
                "dependent": {
                    "anyof": [
                        {"type": "list", "schema": {"type": "integer", "min": 1}},
                        {"type": "string", "regex": r"^\d+(,\d+)*$"},
                        {"type": "integer", "min": 1},
                    ]
                },
                "dt_tolerance": {
                    "type": "string",
                    "regex": r"^(\.)?\d(\.\d+)?+[a-zA-Z]+$",
                },
                "QC": {
                    "type": "dict",
                    "schema": {
                        "range": {"type": "list", "schema": {"type": "float"}},
                        "gradient": {"type": "list", "schema": {"type": "float"}},
                    },
                },
            },
        },
    },
    "QC": {
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": {
                "independent": {"type": "integer", "min": 1},
                "range": {"type": "list", "schema": {"type": "float"}},
                "gradient": {"type": "list", "schema": {"type": "float"}},
            },
        },
    },
}


def main():
    with open("conf/config.yaml", "r") as f:
        config_yaml = yaml.safe_load(f)

    v = Validator(schema) # type: ignore
    # v.validate(config_yaml) # type: ignore

    if v.validate(config_yaml): # type: ignore
        pass
    else:
        raise IOError(f"Config not valid: {v.errors}") # type: ignore


if __name__ == "__main__":
    main()
