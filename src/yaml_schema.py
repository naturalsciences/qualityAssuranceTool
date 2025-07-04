import argparse
from pathlib import Path

import yaml
from cerberus import Validator

timedelta_units = [
    "W",
    "D",
    "days",
    "day",
    "hours",
    "our",
    "hr",
    "h",
    "minutes",
    "minute",
    "min",
    "m",
    "seconds",
    "econd",
    "sec",
    "s",
    "milliseconds",
    "illisecond",
    "millis",
    "milli",
    "ms",
    "microseconds",
    "icrosecond",
    "micros",
    "micro",
    "us",
    "nanoseconds",
    "anosecond",
    "nanos",
    "nano",
    "ns",
]

timedelta_units_pattern = "|".join(
    sorted(timedelta_units, key=len, reverse=True)
)  # Longest first to avoid partial match issues

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
            "verbose": {
                "oneof": [
                    {"type": "string"},
                    {"type": "boolean"},
                    {"type": "list", "schema": {"type": "string"}},
                ]
            },
            "run": {"type": "dict", "schema": {"dir": {"type": "string"}}},
        },
    },
    "data_api": {
        "type": "dict",
        "schema": {
            "base_url": {"type": "string", "regex": r"^https?://[^\s/$.?#].[^\s]*$"},
            "dry_run": {"type": "boolean"},
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
                    },
                    "Datastreams": {
                        "type": "dict",
                        "schema": {
                            "ids": {"type": "list", "schema": {"type": "integer"}}
                        },
                    },
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
    "other": {
        "type": "dict",
        "schema": {
            "count_observations": {"type": "boolean"},
            "write_flags_to_json": {"type": "boolean"},
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
            "time_window": {
                "type": "string",
                "regex": rf"^\d+({timedelta_units_pattern})",
            },
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
                    "regex": rf"^(\.)?\d(\.\d+)?+({timedelta_units_pattern})$",
                },
                "dt_stabilization": {
                    "type": "string",
                    "regex": rf"^(\.)?\d+(\.\d+)?+({timedelta_units_pattern})$",
                },
                "max_allowed_downtime": {
                    "type": "string",
                    "regex": rf"^(\.)?\d+(\.\d+)?+({timedelta_units_pattern})$",
                },
                "QC": {
                    "type": "dict",
                    "schema": {
                        "range": {"type": "list", "schema": {"type": "float"}},
                        "gradient": {"type": "list", "schema": {"type": "float"}},
                        "zscore": {"type": "list", "schema": {"type": "float"}},
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
                "id": {"type": "integer", "min": 1},
                "range": {"type": "list", "schema": {"type": "float"}},
                "gradient": {"type": "list", "schema": {"type": "float"}},
                "zscore": {"type": "list", "schema": {"type": "float"}},
            },
        },
    },
    "QC_global": {
        "type": "dict",
        "schema": {
            "zscore": {
                "type": "dict",
                "schema": {
                    "time_window": {
                        "type": "string",
                        "regex": rf"^\d+({timedelta_units_pattern})$",
                    },
                    "range": {"type": "list", "schema": {"type": "float"}},
                },
            },
            "range": {
                "type": "dict",
                "schema": {
                    "range": {"type": "list", "schema": {"type": "float"}},
                },
            },
            "gradient": {
                "type": "dict",
                "schema": {
                    "range": {"type": "list", "schema": {"type": "float"}},
                },
            }

        },
    },
}


def main():
    parser = argparse.ArgumentParser(
        description="Script to validate yaml config (QualityAssuranceTool)"
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--file", type=str, help="Yaml config file", default=None)
    group.add_argument(
        "--folder", type=str, help="Folder containing yaml file(s)", default="conf"
    )

    args = parser.parse_args()

    v = Validator(schema)  # type: ignore
    for file_i in Path(args.folder).rglob("*.yaml"):
        with open(file_i, "r") as f_i:
            conf_yaml_i = yaml.safe_load(f_i)  # type: ignore
        if v.validate(conf_yaml_i):  # type: ignore
            pass
        else:
            raise IOError(f"{file_i} Config not valid: {v.errors}")  # type: ignore

        pass


if __name__ == "__main__":
    main()
