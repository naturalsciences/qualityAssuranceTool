from datetime import datetime

from models.constants import ISO_STR_FORMAT
from models.enums import Properties


def filter_cfg_to_query(filter_cfg) -> str:
    filter_condition = ""
    if filter_cfg:
        range = filter_cfg.get(Properties.PHENOMENONTIME).get("range")
        format = filter_cfg.get(Properties.PHENOMENONTIME).get("format")

        t0, t1 = [datetime.strptime(str(ti), format) for ti in range]

        filter_condition = (
            f"{Properties.PHENOMENONTIME} gt {t0.strftime(ISO_STR_FORMAT)} and "
            f"{Properties.PHENOMENONTIME} lt {t1.strftime(ISO_STR_FORMAT)}"
        )
    return filter_condition