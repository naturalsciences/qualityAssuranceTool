from ordered_enum.ordered_enum import OrderedEnum
from pandas.api.types import CategoricalDtype


class QualityFlags(OrderedEnum):
    """
    http://vocab.nerc.ac.uk/collection/L20/current/

    Args:
        OrderedEnum (_type_): _description_

    Returns:
        _type_: _description_
    """

    NO_QUALITY_CONTROL = 0
    GOOD = 1
    PROBABLY_GOOD = 2
    PROBABLY_BAD = 3
    CHANGED = 5
    BELOW_detection = 6
    IN_EXCESS = 7
    INTERPOLATED = 8
    MISSING = 9
    PHENOMENON_UNCERTAIN = "A"
    NOMINAL = "B"
    BELOW_LIMIT_OF_QUANTIFICATION = "Q"
    BAD = 4

    def __str__(self):
        return f"{self.value}"


CAT_TYPE = CategoricalDtype(list(QualityFlags), ordered=True)
