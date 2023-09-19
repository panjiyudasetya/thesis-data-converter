from datetime import datetime
from typing import List, Union


def max_timestamp(timestamps: List[Union[datetime, None]]) -> Union[datetime, None]:
    """
    Returns the maximum date from the given `timestamps`.
    """
    valid_timestamps = [t for t in timestamps if t]

    if len(valid_timestamps) <= 1:
        return valid_timestamps[0]

    if len(valid_timestamps) > 1:
        return max(valid_timestamps)

    return None
