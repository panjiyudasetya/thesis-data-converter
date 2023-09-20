import json

from datetime import datetime
from typing import Dict, List, Union


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


def to_dict(value: any) -> Union[Dict, any]:
    """
    Converts value to Python dictionary (if possible).
    """
    # When the incoming value is not string, return as it is.
    if not isinstance(value, str):
        return value
    
    # Otherwise, try parse it to a dictionary.
    # * Clean the string value
    cleaned_value = value.replace('False', '"false"')\
        .replace('True', '"true"')\
        .replace("'", '"')\
        .replace("'", "\'")\
        .replace('"', '\"')

    # * Load cleaned string as JSON
    return json.loads(cleaned_value)
