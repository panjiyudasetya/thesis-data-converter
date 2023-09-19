import pandas as pd

from datetime import datetime
from typing import Union

from app.helpers import max_timestamp


def registrations_to_criterion(
    diaries: pd.DataFrame,
    thought_records: pd.DataFrame,
    smqs: pd.DataFrame,
    custom_trackers: pd.DataFrame,
    timestamp: datetime
) -> Union[int, None]:
    """
    Transforms the client's registrations into criterion
    that refers to the number of days of since last registration.
    """
    # Find latest registration date of diary entries
    diary_reg_dates = diaries['start_time']
    diary_last_reg_date = diary_reg_dates.max() \
        if len(diary_reg_dates.index) > 0 else None

    # Find latest registration date of thought records
    thought_record_reg_dates = thought_records['start_time']
    thought_record_last_reg_date = thought_record_reg_dates.max() \
        if len(thought_record_reg_dates.index) > 0 else None

    # Find latest registration date of SMQs
    smq_reg_dates = smqs['start_time']
    smq_last_reg_date = smq_reg_dates.max() \
        if len(smq_reg_dates.index) > 0 else None

    # Find latest registration date of custom trackers
    custom_tracker_reg_dates = custom_trackers['start_time']
    custom_tracker_last_reg_date = custom_tracker_reg_dates.max() \
        if len(custom_tracker_reg_dates.index) > 0 else None

    # Find the maximum timestamp
    latest_timestamp = max_timestamp([
        diary_last_reg_date,
        thought_record_last_reg_date,
        smq_last_reg_date,
        custom_tracker_last_reg_date
    ])

    return (timestamp - latest_timestamp).days if latest_timestamp else None
