import pandas as pd

from datetime import datetime
from typing import Union

from app.helpers import max_timestamp


def interactions_to_criterion(
    communications: pd.DataFrame,
    sessions: pd.DataFrame,
    timestamp: datetime
) -> Union[int, None]:
    """
    Transforms the client's communications and their sessions into criterion
    that refers to the number of days of since last contact.
    """
    # Find latest communication date
    communication_dates = communications['start_time']
    last_comm_timestamp = communication_dates.max() \
        if len(communication_dates.index) > 0 else None

    # Find latest session date
    session_dates = sessions['start_time']
    last_session_timestamp = session_dates.max() \
        if len(session_dates.index) > 0 else None

    # Find the maximum timestamp
    latest_timestamp = max_timestamp([last_comm_timestamp, last_session_timestamp])

    return (timestamp - latest_timestamp).days if latest_timestamp else None
