import pandas as pd

from datetime import datetime
from typing import Union


def interactions_to_criterion(
    communications: pd.DataFrame,
    sessions: pd.DataFrame,
    timestamp: datetime
) -> Union[int, None]:
    """
    Transforms the client's communications and their sessions
    into the criterion that refers to the number of days of since last contact.
    """
    # Find latest communication date
    communication_dates = communications['start_time']
    last_comm_timestamp = communication_dates.max() \
        if len(communication_dates.index) > 0 else None

    # Find latest session date
    session_dates = sessions['start_time']
    last_session_timestamp = session_dates.max() \
        if len(session_dates.index) > 0 else None

    # Use the maximum date between the last communication and the last session timestamps
    # when both are present
    if last_comm_timestamp and last_session_timestamp:
        last_contact_at = max(last_comm_timestamp, last_comm_timestamp)

        return (timestamp - last_contact_at).days

    # Use the last communication timestamp
    # when the last session timestamp is not available
    elif last_comm_timestamp and not last_session_timestamp:
        return (timestamp - last_comm_timestamp).days

    # Use the last session timestamp
    # when the last communication timestamp is not available
    elif not last_comm_timestamp and last_session_timestamp:
        return (timestamp - last_session_timestamp).days

    # Otherwise return None
    return None
