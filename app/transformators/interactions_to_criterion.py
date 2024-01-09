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
    return (
        _get_days_diff_since_last_call(communications, sessions, timestamp),
        _get_days_diff_since_last_chat(communications, timestamp)
    )


def _get_days_diff_since_last_call(
    communications: pd.DataFrame,
    sessions: pd.DataFrame,
    timestamp: datetime
) -> Union[int, None]:
    """
    Compute days difference since the last call occurred.
    """
    comms = communications.copy(deep=True)
    calls = comms[(communications['call_made'])]

    last_call_at = calls['start_time'].max() \
        if len(calls.index) > 0 else None

    last_session_at = sessions['start_time'].max() \
        if len(sessions.index) > 0 else None

    latest_call_at = max_timestamp([last_call_at, last_session_at])
    return (timestamp - latest_call_at).days if latest_call_at else None


def _get_days_diff_since_last_chat(
    communications: pd.DataFrame,
    timestamp: datetime
) -> Union[int, None]:
    """
    Compute days difference since the last chat occurred.
    """
    comms = communications.copy(deep=True)
    chats = comms[(communications['chat_msg_sent'])]

    last_chat_at = chats['start_time'].max() \
        if len(chats.index) > 0 else None

    return (timestamp - last_chat_at).days if last_chat_at else None
