import itertools
import pandas as pd

from datetime import datetime
from dateutil.rrule import rrulestr
from typing import List


def planned_events_to_criterion(
    events: pd.DataFrame,
    reflections: pd.DataFrame,
    from_datetime: datetime,
    to_datetime: datetime
) -> int:
    """
    Transforms event registrations and their reflections into criterion
    that refers to the completion of the client's planned events.
    """
    # Get total completed events.
    completed_events = reflections[(reflections['status'] == 'COMPLETED')]
    total_completed_events = len(completed_events.index)

    # Get total recurrent events.
    total_events = _total_recurrent_events(events.copy(), from_datetime, to_datetime)

    return _to_criterion(total_events, total_completed_events)


def _total_recurrent_events(events: pd.DataFrame, from_datetime: datetime, to_datetime: datetime) -> int:
    """
    Returns total recurrent events.
    """
    # Get the recurrent rule expressions
    recurrent_rules = [
        event['recurring_expression']['rrule']
        for _, event in events.iterrows()
    ]

    # Generate the timestamps of all activities
    event_timestamp_lists = [
        _events_between(from_datetime, to_datetime, rrule_str)
        for rrule_str in recurrent_rules
    ]
    event_timestamps = list(itertools.chain(*event_timestamp_lists))

    return len(event_timestamps)


def _events_between(from_datetime: datetime, to_datetime: datetime, rrule_str: str) -> List[datetime]:
    """
    Returns the list of the dates that are generated from that given `rrule_str`
    between the given time range.

    Arguments:

    - `from_datetime`: The date after
    - `to_datetime`: The date before
    - `rrule_str`: Recurrent rule expression (i.e. `"DTSTART:20230328T120000\nRRULE:FREQ=DAILY;"`)
    """
    _rrule = rrulestr(rrule_str)
    return _rrule.between(after=from_datetime, before=to_datetime, inc=True)


def _to_criterion(total_event: int, total_completed: int) -> int:
    """
    Transforms total registration into Deeploy's criterion.
    """
    PLANNED_INCOMPLETE_TYPE = 0
    PLANNED_COMPLETE_TYPE = 1
    PLANNED_SOME_COMPLETE_TYPE = 2
    UNPLANNED_TYPE = 3

    if total_event == 0:
        return UNPLANNED_TYPE

    if total_completed == total_event:
        return PLANNED_COMPLETE_TYPE

    if total_completed > 0 and total_completed < total_event:
        return PLANNED_SOME_COMPLETE_TYPE

    if total_completed == 0 and total_event > 0:
        return PLANNED_INCOMPLETE_TYPE

    return None
