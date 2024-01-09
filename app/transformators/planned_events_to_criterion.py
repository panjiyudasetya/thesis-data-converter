import pandas as pd


def planned_events_to_criterion(events: pd.DataFrame) -> int:
    """
    Transforms planned event's completions into criterion.
    """
    return (
        _get_schedule_priority(events),
        _get_completion_priority(events)
    )


def _get_schedule_priority(events: pd.DataFrame) -> int:
    """
    Returns priority of the schedule type from that `events`.
    """
    PLANNED = 1
    UNPLANNED = 3

    events_count = len(events.index)
    return UNPLANNED if events_count == 0 else PLANNED


def _get_completion_priority(events: pd.DataFrame) -> int:
    """
    Returns priority of the event completion of that `events`.
    """
    NONE = 0
    INCOMPLETE = 1
    SOME_COMPLETE = 2
    COMPLETE = 3

    events_count = len(events.index)
    if events_count == 0:
        return NONE

    incomplete_events_count = len(events[
        (events['status'] == 'INCOMPLETED') |
        (events['status'] == 'CANCELED')
    ].index)

    if incomplete_events_count == 0:
        return COMPLETE
    elif incomplete_events_count == events_count:
        return INCOMPLETE
    else:
        return SOME_COMPLETE
