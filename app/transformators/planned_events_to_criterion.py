import pandas as pd


def planned_events_to_criterion(events: pd.DataFrame) -> int:
    """
    Transforms planned event's completions into criterion.
    """
    PLANNED_INCOMPLETE = 0
    PLANNED_COMPLETE = 1
    PLANNED_SOME_COMPLETE = 2
    UNPLANNED = 3

    events_count = len(events.index)
    incomplete_events_count = len(events[
        (events['status'] == 'INCOMPLETED') |
        (events['status'] == 'CANCELED')
    ].index)

    if events_count == 0:
        return UNPLANNED

    if incomplete_events_count == 0:
        return PLANNED_COMPLETE

    elif incomplete_events_count == events_count:
        return PLANNED_INCOMPLETE

    else:
        return PLANNED_SOME_COMPLETE
