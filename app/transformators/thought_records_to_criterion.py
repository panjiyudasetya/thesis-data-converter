import pandas as pd


def thought_records_to_criterion(thought_records: pd.DataFrame, notifications: pd.DataFrame) -> int:
    """
    Transforms the client's thought records and their notifications into criterion
    that refers to the completion status of the thought record.
    """
    return (
        _get_reminder_priority(notifications),
        _get_completion_priority(thought_records)
    )


def _get_reminder_priority(notifications: pd.DataFrame) -> int:
    """
    Returns priority of the reminder activation
    from that thought records `notifications`.
    """
    REMINDED = 1
    UNREMINDED = 3

    notifications_count = len(notifications.index)
    return UNREMINDED if notifications_count == 0 else REMINDED


def _get_completion_priority(thought_records: pd.DataFrame) -> int:
    """
    Returns priority of the completion of that `thought_records` registration.
    """
    INCOMPLETE = 1
    COMPLETE = 3

    thought_records_count = len(thought_records.index)
    return COMPLETE if thought_records_count > 0 else INCOMPLETE
