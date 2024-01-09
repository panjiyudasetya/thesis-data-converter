import pandas as pd


def diary_entries_to_criterion(diary_entries: pd.DataFrame, notifications: pd.DataFrame) -> int:
    """
    Transforms the client's diary entries and their notifications into criterion
    that refers to the completion status of the diary entry.
    """
    return (
        _get_reminder_priority(notifications),
        _get_completion_priority(diary_entries)
    )


def _get_reminder_priority(notifications: pd.DataFrame) -> int:
    """
    Returns priority of the reminder activation
    from that diary entry `notifications`.
    """
    REMINDED = 1
    UNREMINDED = 3

    notifications_count = len(notifications.index)
    return UNREMINDED if notifications_count == 0 else REMINDED


def _get_completion_priority(diary_entries: pd.DataFrame) -> int:
    """
    Returns priority of the completion of that `diary_entries` registration.
    """
    INCOMPLETE = 1
    COMPLETE = 3

    diaries_count = len(diary_entries.index)
    return COMPLETE if diaries_count > 0 else INCOMPLETE
