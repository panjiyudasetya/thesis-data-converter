

import pandas as pd


def diary_entries_to_criterion(diary_entries: pd.DataFrame, notifications: pd.DataFrame) -> int:
    """
    Transforms the client's diary entries and their notifications into criterion
    that refers to the completion status of the diary entry.
    """
    TYPE_REMINDED_INCOMPLETE = 0
    TYPE_UNREMINDED_INCOMPLETE = 1
    TYPE_REMINDED_COMPLETE = 2
    TYPE_UNREMINDED_COMPLETE = 3

    # When diary entries are not filled in
    if len(diary_entries.index) == 0:
        if len(notifications.index) == 0:
            return TYPE_UNREMINDED_INCOMPLETE

        return TYPE_REMINDED_INCOMPLETE

    # When diary entries are filled in
    else:
        if len(notifications.index) == 0:
            return TYPE_UNREMINDED_COMPLETE

        return TYPE_REMINDED_COMPLETE
