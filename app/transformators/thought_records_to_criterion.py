

import pandas as pd


def thought_records_to_criterion(thought_records: pd.DataFrame, notifications: pd.DataFrame) -> int:
    """
    Transforms the client's thought records and their notifications into criterion
    that refers to the completion status of the thought record.
    """
    TYPE_REMINDED_INCOMPLETE = 0
    TYPE_UNREMINDED_INCOMPLETE = 1
    TYPE_REMINDED_COMPLETE = 2
    TYPE_UNREMINDED_COMPLETE = 3

    # When no thought records are not filled in
    if len(thought_records.index) == 0:
        if len(notifications.index) == 0:
            return TYPE_UNREMINDED_INCOMPLETE

        return TYPE_REMINDED_INCOMPLETE

    # When thought records are filled in
    else:
        if len(notifications.index) == 0:
            return TYPE_UNREMINDED_COMPLETE

        return TYPE_REMINDED_COMPLETE
