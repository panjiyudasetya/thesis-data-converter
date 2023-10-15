import numpy as np
import pandas as pd

from typing import Union


def smqs_to_criterion(last_smq: Union[pd.Series, None], previous_smq: Union[pd.Series, None]) -> int:
    """
    Transforms the series of SMQ scores (last and previous) into categorical value.

    Arguments:
        - `last_smq`: The last SMQ.
        - `previous_smq`: The previous SMQ.
    """
    # Extracts relevant columns
    focused_columns = ['applicability', 'connection', 'content', 'progress', 'way_of_working']
    last_scores = None if last_smq is None else last_smq[focused_columns].copy()
    previous_scores = None if previous_smq is None else previous_smq[focused_columns].copy()

    # Calculate criterion
    TYPE_LARGE_DECREASE = 0
    TYPE_LOW_SCORE = 1
    TYPE_LARGE_INCREASE = 2
    TYPE_STABLE = 3

    # If either of the SMQ's last scores or the SMQ's previous scores is None, return `STABLE`
    # @see https://mitrd.slack.com/archives/C049Y8M8G5Q/p1691657753060259?thread_ts=1691566944.227709&cid=C049Y8M8G5Q
    if last_scores is None or previous_scores is None:
        return TYPE_STABLE

    # If an answer below 4.5 found in the SMQ's last answers,
    # we prioritize it and returns the low score type's value.
    if np.any(last_scores < 4.5):
        return TYPE_LOW_SCORE

    scores_diff = last_scores - previous_scores

    if np.any(scores_diff < -1.5):
        return TYPE_LARGE_DECREASE
    elif np.any(scores_diff > 1.5):
        return TYPE_LARGE_INCREASE
    else:
        return TYPE_STABLE
