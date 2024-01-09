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
    return (
        _get_scores_diff(last_smq, previous_smq),
        _get_low_score(last_smq)
    )


def _get_scores_diff(last_smq: Union[pd.Series, None], previous_smq: Union[pd.Series, None]) -> int:
    """
    Returns priority of the scores difference between `last_smq` and `previous_smq`.
    """
    # Extracts relevant columns
    focused_columns = ['applicability', 'connection', 'content', 'progress', 'way_of_working']
    last_scores = None if last_smq is None else last_smq[focused_columns].copy()
    previous_scores = None if previous_smq is None else previous_smq[focused_columns].copy()

    # Calculate criterion
    LARGE_DECREASE = 1
    STABLE = 2
    LARGE_INCREASE = 3

    # If either of the SMQ's last scores or the SMQ's previous scores is None, return `STABLE`
    # @see https://mitrd.slack.com/archives/C049Y8M8G5Q/p1691657753060259?thread_ts=1691566944.227709&cid=C049Y8M8G5Q
    if last_scores is None or previous_scores is None:
        return STABLE

    scores_diff = last_scores - previous_scores

    if np.any(scores_diff > 1.5):
        return LARGE_INCREASE
    elif np.any(scores_diff >= -1.5) and np.any(scores_diff <= 1.5):
        return STABLE
    else:
        return LARGE_DECREASE


def _get_low_score(last_smq: Union[pd.Series, None]) -> int:
    """
    Returns priority of the scores difference between `last_smq` and `previous_smq`.
    """
    # Extracts relevant columns
    focused_columns = ['applicability', 'connection', 'content', 'progress', 'way_of_working']
    last_scores = None if last_smq is None else last_smq[focused_columns].copy()

    # Calculate criterion
    NONE = 0
    LOW_SCORE = 3

    if last_scores is not None and np.any(last_scores < 4.5):
        return LOW_SCORE

    return NONE
