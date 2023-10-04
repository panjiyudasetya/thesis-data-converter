import itertools
import pandas as pd

from datetime import datetime
from typing import List, Dict


# Phases of the client's treatments
TREATMENT__PHASE_START = 0
TREATMENT__PHASE_MID = 1
TREATMENT__PHASE_END = 2

# Normally, clients does an online-treatment for 13 times to feel better.
# The first and second audio/video call aims to observe the client (intake sessions)
# and it doesn't counts as an online-treatment.
SESSION_COUNTS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]


def communications_to_treatment_snapshots(
    clients: pd.DataFrame,
    communications: pd.DataFrame
) -> List[Dict]:
    """
    Transforms the clients and their communications data
    into the time series before the first treatment is occurred.
    """
    client_treatment_lists = [
        _to_client_treatments(client, communications)
        for _, client in clients.iterrows()
    ]
    client_treatments = list(itertools.chain(*client_treatment_lists))

    snapshot_lists = [
        _create_snapshots_from(treatment, days_before=14)
        for treatment in client_treatments
    ]
    return list(itertools.chain(*snapshot_lists))


def _to_client_treatments(client: pd.Series, communications: pd.DataFrame) -> List[Dict]:
    """
    Returns the list of client's treatments.
    """
    # Filters communication data to the audio/video calls.
    calls = communications[
        (communications['client_id'] == client['client_id']) &
        (communications['call_made'])
    ]

    # Sorts audio/video calls in ascending order
    call_timestamps = calls['start_time'].sort_values().iloc[:]

    # Ensures the first timestamp of the client's audio/video call
    # equals to the timestamp of the client's first call (defined as `start_time`).
    first_call_date = call_timestamps.iloc[0]
    first_treatment_date = client['start_time']

    if first_call_date != first_treatment_date:
        raise ValueError(f"Communications data error for client: {client['client_id']}")

    # Generates state of the client's treatment.
    client_treatments = []

    for treatment, timestamp in enumerate(call_timestamps):
        # Validate session
        if treatment not in SESSION_COUNTS:
            continue

        # Client is in the beginning of treatment
        if treatment <= 3:
            phase = TREATMENT__PHASE_START

        # Client is in the middle of treatment
        elif treatment <= 8:
            phase = TREATMENT__PHASE_MID

        # Client is in the end of treatment
        else:
            phase = TREATMENT__PHASE_END

        client_treatments.append(_treatment_state_from(client, phase, timestamp))

    return client_treatments


def _create_snapshots_from(treatment: Dict, days_before: int) -> List[Dict]:
    """
    Returns snapshots of the client's treatment from the day(s)
    before the `treatment` is started.
    """
    return [
        _treatment_state_from(
            treatment['client_info'],
            treatment['treatment_phase'],
            treatment['treatment_timestamp'] - pd.Timedelta(days=day),
            deep_copy_series=True
        )
        for day in range(0, days_before)
    ]


def _treatment_state_from(
    client_info: pd.Series,
    treatment_phase: int,
    treatment_timestamp: datetime,
    deep_copy_series: bool = False
) -> Dict:
    """
    Returns the state of the client's treatment from the given values.
    """
    return {
        'client_info': client_info if not deep_copy_series else client_info.copy(deep=True),
        'treatment_phase': treatment_phase,
        'treatment_timestamp': treatment_timestamp
    }
