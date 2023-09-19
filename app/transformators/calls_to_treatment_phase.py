import pandas as pd


def calls_to_treatment_phase(calls: pd.DataFrame) -> int:
    """
    Transforms the client's calls into the phase of treatment.
    """
    PHASE_START = 0  # Client is in the beginning of treatment
    PHASE_MID = 1  # Client is in the middle of treatment
    PHASE_END = 2  # Client is in the end of treatment

    total_calls = len(calls.index)

    if total_calls <= 3:
        return PHASE_START

    elif total_calls > 3 and total_calls <= 8:
        return PHASE_MID

    else:
        return PHASE_END
