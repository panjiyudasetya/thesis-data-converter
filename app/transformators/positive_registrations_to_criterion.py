import pandas as pd


def positive_registrations_to_criterion(
    trackers_past_7d: pd.DataFrame,
    trackers_1w_before_past_7d: pd.DataFrame
) -> int:
    """
    Transforms these two trackers (avoidance, safety behaviour) into criterion
    that refers to the comparison result of the total positive registrations made by the client
    between the last seven days (1-7) and the seven days before that (8-14).

    Positive registrations of the custom tracker defined as:
    - Avoidance tracker indicated by "value.boolean = False"
    - Safety behaviour tracker indicated by "value.boolean = False"
    """
    total_pos_regs_past_7d = _total_pos_regs(trackers_past_7d.copy())
    total_pos_regs_1w_before_past_7d = _total_pos_regs(trackers_1w_before_past_7d.copy())

    # Compare those two values with this formula
    rate = (total_pos_regs_past_7d - total_pos_regs_1w_before_past_7d) / (total_pos_regs_1w_before_past_7d + 1)

    return _rate_to_criterion(rate * 100)

def _total_pos_regs(trackers: pd.DataFrame) -> int:
    """
    Returns total positive registrations from the given trackers.
    """
    # Get total positive avoidances
    avoidances = trackers[(trackers['name'] == 'measure_avoidance')]
    avoidances['is_negative_reg'] = avoidances['value'].apply(lambda item: bool(item['boolean']))
    total_avoidance = len(avoidances[(avoidances['is_negative_reg'] == False)].index)

    # Get total positive safety behaviours
    safety_behaviours = trackers[(trackers['name'] == 'measure_safety_behaviour')]
    safety_behaviours['is_negative_reg'] = safety_behaviours['value'].apply(lambda item: bool(item['boolean']))
    total_safety_behaviour = len(safety_behaviours[(safety_behaviours['is_negative_reg'] == False)].index)

    return total_avoidance + total_safety_behaviour


def _rate_to_criterion(percentage: int) -> int:
    """
    Transforms the positive registration percentage into the Deeploy's criterion.
    """
    TYPE_DECREASE = 0
    TYPE_STABLE = 1
    TYPE_INCREASE = 2

    if percentage > 20:
        return TYPE_INCREASE

    elif percentage > -20 and percentage <= 20:
        return TYPE_STABLE

    else:
        return TYPE_DECREASE
