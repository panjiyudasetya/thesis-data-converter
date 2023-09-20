import pandas as pd


def negative_registrations_to_criterion(
    trackers_past_7d: pd.DataFrame,
    trackers_1w_before_past_7d: pd.DataFrame
) -> int:
    """
    Transforms these trackers (avoidance, safety behaviour, and worry) into criterion
    that refers to the comparison result of the total negative registrations made by the client
    between the last seven days (1-7) and the seven days before that (8-14).

    Negative registrations of the custom tracker defined as:
    - Avoidance tracker indicated by "value.boolean = True"
    - Safety behaviour tracker indicated by "value.boolean = True"
    - Any registrations from the worry tracker
    """
    total_neg_regs_past_7d = _total_neg_regs(trackers_past_7d.copy())
    total_neg_regs_1w_before_past_7d = _total_neg_regs(trackers_1w_before_past_7d.copy())

    # Compare those two values with this formula
    rate = (total_neg_regs_past_7d - total_neg_regs_1w_before_past_7d) / (total_neg_regs_1w_before_past_7d + 1)

    return _rate_to_criterion(rate * 100)

def _total_neg_regs(trackers: pd.DataFrame) -> int:
    """
    Returns total negative registrations from the given trackers.
    """
    # Get total negative avoidances
    avoidances = trackers[(trackers['name'] == 'measure_avoidance')]
    avoidances['is_negative_reg'] = avoidances['value'].apply(lambda item: bool(item['boolean']))
    total_avoidance = len(avoidances[(avoidances['is_negative_reg'] == True)].index)

    # Get total negative safety behaviours
    safety_behaviours = trackers[(trackers['name'] == 'measure_safety_behaviour')]
    safety_behaviours['is_negative_reg'] = safety_behaviours['value'].apply(lambda item: bool(item['boolean']))
    total_safety_behaviour = len(safety_behaviours[(safety_behaviours['is_negative_reg'] == True)].index)

    # Get total worries
    worries = trackers[(trackers['name'] == 'measure_worry')]
    total_worry = len(worries.index)

    return total_avoidance + total_safety_behaviour + total_worry


def _rate_to_criterion(percentage: int) -> int:
    """
    Transforms the negative registration percentage into the Deeploy's criterion.
    """
    TYPE_DECREASE = 0
    TYPE_STABLE = 1
    TYPE_SMALL_INCREASE = 2
    TYPE_BIG_INCREASE = 3

    if percentage > 100:
        return TYPE_BIG_INCREASE

    elif percentage > 20 and percentage <= 100:
        return TYPE_SMALL_INCREASE

    elif percentage > -20 and percentage <= 20:
        return TYPE_STABLE

    else:
        return TYPE_DECREASE
