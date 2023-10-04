import pandas as pd

from datetime import timedelta
from dateutil.parser import parse
from unittest import TestCase

from app.transformators.positive_registrations_to_criterion import (
    positive_registrations_to_criterion,
    _total_pos_regs,
    _to_criterion,
)


class TestPositiveRegistrationsToCriterion(TestCase):
    """
    Test the `positive_registrations_to_criterion` transformator.
    """

    def test_positive_registrations_to_criterion_1(self):
        """
        Test to ensure the `positive_registrations_to_criterion` method
        returns correct criterion.
        """
        # Trackers in the past 7 days
        safety_behaviours_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_safety_behaviour'],
            'value': [{'boolean': False}]
        })
        avoidances_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_avoidance'],
            'value': [{'boolean': False}]
        })
        trackers_past_7d = pd.concat([
            safety_behaviours_past_7d,
            avoidances_past_7d
        ]).reset_index(drop=True)

        # Trackers on 1 week before the past 7 days
        safety_behaviours_1w_before_past_7d = pd.DataFrame(data={
            'client_id': [],
            'start_time': [],
            'name': [],
            'value': []
        })
        avoidances_1w_before_past_7d = pd.DataFrame(data={
            'client_id': [],
            'start_time': [],
            'name': [],
            'value': []
        })
        trackers_1w_before_past_7d = pd.concat([
            safety_behaviours_1w_before_past_7d,
            avoidances_1w_before_past_7d
        ]).reset_index(drop=True)

        actual = positive_registrations_to_criterion(
            trackers_past_7d,
            trackers_1w_before_past_7d
        )
        expected = 2  # Increase
        self.assertEqual(actual, expected)

    def test_positive_registrations_to_criterion_2(self):
        """
        Test to ensure the `positive_registrations_to_criterion` method
        returns correct criterion.
        """
        # Trackers in the past 7 days
        safety_behaviours_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_safety_behaviour'],
            'value': [{'boolean': False}]
        })
        avoidances_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_avoidance'],
            'value': [{'boolean': False}]
        })
        trackers_past_7d = pd.concat([
            safety_behaviours_past_7d,
            avoidances_past_7d
        ]).reset_index(drop=True)

        # Trackers on 1 week before the past 7 days
        safety_behaviours_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_safety_behaviour'],
            'value': [{'boolean': False}]
        })
        avoidances_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_avoidance'],
            'value': [{'boolean': False}]
        })
        trackers_1w_before_past_7d = pd.concat([
            safety_behaviours_1w_before_past_7d,
            avoidances_1w_before_past_7d
        ]).reset_index(drop=True)

        actual = positive_registrations_to_criterion(
            trackers_past_7d,
            trackers_1w_before_past_7d
        )
        expected = 1  # Stable
        self.assertEqual(actual, expected)

    def test_positive_registrations_to_criterion_3(self):
        """
        Test to ensure the `positive_registrations_to_criterion` method
        returns correct criterion.
        """
        # Trackers in the past 7 days
        safety_behaviours_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_safety_behaviour'],
            'value': [{'boolean': False}]
        })
        avoidances_past_7d = pd.DataFrame(data={
            'client_id': [],
            'start_time': [],
            'name': [],
            'value': []
        })
        trackers_past_7d = pd.concat([
            safety_behaviours_past_7d,
            avoidances_past_7d
        ]).reset_index(drop=True)

        # Trackers on 1 week before the past 7 days
        safety_behaviours_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_safety_behaviour'],
            'value': [{'boolean': False}]
        })
        avoidances_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_avoidance'],
            'value': [{'boolean': False}]
        })
        trackers_1w_before_past_7d = pd.concat([
            safety_behaviours_1w_before_past_7d,
            avoidances_1w_before_past_7d
        ]).reset_index(drop=True)

        actual = positive_registrations_to_criterion(
            trackers_past_7d,
            trackers_1w_before_past_7d
        )
        expected = 0  # Decrease
        self.assertEqual(actual, expected)

    def test_total_pos_regs(self):
        """
        Test to ensure the `_total_pos_regs` method returns correct number.
        """
        safety_behaviours = pd.DataFrame(data={
            'client_id': ['cid-1' for _ in range(0, 2)],
            'start_time': [
                parse('2021-03-02T00:00:00') + timedelta(days=d)
                for d in range(0, 2)
            ],
            'name': ['measure_safety_behaviour' for _ in range(0, 2)],
            'value': [{'boolean': True}, {'boolean': False}]
        })
        avoidances = pd.DataFrame(data={
            'client_id': ['cid-1' for _ in range(0, 2)],
            'start_time': [
                parse('2021-03-02T00:00:00') + timedelta(days=d)
                for d in range(0, 2)
            ],
            'name': ['measure_avoidance' for _ in range(0, 2)],
            'value': [{'boolean': True}, {'boolean': False}]
        })
        trackers = pd.concat([safety_behaviours, avoidances]).reset_index(drop=True)

        # Assert total negative registrations
        # Indicated by:
        # - total safety behaviour registrations with value equals `False`
        # - total avoidance registrations with value equals `False`
        actual = _total_pos_regs(trackers)
        expected = 2
        self.assertEqual(actual, expected)

    def test_to_criterion_1(self):
        """
        Test to ensure the `_to_criterion` method returns correct criterion.
        """
        actual = _to_criterion(120)
        expected = 2  # Increase
        self.assertEqual(actual, expected)

    def test_to_criterion_2(self):
        """
        Test to ensure the `_to_criterion` method returns correct criterion.
        """
        actual = _to_criterion(-10)
        expected = 1  # Stable
        self.assertEqual(actual, expected)

    def test_to_criterion_3(self):
        """
        Test to ensure the `_to_criterion` method returns correct criterion.
        """
        actual = _to_criterion(-20)
        expected = 0  # Decrease
        self.assertEqual(actual, expected)
