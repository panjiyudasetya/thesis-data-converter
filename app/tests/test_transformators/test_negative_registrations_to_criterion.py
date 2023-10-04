import pandas as pd

from datetime import timedelta
from dateutil.parser import parse
from unittest import TestCase

from app.transformators.negative_registrations_to_criterion import (
    negative_registrations_to_criterion,
    _total_neg_regs,
    _to_criterion,
)


class TestNegativeRegistrationsToCriterion(TestCase):
    """
    Test the `negative_registrations_to_criterion` transformator.
    """

    def test_negative_registrations_to_criterion_1(self):
        """
        Test to ensure the `negative_registrations_to_criterion` method
        returns correct criterion.
        """
        # Trackers in the past 7 days
        worries_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1' for _ in range(0, 2)],
            'start_time': [
                parse('2021-03-14T00:00:00') - timedelta(d)
                for d in range(0, 2)
            ],
            'name': ['measure_worry' for _ in range(0, 2)],
            'value': [{'duration': 900}, {}]
        })
        safety_behaviours_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_safety_behaviour'],
            'value': [{'boolean': True}]
        })
        avoidances_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_avoidance'],
            'value': [{'boolean': True}]
        })
        trackers_past_7d = pd.concat([
            worries_past_7d,
            safety_behaviours_past_7d,
            avoidances_past_7d
        ]).reset_index(drop=True)

        # Trackers on 1 week before the past 7 days
        worries_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_worry'],
            'value': [{'duration': 900}]
        })
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
            worries_1w_before_past_7d,
            safety_behaviours_1w_before_past_7d,
            avoidances_1w_before_past_7d
        ]).reset_index(drop=True)

        actual = negative_registrations_to_criterion(
            trackers_past_7d,
            trackers_1w_before_past_7d
        )
        expected = 3  # Big Increase
        self.assertEqual(actual, expected)

    def test_negative_registrations_to_criterion_2(self):
        """
        Test to ensure the `negative_registrations_to_criterion` method
        returns correct criterion.
        """
        # Trackers in the past 7 days
        worries_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_worry'],
            'value': [{'duration': 900}]
        })
        safety_behaviours_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_safety_behaviour'],
            'value': [{'boolean': True}]
        })
        avoidances_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_avoidance'],
            'value': [{'boolean': True}]
        })
        trackers_past_7d = pd.concat([
            worries_past_7d,
            safety_behaviours_past_7d,
            avoidances_past_7d
        ]).reset_index(drop=True)

        # Trackers on 1 week before the past 7 days
        worries_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_worry'],
            'value': [{'duration': 900}]
        })
        safety_behaviours_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_safety_behaviour'],
            'value': [{'boolean': True}]
        })
        avoidances_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_avoidance'],
            'value': [{'boolean': True}]
        })
        trackers_1w_before_past_7d = pd.concat([
            worries_1w_before_past_7d,
            safety_behaviours_1w_before_past_7d,
            avoidances_1w_before_past_7d
        ]).reset_index(drop=True)

        actual = negative_registrations_to_criterion(
            trackers_past_7d,
            trackers_1w_before_past_7d
        )
        expected = 1  # Stable
        self.assertEqual(actual, expected)

    def test_negative_registrations_to_criterion_3(self):
        """
        Test to ensure the `negative_registrations_to_criterion` method
        returns correct criterion.
        """
        # Trackers in the past 7 days
        worries_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_worry'],
            'value': [{'duration': 900}]
        })
        safety_behaviours_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_safety_behaviour'],
            'value': [{'boolean': True}]
        })
        avoidances_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_avoidance'],
            'value': [{'boolean': True}]
        })
        trackers_past_7d = pd.concat([
            worries_past_7d,
            safety_behaviours_past_7d,
            avoidances_past_7d
        ]).reset_index(drop=True)

        # Trackers on 1 week before the past 7 days
        worries_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_worry'],
            'value': [{'duration': 900}]
        })
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
            worries_1w_before_past_7d,
            safety_behaviours_1w_before_past_7d,
            avoidances_1w_before_past_7d
        ]).reset_index(drop=True)

        actual = negative_registrations_to_criterion(
            trackers_past_7d,
            trackers_1w_before_past_7d
        )
        expected = 2  # Small increase
        self.assertEqual(actual, expected)

    def test_negative_registrations_to_criterion_4(self):
        """
        Test to ensure the `negative_registrations_to_criterion` method
        returns correct criterion.
        """
        # Trackers in the past 7 days
        worries_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00')],
            'name': ['measure_worry'],
            'value': [{'duration': 900}]
        })
        safety_behaviours_past_7d = pd.DataFrame(data={
            'client_id': [],
            'start_time': [],
            'name': [],
            'value': []
        })
        avoidances_past_7d = pd.DataFrame(data={
            'client_id': [],
            'start_time': [],
            'name': [],
            'value': []
        })
        trackers_past_7d = pd.concat([
            worries_past_7d,
            safety_behaviours_past_7d,
            avoidances_past_7d
        ]).reset_index(drop=True)

        # Trackers on 1 week before the past 7 days
        worries_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_worry'],
            'value': [{'duration': 900}]
        })
        safety_behaviours_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_safety_behaviour'],
            'value': [{'boolean': True}]
        })
        avoidances_1w_before_past_7d = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2021-03-14T00:00:00') - timedelta(days=7)],
            'name': ['measure_avoidance'],
            'value': [{'boolean': True}]
        })
        trackers_1w_before_past_7d = pd.concat([
            worries_1w_before_past_7d,
            safety_behaviours_1w_before_past_7d,
            avoidances_1w_before_past_7d
        ]).reset_index(drop=True)

        actual = negative_registrations_to_criterion(
            trackers_past_7d,
            trackers_1w_before_past_7d
        )
        expected = 0  # Decrease
        self.assertEqual(actual, expected)

    def test_total_neg_regs(self):
        """
        Test to ensure the `_total_neg_regs` method returns correct number.
        """
        worries = pd.DataFrame(data={
            'client_id': ['cid-1' for _ in range(0, 2)],
            'start_time': [
                parse('2021-03-02T00:00:00') + timedelta(days=d)
                for d in range(0, 2)
            ],
            'name': ['measure_worry' for _ in range(0, 2)],
            'value': [{'duration': 900}, {}]
        })
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
        trackers = pd.concat([worries, safety_behaviours, avoidances]).reset_index(drop=True)

        # Assert total negative registrations
        # Indicated by:
        # - number of worry registrations
        # - total safety behaviour registrations with value equals `True`
        # - total avoidance registrations with value equals `True`
        actual = _total_neg_regs(trackers)
        expected = 4
        self.assertEqual(actual, expected)

    def test_to_criterion_1(self):
        """
        Test to ensure the `_to_criterion` method returns correct criterion.
        """
        actual = _to_criterion(120)
        expected = 3  # Big increase
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
        actual = _to_criterion(30)
        expected = 2  # Small increase
        self.assertEqual(actual, expected)

    def test_to_criterion_4(self):
        """
        Test to ensure the `_to_criterion` method returns correct criterion.
        """
        actual = _to_criterion(-20)
        expected = 0  # Decrease
        self.assertEqual(actual, expected)
