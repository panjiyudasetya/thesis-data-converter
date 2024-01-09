import pandas as pd

from dateutil.parser import parse
from unittest import TestCase

from app.transformators.planned_events_to_criterion import (
    planned_events_to_criterion,
)


class TestPlannedEventsToCriterion(TestCase):
    """
    Test the `planned_events_to_criterion` transformator.
    """

    def test_planned_events_to_criterion_1(self):
        """
        Test to ensure the `planned_events_to_criterion` method
        returns correct criterion.
        """
        events_completion = pd.DataFrame(data={
            'client_id': [],
            'planned_event_id': [],
            'start_time': [],
            'status': []
        })

        actual = planned_events_to_criterion(events_completion)
        expected = (3, 0)  # Unplanned, None
        self.assertEqual(actual, expected)

    def test_planned_events_to_criterion_2(self):
        """
        Test to ensure the `planned_events_to_criterion` method
        returns correct criterion.
        """
        events_completion = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'planned_event_id': ['pid-1'],
            'start_time': [parse('2023-09-01')],
            'status': ['COMPLETED']
        })

        actual = planned_events_to_criterion(events_completion)
        expected = (1, 3)  # Planned, complete
        self.assertEqual(actual, expected)

    def test_planned_events_to_criterion_3(self):
        """
        Test to ensure the `planned_events_to_criterion` method
        returns correct criterion.
        """
        events_completion = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'planned_event_id': ['pid-1'],
            'start_time': [parse('2023-09-01')],
            'status': ['INCOMPLETED']
        })

        actual = planned_events_to_criterion(events_completion)
        expected = (1, 1)  # Planned, incomplete
        self.assertEqual(actual, expected)

    def test_planned_events_to_criterion_4(self):
        """
        Test to ensure the `planned_events_to_criterion` method
        returns correct criterion.
        """
        events_completion = pd.DataFrame(data={
            'client_id': ['cid-1', 'cid-1'],
            'planned_event_id': ['pid-1', 'pid-2'],
            'start_time': [parse('2023-09-01'), parse('2023-09-02')],
            'status': ['COMPLETED', 'INCOMPLETED']
        })

        actual = planned_events_to_criterion(events_completion)
        expected = (1, 2)  # Planned, some complete
        self.assertEqual(actual, expected)
