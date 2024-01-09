import pandas as pd

from dateutil.parser import parse
from unittest import TestCase

from app.transformators.thought_records_to_criterion import (
    thought_records_to_criterion,
)


class TestThoughtRecordsToCriterion(TestCase):
    """
    Test the `thought_records_to_criterion` transformator.
    """

    def test_thought_records_to_criterion_1(self):
        """
        Test to ensure the `thought_records_to_criterion` method
        returns correct criterion.
        """
        thought_records = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')]
        })
        notifications = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'type': ['gscheme_log'],
            'start_time': [parse('2023-09-01')]
        })

        actual = thought_records_to_criterion(thought_records, notifications)
        expected = (1, 3)  # Reminded and complete
        self.assertEqual(actual, expected)

    def test_thought_records_to_criterion_2(self):
        """
        Test to ensure the `thought_records_to_criterion` method
        returns correct criterion.
        """
        thought_records = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')]
        })
        notifications = pd.DataFrame()

        actual = thought_records_to_criterion(thought_records, notifications)
        expected = (3, 3)  # Unreminded but complete
        self.assertEqual(actual, expected)

    def test_thought_records_to_criterion_3(self):
        """
        Test to ensure the `thought_records_to_criterion` method
        returns correct criterion.
        """
        thought_records = pd.DataFrame()
        notifications = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'type': ['gscheme_log'],
            'start_time': [parse('2023-09-01')]
        })

        actual = thought_records_to_criterion(thought_records, notifications)
        expected = (1, 1)  # Reminded and incomplete
        self.assertEqual(actual, expected)

    def test_thought_records_to_criterion_4(self):
        """
        Test to ensure the `thought_records_to_criterion` method
        returns correct criterion.
        """
        thought_records = pd.DataFrame()
        notifications = pd.DataFrame()

        actual = thought_records_to_criterion(thought_records, notifications)
        expected = (3, 1)  # Unreminded and incomplete
        self.assertEqual(actual, expected)
