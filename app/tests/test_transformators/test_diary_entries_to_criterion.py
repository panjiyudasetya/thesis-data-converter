import pandas as pd

from dateutil.parser import parse
from unittest import TestCase

from app.transformators.diary_entries_to_criterion import (
    diary_entries_to_criterion,
)


class TestDiaryEntriesToCriterion(TestCase):
    """
    Test the `diary_entries_to_criterion` transformator.
    """

    def test_diary_entries_to_criterion_1(self):
        """
        Test to ensure the `diary_entries_to_criterion` method
        returns correct criterion.
        """
        diary_entries = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')]
        })
        notifications = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'type': ['diary_entry_log'],
            'start_time': [parse('2023-09-01')]
        })

        actual = diary_entries_to_criterion(diary_entries, notifications)
        expected = 2  # Reminded and complete
        self.assertEqual(actual, expected)

    def test_diary_entries_to_criterion_2(self):
        """
        Test to ensure the `diary_entries_to_criterion` method
        returns correct criterion.
        """
        diary_entries = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')]
        })
        notifications = pd.DataFrame()

        actual = diary_entries_to_criterion(diary_entries, notifications)
        expected = 3  # Unreminded but complete
        self.assertEqual(actual, expected)

    def test_diary_entries_to_criterion_3(self):
        """
        Test to ensure the `diary_entries_to_criterion` method
        returns correct criterion.
        """
        diary_entries = pd.DataFrame()
        notifications = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'type': ['diary_entry_log'],
            'start_time': [parse('2023-09-01')]
        })

        actual = diary_entries_to_criterion(diary_entries, notifications)
        expected = 0  # Reminded and incomplete
        self.assertEqual(actual, expected)

    def test_diary_entries_to_criterion_4(self):
        """
        Test to ensure the `diary_entries_to_criterion` method
        returns correct criterion.
        """
        diary_entries = pd.DataFrame()
        notifications = pd.DataFrame()

        actual = diary_entries_to_criterion(diary_entries, notifications)
        expected = 1  # Unreminded and incomplete
        self.assertEqual(actual, expected)
