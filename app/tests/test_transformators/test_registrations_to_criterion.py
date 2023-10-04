import pandas as pd

from dateutil.parser import parse
from unittest import TestCase

from app.transformators.registrations_to_criterion import (
    registrations_to_criterion,
)


class TestRegistrationsToCriterion(TestCase):
    """
    Test the `registrations_to_criterion` transformator.
    """

    def test_registrations_to_criterion(self):
        """
        Test to ensure the `registrations_to_criterion` method
        returns correct criterion.
        """
        snapshot_timestamp = parse('2023-09-10')
        diary_entries = pd.DataFrame(data={
            'client_id': ['cid-1', 'cid-1'],
            'start_time': [parse('2023-09-03'), parse('2023-09-01')]
        })
        thought_records = pd.DataFrame(data={
            'client_id': ['cid-1', 'cid-1'],
            'start_time': [parse('2023-09-01'), parse('2023-09-04')]
        })
        smqs = pd.DataFrame(data={
            'client_id': ['cid-1', 'cid-1'],
            'start_time': [parse('2023-09-04'), parse('2023-09-07')]
        })
        custom_trackers = pd.DataFrame(data={
            'client_id': ['cid-1', 'cid-1'],
            'start_time': [parse('2023-09-01'), parse('2023-09-06')]
        })

        # Assert no. of days between the snapshot timestamp and the last registration date
        # * The maximum date of the SMQ registration is chosen because it's greater than the other registration dates.
        actual = registrations_to_criterion(diary_entries, thought_records, smqs, custom_trackers, snapshot_timestamp)
        expected = 3
        self.assertEqual(actual, expected)
