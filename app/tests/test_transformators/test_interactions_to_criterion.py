import pandas as pd

from dateutil.parser import parse
from unittest import TestCase

from app.transformators.interactions_to_criterion import (
    interactions_to_criterion,
)


class TestInteractionsToCriterion(TestCase):
    """
    Test the `interactions_to_criterion` transformator.
    """

    def test_interactions_to_criterion_1(self):
        """
        Test to ensure the `interactions_to_criterion` method
        returns correct criterion.
        """
        snapshot_timestamp = parse('2023-09-05')
        sessions = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')]
        })
        communications = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-02')],
            'call_made': [False],
            'chat_msg_sent': [True]
        })

        # Assert no. of days difference between the snapshot timestamp and the last chat
        # * The timestamp of the last chat is chosen because it's greater than the last session
        actual = interactions_to_criterion(communications, sessions, snapshot_timestamp)
        expected = 3
        self.assertEqual(actual, expected)

    def test_interactions_to_criterion_2(self):
        """
        Test to ensure the `interactions_to_criterion` method
        returns correct criterion.
        """
        snapshot_timestamp = parse('2023-09-05')
        sessions = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-03')]
        })
        communications = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')],
            'call_made': [False],
            'chat_msg_sent': [True]
        })

        # Assert no. of days difference between the snapshot timestamp and the last session
        # * The timestamp of the last session is chosen because it's greater than the last chat message
        actual = interactions_to_criterion(communications, sessions, snapshot_timestamp)
        expected = 2
        self.assertEqual(actual, expected)

    def test_interactions_to_criterion_3(self):
        """
        Test to ensure the `interactions_to_criterion` method
        returns correct criterion.
        """
        snapshot_timestamp = parse('2023-09-05')
        sessions = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-03')]
        })
        communications = pd.DataFrame(data={
            'client_id': [],
            'start_time': [],
            'call_made': [],
            'chat_msg_sent': []
        })

        # Assert no. of days difference between the snapshot timestamp and the last session
        # * The timestamp of the last session is chosen when there is no communication data
        actual = interactions_to_criterion(communications, sessions, snapshot_timestamp)
        expected = 2
        self.assertEqual(actual, expected)

    def test_interactions_to_criterion_4(self):
        """
        Test to ensure the `interactions_to_criterion` method
        returns correct criterion.
        """
        snapshot_timestamp = parse('2023-09-05')
        sessions = pd.DataFrame(data={
            'client_id': [],
            'start_time': []
        })
        communications = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')],
            'call_made': [False],
            'chat_msg_sent': [True]
        })

        # Assert no. of days difference between the snapshot timestamp and the last session
        # * The timestamp of the last chat is chosen when there is no sessions
        actual = interactions_to_criterion(communications, sessions, snapshot_timestamp)
        expected = 4
        self.assertEqual(actual, expected)

    def test_interactions_to_criterion_5(self):
        """
        Test to ensure the `interactions_to_criterion` method
        returns correct criterion.
        """
        snapshot_timestamp = parse('2023-09-05')
        sessions = pd.DataFrame(data={
            'client_id': [],
            'start_time': []
        })
        communications = pd.DataFrame(data={
            'client_id': [],
            'start_time': [],
            'call_made': [],
            'chat_msg_sent': []
        })

        # Assert no. of days difference is None when there is no sessions and communication data
        actual = interactions_to_criterion(communications, sessions, snapshot_timestamp)
        self.assertIsNone(actual)
