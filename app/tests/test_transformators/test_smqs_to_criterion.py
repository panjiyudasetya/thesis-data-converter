import pandas as pd

from dateutil.parser import parse
from unittest import TestCase

from app.transformators.smqs_to_criterion import (
    smqs_to_criterion,
)


class TestSMQsToCriterion(TestCase):
    """
    Test the `smqs_to_criterion` transformator.
    """

    def test_smqs_to_criterion_1(self):
        """
        Test to ensure the `smqs_to_criterion` method
        returns correct criterion when the last SMQ is None.
        """
        last_smq = pd.DataFrame(data={
            'client_id': [],
            'start_time': [],
            'applicability': [],
            'connection': [],
            'content': [],
            'progress': [],
            'way_of_working': [],
            'score': []
        })
        previous_smq = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')],
            'applicability': [7.0],
            'connection': [7.0],
            'content': [7.0],
            'progress': [7.0],
            'way_of_working': [7.0],
            'score': [35.0]
        })

        actual = smqs_to_criterion(last_smq, previous_smq)
        expected = 3  # Stable
        self.assertEqual(actual, expected)

    def test_smqs_to_criterion_2(self):
        """
        Test to ensure the `smqs_to_criterion` method
        returns correct criterion when the previous SMQ is None.
        """
        last_smq = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')],
            'applicability': [7.0],
            'connection': [7.0],
            'content': [7.0],
            'progress': [7.0],
            'way_of_working': [7.0],
            'score': [35.0]
        })
        previous_smq = pd.DataFrame(data={
            'client_id': [],
            'start_time': [],
            'applicability': [],
            'connection': [],
            'content': [],
            'progress': [],
            'way_of_working': [],
            'score': []
        })

        actual = smqs_to_criterion(last_smq, previous_smq)
        expected = 3  # Stable
        self.assertEqual(actual, expected)

    def test_smqs_to_criterion_3(self):
        """
        Test to ensure the `smqs_to_criterion` method
        returns correct criterion when at least one indicator in the last SMQ
        has scored below 4.5
        """
        last_smq = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')],
            'applicability': [4.0],  # The score below 4.5
            'connection': [7.0],
            'content': [7.0],
            'progress': [7.0],
            'way_of_working': [7.0],
            'score': [15.0]
        })
        previous_smq = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')],
            'applicability': [7.0],
            'connection': [7.0],
            'content': [7.0],
            'progress': [7.0],
            'way_of_working': [7.0],
            'score': [35.0]
        })

        actual = smqs_to_criterion(last_smq, previous_smq)
        expected = 1  # Low score
        self.assertEqual(actual, expected)

    def test_smqs_to_criterion_4(self):
        """
        Test to ensure the `smqs_to_criterion` method
        returns correct criterion when at least one indicator between the last SMQ
        and the previous SMQ has scored difference below -1.5
        """
        last_smq = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')],
            'applicability': [7.0],
            'connection': [7.0],
            'content': [7.0],
            'progress': [7.0],
            'way_of_working': [7.0],
            'score': [35.0]
        })
        previous_smq = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')],
            'applicability': [8.6],
            'connection': [7.0],
            'content': [7.0],
            'progress': [7.0],
            'way_of_working': [7.0],
            'score': [40.0]
        })

        actual = smqs_to_criterion(last_smq, previous_smq)
        expected = 0  # Large decrease
        self.assertEqual(actual, expected)

    def test_smqs_to_criterion_5(self):
        """
        Test to ensure the `smqs_to_criterion` method
        returns correct criterion when at least one indicator between the last SMQ
        and the previous SMQ has scored difference below -1.5
        """
        last_smq = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')],
            'applicability': [8.6],
            'connection': [7.0],
            'content': [7.0],
            'progress': [7.0],
            'way_of_working': [7.0],
            'score': [40.0]
        })
        previous_smq = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'start_time': [parse('2023-09-01')],
            'applicability': [7.0],
            'connection': [7.0],
            'content': [7.0],
            'progress': [7.0],
            'way_of_working': [7.0],
            'score': [35.0]
        })

        actual = smqs_to_criterion(last_smq, previous_smq)
        expected = 2  # Large increase
        self.assertEqual(actual, expected)
