import pandas as pd
import warnings

from datetime import timedelta
from dateutil.parser import parse
from unittest import TestCase

from app.transformators.communications_to_treatment_snapshots import (
    communications_to_treatment_snapshots,
    _to_client_treatments,
    _create_snapshots_from,
    _treatment_state_from,
)


class TestCommunicationsToTreatmentSnapshots(TestCase):
    """
    Test the `communications_to_treatment_snapshots` transformator.
    """

    def test_communications_to_treatment_snapshots(self):
        """
        Test to ensure the `communications_to_treatment_snapshots` method
        returns correct snapshots for every client.
        """
        CLIENT_1_TOTAL_CALLS = 3

        client1 = pd.DataFrame(data={
            'client_id': ['cid-1'],
            'therapist_id': ['tid-1'],
            'start_time': [parse('2023-09-01')],
            'end_time': [parse('2023-09-30')],
            'no_of_registrations': [13]
        })
        client1_communications = pd.DataFrame(data={
            'client_id': ['cid-1' for _ in range(0, CLIENT_1_TOTAL_CALLS)],
            'start_time': [
                parse('2023-09-01') + timedelta(days=d)
                for d in range(0, CLIENT_1_TOTAL_CALLS)
            ],
            'call_made': [True for _ in range(0, CLIENT_1_TOTAL_CALLS)],
            'chat_msg_sent': [True for _ in range(0, CLIENT_1_TOTAL_CALLS)]
        })

        CLIENT_2_TOTAL_CALLS = 3

        client2 = pd.DataFrame(data={
            'client_id': ['cid-2'],
            'therapist_id': ['tid-2'],
            'start_time': [parse('2023-08-01')],
            'end_time': [parse('2023-08-30')],
            'no_of_registrations': [8]
        })
        client2_communications = pd.DataFrame(data={
            'client_id': ['cid-2' for _ in range(0, CLIENT_2_TOTAL_CALLS)],
            'start_time': [
                parse('2023-08-01') + timedelta(days=d)
                for d in range(0, CLIENT_2_TOTAL_CALLS)
            ],
            'call_made': [True for _ in range(0, CLIENT_2_TOTAL_CALLS)],
            'chat_msg_sent': [True for _ in range(0, CLIENT_2_TOTAL_CALLS)]
        })

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)

            client_info = pd.concat([client1, client2]).reset_index(drop=True)
            communications = pd.concat([client1_communications, client2_communications]).reset_index(drop=True)

            treatment_snapshots = communications_to_treatment_snapshots(client_info, communications)

            # Assert clients
            actual__clients = [t['client_info'].to_dict() for t in treatment_snapshots]
            expected__clients = [
                {
                    'client_id': 'cid-1',
                    'therapist_id': 'tid-1',
                    'start_time': parse('2023-09-01'),
                    'end_time': parse('2023-09-30'),
                    'no_of_registrations': 13
                }
                for _ in range(0, 14)  # Generated 14th times (client1 info for two weeks)
            ] + [
                {
                    'client_id': 'cid-2',
                    'therapist_id': 'tid-2',
                    'start_time': parse('2023-08-01'),
                    'end_time': parse('2023-08-30'),
                    'no_of_registrations': 8
                }
                for _ in range(0, 14)  # Generated 14th times (client2 info for two weeks)
            ]
            self.assertListEqual(actual__clients, expected__clients)

            # Assert treatments
            # * Ensures the first and second call timestamp aren't included.
            actual__treatments = [
                {'treatment_phase': t['treatment_phase'], 'treatment_timestamp': t['treatment_timestamp']}
                for t in treatment_snapshots
            ]
            expected__treatments = [
                {'treatment_phase': 0, 'treatment_timestamp': parse('2023-09-03') - timedelta(days=d)}
                for d in range(0, 14)  # Client1's treatment snapshots for two weeks before their first treatment
            ] + [
                {'treatment_phase': 0, 'treatment_timestamp': parse('2023-08-03') - timedelta(days=d)}
                for d in range(0, 14)  # Client2's treatment snapshots for two weeks before their first treatment
            ]
            self.assertListEqual(actual__treatments, expected__treatments)

    def test_to_client_treatments_1(self):
        """
        Test to ensure the `_to_client_treatments` method raise `ValueError`
        if the date of client's first call is not equal with the date of client's first treatment.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)

            client_info = pd.Series(
                data={
                    'client_id': 'cid-1',
                    'therapist_id': 'tid-1',
                    'start_time': parse('2023-09-01'),
                    'end_time': parse('2023-09-30'),
                    'no_of_registrations': 13
                },
                index=['client_id', 'therapist_id', 'start_time', 'end_time', 'no_of_registrations']
            )

            client_communications = pd.DataFrame(data={
                'client_id': ['cid-1'],
                'start_time': [parse('2023-09-02')],
                'call_made': [True],
                'chat_msg_sent': [True]
            })

            with self.assertRaises(ValueError):
                _to_client_treatments(client_info, client_communications)

    def test_to_client_treatments_2(self):
        """
        Test to ensure the `_to_client_treatments` method
        returns correct result for the client that is in the start of the treatment.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)

            client_info = pd.Series(
                data={
                    'client_id': 'cid-1',
                    'therapist_id': 'tid-1',
                    'start_time': parse('2023-09-01'),
                    'end_time': parse('2023-09-30'),
                    'no_of_registrations': 13
                },
                index=['client_id', 'therapist_id', 'start_time', 'end_time', 'no_of_registrations']
            )

            TOTAL_CALLS = 3

            client_communications = pd.DataFrame(data={
                'client_id': ['cid-1' for _ in range(0, TOTAL_CALLS)],
                'start_time': [
                    parse('2023-09-01') + timedelta(days=d)
                    for d in range(0, TOTAL_CALLS)
                ],
                'call_made': [True for _ in range(0, TOTAL_CALLS)],
                'chat_msg_sent': [True for _ in range(0, TOTAL_CALLS)]
            })

            client_treatments = _to_client_treatments(client_info, client_communications)

            # Assert clients
            actual__clients = [t['client_info'].to_dict() for t in client_treatments]
            expected__clients = [
                {
                    'client_id': 'cid-1',
                    'therapist_id': 'tid-1',
                    'start_time': parse('2023-09-01'),
                    'end_time': parse('2023-09-30'),
                    'no_of_registrations': 13
                }
            ]
            self.assertListEqual(actual__clients, expected__clients)

            # Assert treatments
            # * Ensures the first and second call timestamp aren't included.
            actual__treatments = [
                {'treatment_phase': t['treatment_phase'], 'treatment_timestamp': t['treatment_timestamp']}
                for t in client_treatments
            ]
            expected__treatments = [
                {'treatment_phase': 0, 'treatment_timestamp': parse('2023-09-03')}
            ]
            self.assertListEqual(actual__treatments, expected__treatments)

    def test_to_client_treatments_3(self):
        """
        Test to ensure the `_to_client_treatments` method
        returns correct result for the client that is in the middle of the treatment.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)

            client_info = pd.Series(
                data={
                    'client_id': 'cid-1',
                    'therapist_id': 'tid-1',
                    'start_time': parse('2023-09-01'),
                    'end_time': parse('2023-09-30'),
                    'no_of_registrations': 13
                },
                index=['client_id', 'therapist_id', 'start_time', 'end_time', 'no_of_registrations']
            )

            TOTAL_CALLS = 8

            client_communications = pd.DataFrame(data={
                'client_id': ['cid-1' for _ in range(0, TOTAL_CALLS)],
                'start_time': [
                    parse('2023-09-01') + timedelta(days=d)
                    for d in range(0, TOTAL_CALLS)
                ],
                'call_made': [True for _ in range(0, TOTAL_CALLS)],
                'chat_msg_sent': [True for _ in range(0, TOTAL_CALLS)]
            })

            client_treatments = _to_client_treatments(client_info, client_communications)

            # Assert clients
            actual__clients = [t['client_info'].to_dict() for t in client_treatments]
            expected__clients = [
                {
                    'client_id': 'cid-1',
                    'therapist_id': 'tid-1',
                    'start_time': parse('2023-09-01'),
                    'end_time': parse('2023-09-30'),
                    'no_of_registrations': 13
                }
                for _ in range(0, TOTAL_CALLS - 2)
            ]
            self.assertListEqual(actual__clients, expected__clients)

            # Assert treatments
            # * Ensures the first and second call timestamp aren't included.
            actual__treatments = [
                {'treatment_phase': t['treatment_phase'], 'treatment_timestamp': t['treatment_timestamp']}
                for t in client_treatments
            ]
            expected__treatments = [
                {'treatment_phase': 0, 'treatment_timestamp': parse('2023-09-03')},
                {'treatment_phase': 0, 'treatment_timestamp': parse('2023-09-04')},
                {'treatment_phase': 1, 'treatment_timestamp': parse('2023-09-05')},
                {'treatment_phase': 1, 'treatment_timestamp': parse('2023-09-06')},
                {'treatment_phase': 1, 'treatment_timestamp': parse('2023-09-07')},
                {'treatment_phase': 1, 'treatment_timestamp': parse('2023-09-08')}
            ]
            self.assertListEqual(actual__treatments, expected__treatments)

    def test_to_client_treatments_4(self):
        """
        Test to ensure the `_to_client_treatments` method
        returns correct result for the client that is in the end of the treatment.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)

            client_info = pd.Series(
                data={
                    'client_id': 'cid-1',
                    'therapist_id': 'tid-1',
                    'start_time': parse('2023-09-01'),
                    'end_time': parse('2023-09-30'),
                    'no_of_registrations': 13
                },
                index=['client_id', 'therapist_id', 'start_time', 'end_time', 'no_of_registrations']
            )

            TOTAL_CALLS = 11

            client_communications = pd.DataFrame(data={
                'client_id': ['cid-1' for _ in range(0, TOTAL_CALLS)],
                'start_time': [
                    parse('2023-09-01') + timedelta(days=d)
                    for d in range(0, TOTAL_CALLS)
                ],
                'call_made': [True for _ in range(0, TOTAL_CALLS)],
                'chat_msg_sent': [True for _ in range(0, TOTAL_CALLS)]
            })

            client_treatments = _to_client_treatments(client_info, client_communications)

            # Assert clients
            actual__clients = [t['client_info'].to_dict() for t in client_treatments]
            expected__clients = [
                {
                    'client_id': 'cid-1',
                    'therapist_id': 'tid-1',
                    'start_time': parse('2023-09-01'),
                    'end_time': parse('2023-09-30'),
                    'no_of_registrations': 13
                }
                for _ in range(0, TOTAL_CALLS - 2)
            ]
            self.assertListEqual(actual__clients, expected__clients)

            # Assert treatments
            # * Ensures the first and second call timestamp aren't included.
            actual__treatments = [
                {'treatment_phase': t['treatment_phase'], 'treatment_timestamp': t['treatment_timestamp']}
                for t in client_treatments
            ]
            expected__treatments = [
                {'treatment_phase': 0, 'treatment_timestamp': parse('2023-09-03')},
                {'treatment_phase': 0, 'treatment_timestamp': parse('2023-09-04')},
                {'treatment_phase': 1, 'treatment_timestamp': parse('2023-09-05')},
                {'treatment_phase': 1, 'treatment_timestamp': parse('2023-09-06')},
                {'treatment_phase': 1, 'treatment_timestamp': parse('2023-09-07')},
                {'treatment_phase': 1, 'treatment_timestamp': parse('2023-09-08')},
                {'treatment_phase': 1, 'treatment_timestamp': parse('2023-09-09')},
                {'treatment_phase': 2, 'treatment_timestamp': parse('2023-09-10')},
                {'treatment_phase': 2, 'treatment_timestamp': parse('2023-09-11')}
            ]
            self.assertListEqual(actual__treatments, expected__treatments)

    def test_create_snapshots_from(self):
        """
        Test to ensure the `_create_snapshots_from` method returns correct result.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)

            client_info = pd.Series(
                data={
                    'client_id': 'cid-1',
                    'therapist_id': 'tid-1',
                    'start_time': parse('2023-09-01'),
                    'end_time': parse('2023-09-30'),
                    'no_of_registrations': 13
                },
                index=['client_id', 'therapist_id', 'start_time', 'end_time', 'no_of_registrations']
            )

            client_treatment = {
                'client_info': client_info,
                'treatment_phase': 0,
                'treatment_timestamp': parse('2023-09-03')
            }

            snapshots = _create_snapshots_from(client_treatment, days_before=3)

            clients_info, treatments = [], []
            for snapshot in snapshots:
                clients_info.append(snapshot.pop('client_info').to_dict())
                treatments.append(snapshot)

            # Assert client info
            actual__clients_info = clients_info
            expected__clients_info = [
                {
                    'client_id': 'cid-1',
                    'therapist_id': 'tid-1',
                    'start_time': parse('2023-09-01'),
                    'end_time': parse('2023-09-30'),
                    'no_of_registrations': 13
                }
                for _ in range(0, 3)
            ]
            self.assertListEqual(actual__clients_info, expected__clients_info)

            # Assert treatment state
            actual__treatments = treatments
            expected__treatments = [
                {
                    'treatment_phase': 0,
                    'treatment_timestamp': parse('2023-09-03')
                },
                {
                    'treatment_phase': 0,
                    'treatment_timestamp': parse('2023-09-02')
                },
                {
                    'treatment_phase': 0,
                    'treatment_timestamp': parse('2023-09-01')
                }
            ]
            self.assertListEqual(actual__treatments, expected__treatments)

    def test_treatment_state_from(self):
        """
        Test to ensure the `_treatment_state_from` method returns correct result.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)

            client_info = pd.Series(
                data={
                    'client_id': 'cid-1',
                    'therapist_id': 'tid-1',
                    'start_time': parse('2023-09-01'),
                    'end_time': parse('2023-09-30'),
                    'no_of_registrations': 13
                },
                index=['client_id', 'therapist_id', 'start_time', 'end_time', 'no_of_registrations']
            )

            treatment_state = _treatment_state_from(client_info, 0, parse('2023-09-01'))

            # Assert client info
            actual__client_info = treatment_state.pop('client_info').to_dict()
            expected__client_info = {
                'client_id': 'cid-1',
                'therapist_id': 'tid-1',
                'start_time': parse('2023-09-01'),
                'end_time': parse('2023-09-30'),
                'no_of_registrations': 13
            }
            self.assertDictEqual(actual__client_info, expected__client_info)

            # Assert treatment state
            actual__treatment = treatment_state
            expected__treatment = {
                'treatment_phase': 0,
                'treatment_timestamp': parse('2023-09-01')
            }
            self.assertDictEqual(actual__treatment, expected__treatment)
