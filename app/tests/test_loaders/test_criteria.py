import logging
import os
import pandas as pd
import warnings

from unittest import (
    mock,
    TestCase,
)

from app import loaders
from app.helpers import to_dict


logging.getLogger('app.loaders').setLevel(logging.WARNING)


def mock_criteria_load(self):
    """
    Mock the `Criteria.load()` method to return valid criteria dataset
    instead return nothing.
    """
    criteria = self._create()

    # Clean up criteria from duplicated rows and null values
    criteria = criteria.drop_duplicates().dropna()

    # Return valid criteria dataset
    return criteria.groupby('client_id').filter(self._valid_treatments).reset_index(drop=True)


class TestCriteria(TestCase):
    """
    Test the `Criteria` loader.
    """

    def setUp(self):
        self.class_loader = loaders.Criteria

    @mock.patch('app.extractors.SMQ.read_snapshot')
    @mock.patch('app.extractors.ThoughtRecord.read_snapshot')
    @mock.patch('app.extractors.TherapySession.read_snapshot')
    @mock.patch('app.extractors.PlannedEventCompletion.read_snapshot')
    @mock.patch('app.extractors.Notification.read_snapshot')
    @mock.patch('app.extractors.DiaryEntry.read_snapshot')
    @mock.patch('app.extractors.CustomTracker.read_snapshot')
    @mock.patch('app.extractors.Communication.read_snapshot')
    @mock.patch('app.extractors.ClientInfo.read_snapshot')
    def test_load(
        self,
        mock_read_client_snapshot,
        mock_read_communication_snapshot,
        mock_read_custom_tracker_snapshot,
        mock_read_diary_entry_snapshot,
        mock_read_notification_snapshot,
        mock_read_event_completion_snapshot,
        mock_read_therapy_session_snapshot,
        mock_read_thought_record_snapshot,
        mock_read_smq_snapshot
    ):
        """
        Test to ensure the `load` method produces correct dataset.
        """
        input_dir = f'{os.path.dirname(__file__)}/input'

        # Client info
        mock_read_client_snapshot.return_value = pd.read_csv(
            f'{input_dir}/dummy_users.csv',
            dtype={
                'client_id': str,
                'therapist_id': str,
                'start_time': str,
                'end_time': str,
                'no_of_registrations': 'int64',
            },
            parse_dates=['start_time', 'end_time']
        )

        # Communication
        mock_read_communication_snapshot.return_value = pd.read_csv(
            f'{input_dir}/dummy_communications.csv',
            dtype={
                'client_id': str,
                'start_time': str,
                'call_made': bool,
                'chat_msg_sent': bool,
            },
            parse_dates=['start_time']
        )

        # Custom tracker
        custom_trackers = pd.read_csv(
            f'{input_dir}/dummy_custom_trackers.csv',
            dtype={
                'client_id': str,
                'start_time': str,
                'name': str,
                'value': str,
            }
        )
        custom_trackers['start_time'] = pd.to_datetime(custom_trackers['start_time'], format='ISO8601')
        custom_trackers['value'] = custom_trackers['value'].apply(lambda item: to_dict(item))
        mock_read_custom_tracker_snapshot.return_value = custom_trackers

        # Diary entry
        diary_entries = pd.read_csv(
            f'{input_dir}/dummy_diary_entries.csv',
            dtype={
                'client_id': str,
                'start_time': str,
                'name': str,
                'value': str,
            }
        )
        diary_entries['start_time'] = pd.to_datetime(diary_entries['start_time'], format='ISO8601')
        mock_read_diary_entry_snapshot.return_value = diary_entries

        # Notification
        mock_read_notification_snapshot.return_value = pd.read_csv(
            f'{input_dir}/dummy_notifications.csv',
            dtype={
                'client_id': str,
                'type': str,
                'start_time': str,
            },
            parse_dates=['start_time']
        )

        # Event completions
        mock_read_event_completion_snapshot.return_value = pd.read_csv(
            f'{input_dir}/dummy_event_completions.csv',
            dtype={
                'client_id': str,
                'planned_event_id': str,
                'start_time': str,
                'status': str,
            },
            parse_dates=['start_time']
        )

        # Therapy session
        mock_read_therapy_session_snapshot.return_value = pd.read_csv(
            f'{input_dir}/dummy_therapy_sessions.csv',
            dtype={
                'client_id': str,
                'start_time': str,
            },
            parse_dates=['start_time']
        )

        # Thought record
        mock_read_thought_record_snapshot.return_value = pd.read_csv(
            f'{input_dir}/dummy_thought_records.csv',
            dtype={
                'client_id': str,
                'start_time': str,
            },
            parse_dates=['start_time']
        )

        # SMQ
        mock_read_smq_snapshot.return_value = pd.read_csv(
            f'{input_dir}/dummy_therapy_sessions.csv',
            dtype={
                'client_id': str,
                'start_time': str,
                'applicability': 'float64',
                'connection': 'float64',
                'content': 'float64',
                'progress': 'float64',
                'way_of_working': 'float64',
                'score': 'float64'
            },
            parse_dates=['start_time']
        )

        # Assert criteria dataset
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)

            with mock.patch.object(loaders.Criteria, 'load', mock_criteria_load):
                output_dir = f'{os.path.dirname(__file__)}/output'

                actual__dataframe = self.class_loader().load()
                expected__dataframe = pd.read_csv(
                    f'{output_dir}/expected_criteria.csv',
                    dtype={
                        'case_id': str,
                        'client_id': str,
                        'p': 'int64',
                        'a': 'int64',
                        'b': 'int64',
                        'c': 'int64',
                        'd': 'int64',
                        'e': 'int64',
                        'f': 'int64',
                        'g': 'int64',
                        'h': 'int64',
                        'i': 'int64'
                    }
                )

                actual__dict = [
                    {key: series.tolist()}
                    for key, series in actual__dataframe.iterrows()
                ]
                expected__dict = [
                    {key: series.tolist()}
                    for key, series in expected__dataframe.iterrows()
                ]
                self.assertListEqual(actual__dict, expected__dict)
