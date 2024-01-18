import os
import pandas as pd
import warnings

from dateutil.parser import parse
from unittest import (
    mock,
    TestCase,
)

from app import loaders
from app.helpers import to_dict


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
    INPUT_DIR = f'{os.path.dirname(__file__)}/input'
    OUTPUT_DIR = f'{os.path.dirname(__file__)}/output'

    def setUp(self):
        self.class_loader = loaders.Criteria

        # Mock snapshot of the client info
        self.mock_read_snapshot_client_info = mock.patch('app.extractors.ClientInfo.read_snapshot')
        read_snapshot_client_info = self.mock_read_snapshot_client_info.start()
        read_snapshot_client_info.return_value = pd.read_csv(
            f'{TestCriteria.INPUT_DIR}/dummy_users.csv',
            dtype={
                'client_id': str,
                'therapist_id': str,
                'start_time': str,
                'end_time': str,
                'no_of_registrations': 'int64',
            },
            parse_dates=['start_time', 'end_time']
        )

        # Mock snapshot of the client's communications
        self.mock_read_snapshot_communication = mock.patch('app.extractors.Communication.read_snapshot')
        read_snapshot_communication = self.mock_read_snapshot_communication.start()
        read_snapshot_communication.return_value = pd.read_csv(
            f'{TestCriteria.INPUT_DIR}/dummy_communications.csv',
            dtype={
                'client_id': str,
                'start_time': str,
                'call_made': bool,
                'chat_msg_sent': bool,
            },
            parse_dates=['start_time']
        )

        # Mock snapshot of the client's custom trackers
        custom_trackers = pd.read_csv(
            f'{TestCriteria.INPUT_DIR}/dummy_custom_trackers.csv',
            dtype={
                'client_id': str,
                'start_time': str,
                'name': str,
                'value': str,
            }
        )
        custom_trackers['start_time'] = pd.to_datetime(custom_trackers['start_time'], format='ISO8601')
        custom_trackers['value'] = custom_trackers['value'].apply(lambda item: to_dict(item))

        self.mock_read_snapshot_custom_tracker = mock.patch('app.extractors.CustomTracker.read_snapshot')
        read_snapshot_custom_tracker = self.mock_read_snapshot_custom_tracker.start()
        read_snapshot_custom_tracker.return_value = custom_trackers

        # Mock snapshot of the client's diary entries
        diary_entries = pd.read_csv(
            f'{TestCriteria.INPUT_DIR}/dummy_diary_entries.csv',
            dtype={
                'client_id': str,
                'start_time': str,
                'name': str,
                'value': str,
            }
        )
        diary_entries['start_time'] = pd.to_datetime(diary_entries['start_time'], format='ISO8601')

        self.mock_read_snapshot_diary_entry = mock.patch('app.extractors.DiaryEntry.read_snapshot')
        read_snapshot_diary_entry = self.mock_read_snapshot_diary_entry.start()
        read_snapshot_diary_entry.return_value = diary_entries

        # Mock snapshot of the client's notifications
        self.mock_read_snapshot_notification = mock.patch('app.extractors.Notification.read_snapshot')
        read_snapshot_notification = self.mock_read_snapshot_notification.start()
        read_snapshot_notification.return_value = pd.read_csv(
            f'{TestCriteria.INPUT_DIR}/dummy_notifications.csv',
            dtype={
                'client_id': str,
                'type': str,
                'start_time': str,
            },
            parse_dates=['start_time']
        )

        # Mock snapshot of the client's event completions
        self.mock_read_snapshot_event_completion = mock.patch('app.extractors.PlannedEventCompletion.read_snapshot')
        read_snapshot_event_completion = self.mock_read_snapshot_event_completion.start()
        read_snapshot_event_completion.return_value = pd.read_csv(
            f'{TestCriteria.INPUT_DIR}/dummy_event_completions.csv',
            dtype={
                'client_id': str,
                'planned_event_id': str,
                'start_time': str,
                'status': str,
            },
            parse_dates=['start_time']
        )

        # Mock snapshot of the client's therapy sessions
        self.mock_read_snapshot_therapy_session = mock.patch('app.extractors.TherapySession.read_snapshot')
        read_snapshot_therapy_session = self.mock_read_snapshot_therapy_session.start()
        read_snapshot_therapy_session.return_value = pd.read_csv(
            f'{TestCriteria.INPUT_DIR}/dummy_therapy_sessions.csv',
            dtype={
                'client_id': str,
                'start_time': str,
            },
            parse_dates=['start_time']
        )

        # Mock snapshot of the client's thought records
        self.mock_read_snapshot_thought_record = mock.patch('app.extractors.ThoughtRecord.read_snapshot')
        read_snapshot_thought_record = self.mock_read_snapshot_thought_record.start()
        read_snapshot_thought_record.return_value = pd.read_csv(
            f'{TestCriteria.INPUT_DIR}/dummy_thought_records.csv',
            dtype={
                'client_id': str,
                'start_time': str,
            },
            parse_dates=['start_time']
        )

        # Mock snapshot of the client's SMQ
        self.mock_read_snapshot_smq = mock.patch('app.extractors.SMQ.read_snapshot')
        read_snapshot_smq = self.mock_read_snapshot_smq.start()
        read_snapshot_smq.return_value = pd.read_csv(
            f'{TestCriteria.INPUT_DIR}/dummy_therapy_sessions.csv',
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

    def tearDown(self):
        self.mock_read_snapshot_client_info.stop()
        self.mock_read_snapshot_communication.stop()
        self.mock_read_snapshot_custom_tracker.stop()
        self.mock_read_snapshot_diary_entry.stop()
        self.mock_read_snapshot_notification.stop()
        self.mock_read_snapshot_notification.stop()
        self.mock_read_snapshot_therapy_session.stop()
        self.mock_read_snapshot_thought_record.stop()
        self.mock_read_snapshot_smq.stop()

    @mock.patch.object(loaders.Criteria, 'load', mock_criteria_load)
    def test_load(self):
        """
        Test to ensure the `load` method produces correct dataset.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)

            criteria = self.class_loader()

            # Assert criteria dataset
            actual__dataframe = criteria.load()
            actual__dict = [
                {key: series.tolist()}
                for key, series in actual__dataframe.iterrows()
            ]
            expected__dataframe = pd.read_csv(
                f'{TestCriteria.OUTPUT_DIR}/expected_criteria.csv',
                dtype={
                    'case_id': str,
                    'case_created_at': str,
                    'client_id': str,
                    'p': 'int64',
                    'a__by_call': 'int64',
                    'a__by_chat': 'int64',
                    'b': 'int64',
                    'c': 'int64',
                    'd': 'int64',
                    'e': 'int64',
                    'f__is_scheduled': 'int64',
                    'f__completion_status': 'int64',
                    'g__is_reminder_activated': 'int64',
                    'g__is_completed': 'int64',
                    'h': 'int64',
                    'h__low_score': 'int64',
                    'i__is_reminder_activated': 'int64',
                    'i__is_completed': 'int64',
                }
            )
            expected__dict = [
                {key: series.tolist()}
                for key, series in expected__dataframe.iterrows()
            ]
            self.maxDiff = None
            self.assertListEqual(actual__dict, expected__dict)

    def test_compute_case_id(self):
        """
        Test to ensure the `compute_case_id` method returns correct Case ID.
        """
        criteria = self.class_loader()

        actual = criteria._compute_case_id('CID-1', 'TID-1', parse('2023-10-05'))
        expected = 'a3c2c63911d765afb8f6ec7bf69fcc1c'
        self.assertEqual(actual, expected)
