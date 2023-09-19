import pandas as pd

from datetime import date

from app.settings import app_settings as settings
from app.sources.metabase import (
    ClientProfileAPI,
    CommunicationAPI,
    CustomTrackerAPI,
    DiaryEntryAPI,
    NotificationAPI,
    PlannedEventAPI,
    PlannedEventReflectionAPI,
    TherapySessionAPI,
    ThoughtRecordAPI,
    SMQAPI,
)


FILE_LOCATOR = settings.FILE_LOCATOR


class MetabaseCollection:

    def extract_all(self):
        """
        Pulls all of collection data from Metabase
        and stores in the local storage.
        """
        ClientProfileAPI().extract()
        CommunicationAPI().extract()
        CustomTrackerAPI().extract()
        DiaryEntryAPI().extract()
        NotificationAPI().extract()
        PlannedEventAPI().extract()
        PlannedEventReflectionAPI().extract()
        TherapySessionAPI().extract()
        ThoughtRecordAPI().extract()
        SMQAPI().extract()


class ClientProfile:

    def select(self, snapshot_date: date) -> pd.DataFrame:
        """
        Selects snapshot of the clients data from local storage
        that have been downloaded at the given `snapshot_date`.
        """
        snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")

        directory = f'{FILE_LOCATOR.clients[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.clients[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{snapshot_date_str}/{filename}'

        return pd.read_csv(
            path,
            dtype={
                'client_id': str,
                'therapist_id': str,
                'no_of_days_with_calls': 'int64',
                'no_of_registrations': 'int64',
                'start_time': str,
                'end_time': str,
            },
            parse_dates=['start_time', 'end_time']
        )


class Communication:

    def select(self, snapshot_date: date) -> pd.DataFrame:
        """
        Selects snapshot of the communication data from local storage
        that have been downloaded at the given `snapshot_date`.
        """
        snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")

        directory = f'{FILE_LOCATOR.communications[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.communications[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{snapshot_date_str}/{filename}'

        return pd.read_csv(
            path,
            dtype={
                'client_id': str,
                'start_time': str,
                'call_made': bool,
                'chat_msg_sent': bool,
            },
            parse_dates=['start_time']
        )


class CustomTracker:

    def select(self, snapshot_date: date) -> pd.DataFrame:
        """
        Selects snapshot of the custom trackers data from local storage
        that have been downloaded at the given `snapshot_date`.
        """
        snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")

        directory = f'{FILE_LOCATOR.custom_trackers[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.custom_trackers[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{snapshot_date_str}/{filename}'

        # Read dataframe
        df = pd.read_csv(
            path,
            dtype={
                'client_id': str,
                'start_time': str,
                'name': str,
                'value': str,
            }
        )

        # The `start_time` column is a stringify date-time, 
        # and `parse_dates` property can't convert them automatically.
        # Therefore, we need to convert it to datetime object manually.
        df['start_time'] = pd.to_datetime(df['start_time'], format='ISO8601')
        return df


class DiaryEntry:

    def select(self, snapshot_date: date) -> pd.DataFrame:
        """
        Selects snapshot of the diary entries data from local storage
        that have been downloaded at the given `snapshot_date`.
        """
        snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")

        directory = f'{FILE_LOCATOR.diary_entries[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.diary_entries[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{snapshot_date_str}/{filename}'

        # Read dataframe
        df = pd.read_csv(
            path,
            dtype={
                'client_id': str,
                'start_time': str,
            }
        )

        # The `start_time` column is a stringify date-time, 
        # and `parse_dates` property can't convert them automatically.
        # Therefore, we need to convert it to datetime object manually.
        df['start_time'] = pd.to_datetime(df['start_time'], format='ISO8601')
        return df


class Notification:

    def select(self, snapshot_date: date) -> pd.DataFrame:
        """
        Selects snapshot of the notification data from local storage
        that have been downloaded at the given `snapshot_date`.
        """
        snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")

        directory = f'{FILE_LOCATOR.notifications[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.notifications[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{snapshot_date_str}/{filename}'

        return pd.read_csv(
            path,
            dtype={
                'client_id': str,
                'type': str,
                'start_time': str,
            },
            parse_dates=['start_time']
        )


class PlannedEvent:

    def select(self, snapshot_date: date) -> pd.DataFrame:
        """
        Selects snapshot of the planned event data from local storage
        that have been downloaded at the given `snapshot_date`.
        """
        snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")

        directory = f'{FILE_LOCATOR.events[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.events[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{snapshot_date_str}/{filename}'

        return pd.read_csv(
            path,
            dtype={
                'id': str,
                'recurring_expression': str,
                'client_id': str,
                'created_at': str,
                'start_time': str,
                'end_time': str,
                'terminated_time': str,
            },
            parse_dates=['start_time', 'end_time', 'terminated_time']
        )


class PlannedEventReflection:

    def select(self, snapshot_date: date) -> pd.DataFrame:
        """
        Selects snapshot of the planned event's reflections data from local storage
        that have been downloaded at the given `snapshot_date`.
        """
        snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")

        directory = f'{FILE_LOCATOR.event_reflections[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.event_reflections[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{snapshot_date_str}/{filename}'

        return pd.read_csv(
            path,
            dtype={
                'status': str,
                'planned_event_id': str,
                'start_time': str,
            },
            parse_dates=['start_time']
        )


class TherapySession:

    def select(self, snapshot_date: date) -> pd.DataFrame:
        """
        Selects snapshot of the therapy session data from local storage
        that have been downloaded at the given `snapshot_date`.
        """
        snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")

        directory = f'{FILE_LOCATOR.therapy_sessions[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.therapy_sessions[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{snapshot_date_str}/{filename}'

        return pd.read_csv(
            path,
            dtype={
                'client_id': str,
                'start_time': str,
            },
            parse_dates=['start_time']
        )


class ThoughtRecord:

    def select(self, snapshot_date: date) -> pd.DataFrame:
        """
        Selects snapshot of the thought records data from local storage
        that have been downloaded at the given `snapshot_date`.
        """
        snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")

        directory = f'{FILE_LOCATOR.thought_records[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.thought_records[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{snapshot_date_str}/{filename}'

        # Read dataframe
        df = pd.read_csv(
            path,
            dtype={
                'client_id': str,
                'start_time': str,
            }
        )

        # The `start_time` column is a stringify date-time, 
        # and `parse_dates` property can't convert them automatically.
        # Therefore, we need to convert it to datetime object manually.
        df['start_time'] = pd.to_datetime(df['start_time'], format='ISO8601')
        return df


class SMQ:

    def select(self, snapshot_date: date) -> pd.DataFrame:
        """
        Selects snapshot of the Session Measurement Questionnaires (SMQ)
        data from local storage that have been downloaded at the given `snapshot_date`.
        """
        snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")

        directory = f'{FILE_LOCATOR.smqs[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.smqs[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{snapshot_date_str}/{filename}'

        return pd.read_csv(
            path,
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
