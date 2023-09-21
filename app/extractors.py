import pandas as pd


from app.datasources.metabase import (
    ClientInfoAPI,
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
from app.helpers import to_dict
from app.settings import app_settings as settings


FILE_LOCATOR = settings.FILE_LOCATOR


class MetabaseCollection:

    def collect_all(self):
        """
        Pulls all of collection data from Metabase
        and stores in the local storage.
        """
        ClientInfoAPI().collect()
        CommunicationAPI().collect()
        CustomTrackerAPI().collect()
        DiaryEntryAPI().collect()
        NotificationAPI().collect()
        PlannedEventAPI().collect()
        PlannedEventReflectionAPI().collect()
        TherapySessionAPI().collect()
        ThoughtRecordAPI().collect()
        SMQAPI().collect()


class ClientInfo:

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the clients data from the local storage.
        """
        directory = f'{FILE_LOCATOR.clients[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.clients[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{filename}'

        return pd.read_csv(
            path,
            dtype={
                'client_id': str,
                'therapist_id': str,
                'start_time': str,
                'end_time': str,
                'no_of_registrations': 'int64',
            },
            parse_dates=['start_time', 'end_time']
        )


class Communication:

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the communication data from the local storage.
        """
        directory = f'{FILE_LOCATOR.communications[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.communications[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{filename}'

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

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the custom trackers data from the local storage.
        """
        directory = f'{FILE_LOCATOR.custom_trackers[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.custom_trackers[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{filename}'

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

        # Convert `value` into dictionary
        df['value'] = df['value'].apply(lambda item: to_dict(item))

        return df


class DiaryEntry:

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the diary entries data from the local storage.
        """
        directory = f'{FILE_LOCATOR.diary_entries[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.diary_entries[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{filename}'

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

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the notification data from the local storage.
        """
        directory = f'{FILE_LOCATOR.notifications[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.notifications[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{filename}'

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

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the planned event data from the local storage.
        """
        directory = f'{FILE_LOCATOR.events[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.events[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{filename}'

        # Read dataframe
        df = pd.read_csv(
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

        # Convert `recurring_expression` into dictionary
        df['recurring_expression'] = df['recurring_expression'].apply(lambda item: to_dict(item))

        return df


class PlannedEventReflection:

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the planned event's reflections data from the local storage.
        """
        directory = f'{FILE_LOCATOR.event_reflections[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.event_reflections[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{filename}'

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

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the therapy session data from the local storage.
        """
        directory = f'{FILE_LOCATOR.therapy_sessions[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.therapy_sessions[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{filename}'

        return pd.read_csv(
            path,
            dtype={
                'client_id': str,
                'start_time': str,
            },
            parse_dates=['start_time']
        )


class ThoughtRecord:

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the thought records data from the local storage.
        """
        directory = f'{FILE_LOCATOR.thought_records[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.thought_records[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{filename}'

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

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the Session Measurement Questionnaires (SMQ)
        data from the local storage.
        """
        directory = f'{FILE_LOCATOR.smqs[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.smqs[FILE_LOCATOR.FILENAME]}'

        path = f'{directory}/{filename}'

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
