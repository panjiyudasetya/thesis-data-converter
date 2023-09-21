import logging
import pandas as pd

from datetime import datetime
from dateutil.rrule import rrulestr
from typing import List, Dict

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


logger = logging.getLogger(__name__)
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


class PlannedEventCompletion:

    def read_snapshot(self) -> pd.DataFrame:
        """
        Generates planned event completion from the snapshots of the users, events,
        and event's reflections data.
        """
        # Load required snapshots
        clients = ClientInfo().read_snapshot()

        events = PlannedEvent().read_snapshot()
        events_reflections = PlannedEventReflection().read_snapshot()

        # Merge events with clients to get client `start_time` and `end_time`
        # that refers to when treatment is started / ended.
        events = events.merge(clients, on='client_id', suffixes=('', '_client'))

        # Filter out planned_events with start_time outside
        # the client's `start_time` and `end_time` range.
        current_date = settings.running_date()
        events = events[
            (events['start_time'] >= events['start_time_client']) &
            (events['start_time'] <= events['end_time_client'])
        ]

        # Find the minimum date of when the recurrent event must stop.
        events['calculated_end_time'] = self._coalesce(
            events, ['terminated_time', 'end_time', 'end_time_client']
        ).fillna(current_date)

        events['calculated_end_time'] = events['calculated_end_time'] + pd.Timedelta(days=1)

        # Create planned event completions dataframe.
        data = self._create_event_completions_data(events, events_reflections)
        events_completions = pd.DataFrame(data)

        # Stores planned event completions to the local storage.
        directory = f'{FILE_LOCATOR.event_completions[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.event_completions[FILE_LOCATOR.FILENAME]}'

        events_completions.to_csv(f'{directory}/{filename}', float_format='%g', index=False)

        return events_completions

    def _coalesce(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Fills the NA/NaN values by using the next valid observation to fill the gap
        and then returns all rows on the first column.
        """
        # Fills the NA/NaN values by using the next valid observation to fill the gap
        df = df[columns].bfill(axis=1)

        # Returns all rows on the first column
        return df.iloc[:, 0]

    def _create_event_completions_data(self, events, events_reflections) -> List[Dict]:
        """
        Creates planned events completions dataset.
        """
        events_completions = []
        counter = 0

        for _, event in events.iterrows():
            if counter % 10 == 0:
                logger.info(f"Generating planned event's completions {counter}/{len(events)}...")

            counter += 1

            rrule_str = event['recurring_expression']['rrule']
            start_time = event['start_time']
            end_time = event['calculated_end_time']

            timestamps = rrulestr(rrule_str, dtstart=start_time).between(start_time, end_time, inc=True)

            for timestamp in timestamps:
                # Ignores the hours, minutes, and seconds of the instance time.
                instance_date = datetime.combine(timestamp.date(), datetime.min.time())

                # Filters event's reflections.
                actual_event = events_reflections[
                    (events_reflections['planned_event_id'] == event['id']) &
                    (events_reflections['start_time'] == instance_date)
                ]

                if not actual_event.empty:
                    status = actual_event.iloc[0]['status']
                else:
                    status = 'INCOMPLETED'

                events_completions.append({
                    'client_id': event['client_id'],
                    'planned_event_id': event['id'],
                    'start_time': instance_date,
                    'status': status
                })

        return events_completions


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
