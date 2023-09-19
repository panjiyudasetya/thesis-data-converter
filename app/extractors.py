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
        Selects snapshot of the client's profile data from local storage
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
                'no_of_days_with_calls': 'Int64',
                'no_of_registrations': 'Int64',
                'start_time': str,
                'end_time': str,
            },
            parse_dates=['start_time', 'end_time']
        )
