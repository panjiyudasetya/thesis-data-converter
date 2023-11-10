import pandas as pd


class StubClientInfo:

    def download(self) -> None:
        """
        Downloads clients' information from Metabase.
        """
        print('Stub `extractors.ClientInfo.download` is called.')

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the clients data from the local storage.
        """
        print('Stub `extractors.ClientInfo.read_snapshot` is called.')

        # Mock output dataframe
        data = {
            'client_id': [],
            'therapist_id': [],
            'start_time': [],
            'end_time': [],
            'no_of_registrations': []
        }

        # Return mock data as a dataframe
        return pd.DataFrame(
            data=data,
            dtype={
                'client_id': str,
                'therapist_id': str,
                'start_time': str,
                'end_time': str,
                'no_of_registrations': 'int64',
            }
        )


class StubCommunication:

    def download(self) -> None:
        """
        Downloads clients' communications from Metabase.
        """
        print('Stub `extractors.Communication.download` is called.')

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the communication data from the local storage.
        """
        print('Stub `extractors.Communication.read_snapshot` is called.')

        # Mock output dataframe
        data = {
            'client_id': [],
            'start_time': [],
            'call_made': [],
            'chat_msg_sent': []
        }

        # Return mock data as a dataframe
        return pd.DataFrame(
            data=data,
            dtype={
                'client_id': str,
                'start_time': str,
                'call_made': bool,
                'chat_msg_sent': bool,
            }
        )


class StubCustomTracker:

    # def __init__(self) -> None:
    #     print('Stub `extractors.CustomTracker` is created.')

    def download(self) -> None:
        """
        Downloads clients' custom trackers from Metabase.
        """
        print('Stub `extractors.CustomTracker.download` is called.')

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the custom trackers data from the local storage.
        """
        print('Stub `extractors.CustomTracker.read_snapshot` is called.')

        # Mock output dataframe
        data = {
            'client_id': [],
            'start_time': [],
            'name': [],
            'value': []
        }

        # Return mock data as a dataframe
        return pd.DataFrame(
            data=data,
            dtype={
                'client_id': str,
                'start_time': str,
                'name': str,
                'value': str,
            }
        )


class StubDiaryEntry:

    def download(self) -> None:
        """
        Downloads clients' diary entries from Metabase.
        """
        print('Stub `extractors.DiaryEntry.download` is called.')

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the diary entries data from the local storage.
        """
        print('Stub `extractors.DiaryEntry.read_snapshot` is called.')

        # Mock output dataframe
        data = {'client_id': [], 'start_time': []}

        # Return mock data as a dataframe
        return pd.DataFrame(
            data=data,
            dtype={'client_id': str, 'start_time': str}
        )


class StubNotification:

    def download(self) -> None:
        """
        Downloads notification data from Metabase.
        """
        print('Stub `extractors.Notification.download` is called.')

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the notification data from the local storage.
        """
        print('Stub `extractors.Notification.read_snapshot` is called.')

        # Mock output dataframe
        data = {'client_id': [], 'type': [], 'start_time': []}

        # Return mock data as a dataframe
        return pd.DataFrame(
            data=data,
            dtype={
                'client_id': str,
                'type': str,
                'start_time': str,
            }
        )


class StubPlannedEvent:

    # def __init__(self) -> None:
    #     print('Stub `extractors.PlannedEvent` is created.')

    def download(self) -> None:
        """
        Downloads planned events from Metabase.
        """
        print('Stub `extractors.PlannedEvent.download` is called.')

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the planned event data from the local storage.
        """
        print('Stub `extractors.PlannedEvent.read_snapshot` is called.')

        # Mock output dataframe
        data = {
            'id': [],
            'recurring_expression': [],
            'client_id': [],
            'created_at': [],
            'start_time': [],
            'end_time': [],
            'terminated_time': []
        }

        # Return mock data as a dataframe
        return pd.DataFrame(
            data=data,
            dtype={
                'id': str,
                'recurring_expression': str,
                'client_id': str,
                'created_at': str,
                'start_time': str,
                'end_time': str,
                'terminated_time': str,
            }
        )


class StubPlannedEventReflection:

    def download(self) -> None:
        """
        Downloads planned event's reflections from Metabase.
        """
        print('Stub `extractors.PlannedEventReflection.download` is called.')

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the planned event's reflections data from the local storage.
        """
        print('Stub `extractors.PlannedEventReflection.read_snapshot` is called.')

        # Mock output dataframe
        data = {
            'status': [],
            'planned_event_id': [],
            'start_time': []
        }

        # Return mock data as a dataframe
        return pd.DataFrame(
            data=data,
            dtype={
                'status': str,
                'planned_event_id': str,
                'start_time': str,
            }
        )


class StubTherapySession:

    def download(self) -> None:
        """
        Downloads therapy sessions from Metabase.
        """
        print('Stub `extractors.TherapySession.download` is called.')

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the therapy session data from the local storage.
        """
        print('Stub `extractors.TherapySession.read_snapshot` is called.')

        # Mock output dataframe
        data = {
            'client_id': [],
            'start_time': []
        }

        # Return mock data as a dataframe
        return pd.DataFrame(
            data=data,
            dtype={'client_id': str, 'start_time': str}
        )


class StubThoughtRecord:

    def download(self) -> None:
        """
        Downloads clients' thought records from Metabase.
        """
        print('Stub `extractors.ThoughtRecord.download` is called.')

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the thought records data from the local storage.
        """
        print('Stub `extractors.ThoughtRecord.read_snapshot` is called.')

        # Mock output dataframe
        data = {
            'client_id': [],
            'start_time': []
        }

        # Return mock data as a dataframe
        return pd.DataFrame(
            data=data,
            dtype={'client_id': str, 'start_time': str}
        )


class StubSMQ:

    def download(self) -> None:
        """
        Downloads SMQ results from Metabase.
        """
        print('Stub `extractors.SMQ.download` is called.')

    def read_snapshot(self) -> pd.DataFrame:
        """
        Selects snapshot of the Session Measurement Questionnaires (SMQ)
        data from the local storage.
        """
        print('Stub `extractors.SMQ.read_snapshot` is called.')

        # Mock output dataframe
        data = {
            'client_id': [],
            'start_time': [],
            'applicability': [],
            'connection': [],
            'content': [],
            'progress': [],
            'way_of_working': [],
            'score': []
        }

        # Return mock data as a dataframe
        return pd.DataFrame(
            data=data,
            dtype={
                'client_id': str,
                'start_time': str,
                'applicability': 'float64',
                'connection': 'float64',
                'content': 'float64',
                'progress': 'float64',
                'way_of_working': 'float64',
                'score': 'float64'
            }
        )
