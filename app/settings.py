import logging
import os
import time

from datetime import datetime
from dotenv import load_dotenv
from typing import Tuple, Union


load_dotenv()


class FileLocator:
    """
    A class that defines location map of the generated temporary files
    of the `app` project
    """

    def __init__(self, root_dir: str = 'snapshots'):
        self.root_dir = root_dir

    @property
    def clients(self) -> Tuple:
        """
        Returns tuple of directory and filename of the user data.
        """
        return (f'{self.root_dir}', 'users.csv')

    @property
    def communications(self) -> Tuple:
        """
        Returns tuple of directory and filename of the client's communication data.
        """
        return (f'{self.root_dir}', 'communications.csv')

    @property
    def custom_trackers(self) -> Tuple:
        """
        Returns tuple of directory and filename of the client's custom tracker data.
        """
        return (f'{self.root_dir}', 'custom_trackers.csv')

    @property
    def diary_entries(self) -> Tuple:
        """
        Returns tuple of directory and filename of the client's diary entry data.
        """
        return (f'{self.root_dir}', 'diary_entries.csv')

    @property
    def notifications(self) -> Tuple:
        """
        Returns tuple of directory and filename of the notification data.
        """
        return (f'{self.root_dir}', 'notifications.csv')

    @property
    def events(self) -> Tuple:
        """
        Returns tuple of directory and filename of the planned events data.
        """
        return (f'{self.root_dir}', 'planned_events.csv')

    @property
    def event_reflections(self) -> Tuple:
        """
        Returns tuple of directory and filename of the planed event's reflections data.
        """
        return (f'{self.root_dir}', 'planned_event_reflections.csv')

    @property
    def event_completions(self) -> Tuple:
        """
        Returns tuple of directory and filename of the event's completions data.
        """
        return (f'{self.root_dir}', 'planned_event_completions.csv')

    @property
    def therapy_sessions(self) -> Tuple:
        """
        Returns tuple of directory and filename of the client's therapy session data.
        """
        return (f'{self.root_dir}', 'therapy_sessions.csv')

    @property
    def thought_records(self) -> Tuple:
        """
        Returns tuple of directory and filename of the client's thought record data.
        """
        return (f'{self.root_dir}', 'thought_records.csv')

    @property
    def smqs(self) -> Tuple:
        """
        Returns tuple of directory and filename of the SMQ data.
        """
        return (f'{self.root_dir}', 'smqs.csv')

    @property
    def criteria(self) -> Tuple:
        """
        Returns tuple of directory and filename of the criteria data.
        """
        return ('outputs/', 'criteria.csv')


class CommonSetting:
    """
    A class that provides common settings for each environment
    """

    # Datasource variables
    METABASE_URL = os.environ.get('METABASE_URL', '')
    METABASE_SERVICE_ACCOUNT = os.environ.get('METABASE_SERVICE_ACCOUNT', '')
    METABASE_SERVICE_ACCOUNT_PASSWORD = os.environ.get('METABASE_SERVICE_ACCOUNT_PASSWORD', '')

    # App variables
    SECRET_KEY = os.environ.get('SECRET_KEY', '')
    RUN_FOR_SPECIFIC_DATE = os.environ.get('RUN_FOR_SPECIFIC_DATE', '')

    FILE_LOCATOR = FileLocator()

    @staticmethod
    def running_date():
        """
        Returns the date of when the app converter must run.
        """
        maximum_date = datetime.now().date()
        selected_date_str = os.environ.get('RUN_FOR_SPECIFIC_DATE')

        if selected_date_str:
            selected_time = time.strptime(selected_date_str, '%d/%m/%Y')
            selected_date = datetime.fromtimestamp(time.mktime(selected_time)).date()

            if selected_date > maximum_date:
                return maximum_date

            return selected_date

        return maximum_date


#
# Application settings
#

class Development(CommonSetting):
    """
    The class settings to run the `app` converter in local environment.
    """
    USE_REMOTE_DATA = False

    def __init__(self) -> None:
        # Decrease minimum modify app log level into the `INFO` flag.
        logging.root.setLevel(logging.INFO)
        logging.basicConfig(level=logging.INFO)


class Production(CommonSetting):
    """
    The class settings to run the `app` converter in production environment.
    """
    USE_REMOTE_DATA = True


#
# Setting's class loader
#

def load_settings() -> Union[Development, Production]:
    setting_class_map = {
        'develop': Development,
        'production': Production
    }

    return setting_class_map[os.environ.get('ENVIRONMENT', 'develop')]()


app_settings = load_settings()
