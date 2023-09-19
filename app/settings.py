import logging
import os
import time

from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Union


load_dotenv()


class FileLocator:
    """
    A class that defines location map of the generated temporary files
    of the XAI `app` project
    """
    DIR = 'DIRECTORY'
    FILENAME = 'FILENAME'

    def __init__(self, root_dir: str = 'datasources'):
        self.root_dir = root_dir

    @property
    def clients(self) -> Dict:
        """
        Returns dictionary of the file's location of the user data.
        """
        return {
            FileLocator.DIR: f'{self.root_dir}',
            FileLocator.FILENAME: 'users.csv'
        }

    @property
    def communications(self) -> Dict:
        """
        Returns dictionary of the file's location of the client's communication data.
        """
        return {
            FileLocator.DIR: f'{self.root_dir}',
            FileLocator.FILENAME: 'communications.csv'
        }

    @property
    def custom_trackers(self) -> Dict:
        """
        Returns dictionary of the file's location of the client's custom tracker data.
        """
        return {
            FileLocator.DIR: f'{self.root_dir}',
            FileLocator.FILENAME: 'custom_trackers.csv'
        }

    @property
    def diary_entries(self) -> Dict:
        """
        Returns dictionary of the file's location of the client's diary entry data.
        """
        return {
            FileLocator.DIR: f'{self.root_dir}',
            FileLocator.FILENAME: 'diary_entries.csv'
        }

    @property
    def notifications(self) -> Dict:
        """
        Returns dictionary of the file's location of the notification data.
        """
        return {
            FileLocator.DIR: f'{self.root_dir}',
            FileLocator.FILENAME: 'notifications.csv'
        }

    @property
    def events(self) -> Dict:
        """
        Returns dictionary of the file's location of the planned events data.
        """
        return {
            FileLocator.DIR: f'{self.root_dir}',
            FileLocator.FILENAME: 'planned_events.csv'
        }

    @property
    def event_reflections(self) -> Dict:
        """
        Returns dictionary of the file's location of the planed event's reflections data.
        """
        return {
            FileLocator.DIR: f'{self.root_dir}',
            FileLocator.FILENAME: 'planned_event_reflections.csv'
        }

    @property
    def therapy_sessions(self) -> Dict:
        """
        Returns dictionary of the file's location of the client's therapy session data.
        """
        return {
            FileLocator.DIR: f'{self.root_dir}',
            FileLocator.FILENAME: 'therapy_sessions.csv'
        }

    @property
    def thought_records(self) -> Dict:
        """
        Returns dictionary of the file's location of the client's thought record data.
        """
        return {
            FileLocator.DIR: f'{self.root_dir}',
            FileLocator.FILENAME: 'thought_records.csv'
        }

    @property
    def smqs(self) -> Dict:
        """
        Returns dictionary of the file's location of the SMQ data.
        """
        return {
            FileLocator.DIR: f'{self.root_dir}',
            FileLocator.FILENAME: 'smqs.csv'
        }

    @property
    def criteria(self) -> Dict:
        """
        Returns dictionary of the file's location of the criteria data.
        """
        return {
            FileLocator.DIR: 'input/',
            FileLocator.FILENAME: 'criteria.csv'
        }


class CommonSetting:
    """
    A class that provides common settings for each environment
    """

    # Datasource variables
    METABASE_URL = os.environ.get('METABASE_URL', '')
    METABASE_SERVICE_ACCOUNT = os.environ.get('METABASE_SERVICE_ACCOUNT', '')
    METABASE_SERVICE_ACCOUNT_PASSWORD = os.environ.get('METABASE_SERVICE_ACCOUNT_PASSWORD', '')

    # App variables
    SECRET_KEY=os.environ.get('SECRET_KEY', '')
    RUN_FOR_SPECIFIC_DATE=os.environ.get('RUN_FOR_SPECIFIC_DATE', '')

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

    def __init__(self) -> None:
        # Decrease minimum modify app log level into the `INFO` flag.
        logging.root.setLevel(logging.INFO)
        logging.basicConfig(level=logging.INFO)


class Production(CommonSetting):
    """
    The class settings to run the `app` converter in production environment.
    """
    pass


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
