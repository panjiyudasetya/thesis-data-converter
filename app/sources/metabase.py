import logging
import os

from cryptography.fernet import Fernet
from datetime import datetime
from requests import (
    post,
    request,
    HTTPError,
    Response
)
from typing import Dict, List, Union

from app.settings import (
    app_settings as settings,
    Development
)


logger = logging.getLogger(__name__)
FILE_LOCATOR = settings.FILE_LOCATOR


def download_tracer(response,  *args, **kwargs) -> None:
    """
    A hook function to print-out response from the download requests.
    """
    timestamp = datetime.now().isoformat()

    endpoint = f"{timestamp}: {response.request.method} {response.url}"
    res_code = f"{response.status_code} {response.reason}"
    logger.info(f"\n{endpoint} - {res_code}")


def api_tracer(response, *args, **kwargs) -> None:
    """
    A hook function to print-out response from the api requests.
    """
    timestamp = datetime.now().isoformat()

    endpoint = f"{timestamp}: {response.request.method} {response.url}"
    res_code = f"{response.status_code} {response.reason}"
    res_data = f"{timestamp}: {str(response.content, 'utf-8')}"

    logger.info(f"\n{endpoint} - {res_code}\n{res_data}")


class BaseAPI:

    def __init__(self):
        self._debug_mode = True if isinstance(settings, Development) else False

        self._api_url = settings.METABASE_URL + '/api'

    def _api_request(
        self,
        method: str,
        path: str,
        session_id: str = '',
        payload: Union[Dict, List, None] = None,
        custom_headers: Dict = {}) -> Response:
        """
        Execute Metabase API requests.
        """
        url = self._api_url + path
        hooks = {'response': api_tracer} if self._debug_mode else None

        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        headers.update(custom_headers)

        if session_id:
            headers.update({"X-Metabase-Session": session_id})

        response = request(method, url, json=payload, headers=headers, hooks=hooks)
        response.raise_for_status()

        return response

    def _download_file(
        self,
        path: str,
        save_to: str,
        format: str,
        session_id: str,
        payload: Union[Dict, None] = None,
        custom_headers: Dict = {}
    ) -> None:
        """
        Download datasource from Metabase.
        """
        # Validate incoming download format
        if format not in ['json', 'csv', 'xlsx', 'api']:
            raise ValueError(f'{format} is invalid format.')

        # Setup download directories
        dirs = save_to.split('/')
        size_dirs = len(dirs)

        save_to_dirs = dirs[:size_dirs - 1] if size_dirs > 1 else None
        if not os.path.exists(save_to):
            os.makedirs('/'.join(save_to_dirs))

        # Setup download location
        save_as_file = dirs[size_dirs - 1]
        if save_to_dirs:
            save_as_file = f"{'/'.join(save_to_dirs)}/{save_as_file}"

        # Download file
        url = self._api_url + path
        hooks = {'response': download_tracer} if self._debug_mode else None

        headers = {"Accept": "*/*", "X-Metabase-Session": session_id}
        headers.update(custom_headers)

        chunk_size = 4096

        with post(url, data=payload, headers=headers, stream=True, hooks=hooks) as req:
            with open(save_to, 'wb') as file:
                # Writes response data in chunk
                for chunk in req.iter_content(chunk_size):
                    if not chunk:
                        continue

                    file.write(chunk)


class MetabaseAPI(BaseAPI):

    def __init__(self):
        super().__init__()

        self._session_file = '.metabase.session.tmp'
        self._session_id = self._get_session()

    def _get_session(self) -> str:
        """
        Reads the user's session ID from local file and validates it.
        If absent or invalid, it generates a new token and writes it to the temp file.

        Returns the session ID.
        """
        session_id = self._read_session_id()
        if session_id and self._is_valid(session_id):
            return session_id

        return self._request_session()

    def _read_session_id(self) -> Union[str, None]:
        """
        Read session ID from the temporary file.

        Returns the session ID or None.
        """
        if not os.path.exists(self._session_file):
            return None

        with open(self._session_file, "r") as file_session_id:
            try:
                return file_session_id.read()
            except Exception:
                return None

    def _is_valid(self, session_id: str) -> bool:
        """
        Returns True if that `session_id` is valid.
        If we can retrieve Metabase user profile belonging to the given `session_id`,
        we assume that session is valid.

        Normally, the Metabase session ID is valid for up to 14 days.
        @see https://www.metabase.com/learn/administration/metabase-api
        """
        method = 'GET'
        path = '/user/current'

        try:
            self._api_request(method, path, session_id)
        except HTTPError:
            return False

        return True

    def _request_session(self) -> str:
        """
        Login to the Metabase using predefine service account
        and writes the session ID to the temp file.
        """
        method = 'POST'
        path = '/session'

        payload = {
            'username': self._decrypt(settings.METABASE_SERVICE_ACCOUNT),
            'password': self._decrypt(settings.METABASE_SERVICE_ACCOUNT_PASSWORD)
        }
        response = self._api_request(method, path, payload=payload)

        session_id = response.json().get('id')
        self._write_session(session_id)

        return session_id

    def _write_session(self, session_id: str) -> None:
        """
        Write that `session_id` into disk.
        """
        with open(self._session_file, "w") as file_session_id:
            file_session_id.write(session_id)

    def _decrypt(self, encrypted_text):
        """
        Decrypts an encrypted text
        """
        return Fernet(settings.SECRET_KEY).decrypt(encrypted_text).decode('ascii')


class ClientProfileAPI(MetabaseAPI):

    CARD_ID = 2221

    def download(self, format='csv') -> None:
        """
        Downloads clients' profiles from Metabase in CSV format.
        """
        path = f'/card/{ClientProfileAPI.CARD_ID}/query/{format}'

        directory = f'{FILE_LOCATOR.clients[FILE_LOCATOR.DIR]}'
        filename = f'{FILE_LOCATOR.clients[FILE_LOCATOR.FILENAME]}'

        save_to = f'{directory}/{filename}'

        self._download_file(path, save_to, format, self._get_session())
