from typing import Dict, Union


class StubMetabaseAPI:

    def __init__(self) -> None:
        self._session_id = self._get_session()

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
        print('Stub `MetabaseAPI._download_file` is called.')

    def _get_session(self) -> str:
        """
        Reads the user's session ID from local file and validates it.
        If absent or invalid, it generates a new token and writes it to the temp file.

        Returns the session ID.
        """
        print('Stub `MetabaseAPI._get_session` is called.')

        return 'this-is-your-dummy-session'


class StubClientInfoAPI(StubMetabaseAPI):

    # Refers to a dummy Metabase collection's ID
    # that is pointing to the client's card.
    card_id = 1234

    def download(self, format='csv') -> None:
        """
        Downloads client's information from Metabase in CSV format.
        """
        print('Stub `ClientInfoAPI.download` is called.')

        path = f'/card/{self.card_id}/query/{format}'

        directory, filename = ('app/tests/test_integrations/snapshots', 'users.csv',)
        save_to = f'{directory}/{filename}'

        self._download_file(path, save_to, format, self._session_id)
