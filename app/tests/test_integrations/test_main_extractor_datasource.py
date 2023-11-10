import responses
import warnings

from unittest import TestCase
from unittest.mock import patch, PropertyMock

from app.main import Main
from app.extractors import ClientInfo
from app.tests.test_integrations.stubs.datasources import (
    StubClientInfoAPI
)
from app.tests.test_integrations.stubs.extractors import (
    StubClientInfo,
    StubCommunication,
    StubCustomTracker,
    StubDiaryEntry,
    StubNotification,
    StubPlannedEvent,
    StubPlannedEventReflection,
    StubTherapySession,
    StubThoughtRecord,
    StubSMQ
)
from app.tests.test_integrations.stubs.loaders import StubCriteria


class TestIntegrationMainExtractorModules(TestCase):
    """
    Integration tests between `Main`, `extractors.*`,
    and `datasources.*` modules.
    """

    def setUp(self) -> None:
        # Patch settings to pull remote data
        self.patch_use_remote_data = patch('app.settings.app_settings.USE_REMOTE_DATA')
        mock_use_remote_data = self.patch_use_remote_data.start()
        mock_use_remote_data.return_value = True

        # Patch file locator for the clients info
        self.patch_clients_info_location = patch(
            'app.settings.FileLocator.clients',
            new_callable=PropertyMock)
        clients_info_location = self.patch_clients_info_location.start()
        clients_info_location.return_value = ('app/tests/test_integrations/snapshots', 'users.csv')

    def tearDown(self) -> None:
        # Stop patching the flag for pulling remote data
        self.patch_use_remote_data.stop()

        # Stop patching the flag to the file's locator
        self.patch_clients_info_location.stop()

    @patch('app.extractors.SMQ.__new__')
    @patch('app.extractors.ThoughtRecord.__new__')
    @patch('app.extractors.TherapySession.__new__')
    @patch('app.extractors.PlannedEventReflection.__new__')
    @patch('app.extractors.PlannedEvent.__new__')
    @patch('app.extractors.Notification.__new__')
    @patch('app.extractors.DiaryEntry.__new__')
    @patch('app.extractors.CustomTracker.__new__')
    @patch('app.extractors.Communication.__new__')
    @patch('app.extractors.ClientInfo.__new__')
    @patch('app.loaders.Criteria.__new__')
    def test_integration_main__metabase_collection(
        self,
        stub_criteria,
        stub_extractor_client_info,
        stub_extractor_communication,
        stub_extractor_custom_tracker,
        stub_extractor_diary_entry,
        stub_extractor_notification,
        stub_extractor_planned_event,
        stub_extractor_planned_event_reflection,
        stub_extractor_therapy_session,
        stub_extractor_thought_record,
        stub_extractor_smq
    ) -> None:
        """
        Integration tests between `Main` module
        and these `MetabaseCollection` extractor/interface (
            `extractors.MetabaseCollection`,
            `extractors.MetabaseCollection.download`
        )
        """
        # Activate stubs
        stub_criteria.return_value = StubCriteria()
        stub_extractor_client_info.return_value = StubClientInfo()
        stub_extractor_communication.return_value = StubCommunication()
        stub_extractor_custom_tracker.return_value = StubCustomTracker()
        stub_extractor_diary_entry.return_value = StubDiaryEntry()
        stub_extractor_notification.return_value = StubNotification()
        stub_extractor_planned_event.return_value = StubPlannedEvent()
        stub_extractor_planned_event_reflection.return_value = StubPlannedEventReflection()
        stub_extractor_therapy_session.return_value = StubTherapySession()
        stub_extractor_thought_record.return_value = StubThoughtRecord()
        stub_extractor_smq.return_value = StubSMQ()

        # Run main module
        Main()

    @patch('app.extractors.SMQ.__new__')
    @patch('app.extractors.ThoughtRecord.__new__')
    @patch('app.extractors.TherapySession.__new__')
    @patch('app.extractors.PlannedEventReflection.__new__')
    @patch('app.extractors.PlannedEvent.__new__')
    @patch('app.extractors.Notification.__new__')
    @patch('app.extractors.DiaryEntry.__new__')
    @patch('app.extractors.CustomTracker.__new__')
    @patch('app.extractors.Communication.__new__')
    @patch('app.extractors.ClientInfo.download')
    @patch('app.loaders.Criteria.__new__')
    def test_integration_main__metabase_collection__stub_client_info_download(
        self,
        stub_criteria,
        stub_extractor_client_info_download,
        stub_extractor_communication,
        stub_extractor_custom_tracker,
        stub_extractor_diary_entry,
        stub_extractor_notification,
        stub_extractor_planned_event,
        stub_extractor_planned_event_reflection,
        stub_extractor_therapy_session,
        stub_extractor_thought_record,
        stub_extractor_smq
    ) -> None:
        """
        Integration tests between `Main` module
        and these interface / extractor modules (
            `extractors.MetabaseCollection`,
            `extractors.MetabaseCollection.download`,
            `extractors.ClientInfo`,
            `extractors.ClientInfo.read_snapshot`
        )
        """
        # Activate stubs
        stub_criteria.return_value = StubCriteria()
        stub_extractor_client_info_download.side_effect = StubClientInfo().download()
        stub_extractor_communication.return_value = StubCommunication()
        stub_extractor_custom_tracker.return_value = StubCustomTracker()
        stub_extractor_diary_entry.return_value = StubDiaryEntry()
        stub_extractor_notification.return_value = StubNotification()
        stub_extractor_planned_event.return_value = StubPlannedEvent()
        stub_extractor_planned_event_reflection.return_value = StubPlannedEventReflection()
        stub_extractor_therapy_session.return_value = StubTherapySession()
        stub_extractor_thought_record.return_value = StubThoughtRecord()
        stub_extractor_smq.return_value = StubSMQ()

        # Run main module
        Main()

    @patch('app.datasources.metabase.ClientInfoAPI.__new__')
    @patch('app.extractors.SMQ.__new__')
    @patch('app.extractors.ThoughtRecord.__new__')
    @patch('app.extractors.TherapySession.__new__')
    @patch('app.extractors.PlannedEventReflection.__new__')
    @patch('app.extractors.PlannedEvent.__new__')
    @patch('app.extractors.Notification.__new__')
    @patch('app.extractors.DiaryEntry.__new__')
    @patch('app.extractors.CustomTracker.__new__')
    @patch('app.extractors.Communication.__new__')
    @patch('app.loaders.Criteria.__new__')
    def test_integration_main__metabase_collection__client_info__stub_metabase_api__stub_client_info_download(
        self,
        stub_criteria,
        stub_extractor_communication,
        stub_extractor_custom_tracker,
        stub_extractor_diary_entry,
        stub_extractor_notification,
        stub_extractor_planned_event,
        stub_extractor_planned_event_reflection,
        stub_extractor_therapy_session,
        stub_extractor_thought_record,
        stub_extractor_smq,
        stub_client_info_api,
    ) -> None:
        """
        Integration tests between `Main` module
        and these interface / extractor modules (
            `extractors.MetabaseCollection`,
            `extractors.MetabaseCollection.download`,
            `extractors.ClientInfo`,
            `extractors.ClientInfo.read_snapshot`
            `extractors.ClientInfo.download`
            `datasources.metabase.ClientInfo.download`
        )
        """
        # Activate stubs
        stub_criteria.return_value = StubCriteria()
        stub_extractor_communication.return_value = StubCommunication()
        stub_extractor_custom_tracker.return_value = StubCustomTracker()
        stub_extractor_diary_entry.return_value = StubDiaryEntry()
        stub_extractor_notification.return_value = StubNotification()
        stub_extractor_planned_event.return_value = StubPlannedEvent()
        stub_extractor_planned_event_reflection.return_value = StubPlannedEventReflection()
        stub_extractor_therapy_session.return_value = StubTherapySession()
        stub_extractor_thought_record.return_value = StubThoughtRecord()
        stub_extractor_smq.return_value = StubSMQ()
        stub_client_info_api.return_value = StubClientInfoAPI()

        # Run main module
        Main()

    @responses.activate
    @patch('app.datasources.metabase.MetabaseAPI._is_valid')
    @patch('app.extractors.SMQ.__new__')
    @patch('app.extractors.ThoughtRecord.__new__')
    @patch('app.extractors.TherapySession.__new__')
    @patch('app.extractors.PlannedEventReflection.__new__')
    @patch('app.extractors.PlannedEvent.__new__')
    @patch('app.extractors.Notification.__new__')
    @patch('app.extractors.DiaryEntry.__new__')
    @patch('app.extractors.CustomTracker.__new__')
    @patch('app.extractors.Communication.__new__')
    @patch('app.loaders.Criteria.__new__')
    def test_integration_main__metabase_collection__client_info__client_info_api(
        self,
        stub_criteria,
        stub_extractor_communication,
        stub_extractor_custom_tracker,
        stub_extractor_diary_entry,
        stub_extractor_notification,
        stub_extractor_planned_event,
        stub_extractor_planned_event_reflection,
        stub_extractor_therapy_session,
        stub_extractor_thought_record,
        stub_extractor_smq,
        mock_is_valid
    ) -> None:
        """
        Integration tests between `Main` module
        and these interface / extractor modules (
            `extractors.MetabaseCollection`,
            `extractors.MetabaseCollection.download`,
            `extractors.ClientInfo`,
            `extractors.ClientInfo.read_snapshot`
            `extractors.ClientInfo.download`
            `datasources.metabase.ClientInfo.download`
        )
        """
        warnings.simplefilter("ignore", ResourceWarning)

        # Mock metabase session
        mock_is_valid.return_value = 'False'
        responses.add(
            responses.POST,
            "https://metabase.sense-os.nl/session",
            body='this-is-your-dummy-session'
        )

        # Mock response of the client info snapshot
        mock_body = "client_id,therapist_id,start_time,end_time,no_of_registrations\n" \
            "ad695dd5e8d044b7e61d7122443d90e8,41e1ae94876807540a5ad4b3d1ef737c,2019-11-14,2020-02-07,13"

        responses.add(
            responses.POST,
            "https://metabase.sense-os.nl/api/card/2254/query/csv",
            body=mock_body
        )

        # Activate stubs
        stub_criteria.return_value = StubCriteria()
        stub_extractor_communication.return_value = StubCommunication()
        stub_extractor_custom_tracker.return_value = StubCustomTracker()
        stub_extractor_diary_entry.return_value = StubDiaryEntry()
        stub_extractor_notification.return_value = StubNotification()
        stub_extractor_planned_event.return_value = StubPlannedEvent()
        stub_extractor_planned_event_reflection.return_value = StubPlannedEventReflection()
        stub_extractor_therapy_session.return_value = StubTherapySession()
        stub_extractor_thought_record.return_value = StubThoughtRecord()
        stub_extractor_smq.return_value = StubSMQ()

        # Run main module
        Main()

        # Check client info read snapshots
        print('\nTesting path output for `ClientInfo > read_snapshot`')
        print(ClientInfo().read_snapshot())
