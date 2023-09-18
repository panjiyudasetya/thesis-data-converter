from app.sources.metabase import (
    ClientProfileAPI,
    CommunicationAPI,
    CustomTrackerAPI,
    DiaryEntryAPI,
    NotificationAPI,
    PlannedEventAPI,
    PlannedEventReflectionAPI,
    TherapySessionAPI,
    ThoughtRecordAPI
)

class DataExtractor:

    def extract(self):
        """
        Extracts raw data from Metabase.
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
