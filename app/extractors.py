from app.sources.metabase import (
    ClientProfileAPI,
    CommunicationAPI,
    CustomTrackerAPI,
)

class DataExtractor:

    def extract(self):
        """
        Extracts raw data from Metabase.
        """
        ClientProfileAPI().download()
        CommunicationAPI().download()
        CustomTrackerAPI().download()
