from app.sources.metabase import (
    ClientProfileAPI,
    CommunicationAPI
)

class DataExtractor:

    def extract(self):
        """
        Extracts clients' profile from Metabase.
        """
        ClientProfileAPI().download()
        CommunicationAPI().download()
