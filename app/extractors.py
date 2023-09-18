from app.sources.metabase import ClientProfileAPI

class DataExtractor:

    def extract(self):
        """
        Extracts clients' profile from Metabase.
        """
        ClientProfileAPI().download()
