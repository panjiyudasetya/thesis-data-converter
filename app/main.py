from app.extractors import MetabaseCollection
from app.loaders import Criteria
from app.settings import app_settings as settings

if __name__ == '__main__':
    # Extracts all collections from Metabase
    MetabaseCollection().extract_all()

    # Loads criteria data for specific date
    for_date = settings.running_date()
    Criteria(for_date).load()
