from app.extractors import MetabaseCollection
from app.loaders import Criteria
from app.settings import app_settings as settings


if __name__ == '__main__':
    # Extracts all collections from Metabase
    # when necessary.
    if settings.USE_REMOTE_DATA:
        MetabaseCollection().download()

    # Loads criteria data
    Criteria().load()
