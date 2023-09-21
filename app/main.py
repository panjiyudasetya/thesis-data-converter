from app.extractors import MetabaseCollection
from app.loaders import Criteria


if __name__ == '__main__':
    # Extracts all collections from Metabase
    MetabaseCollection().collect_all()

    # Loads criteria data
    Criteria().load()
