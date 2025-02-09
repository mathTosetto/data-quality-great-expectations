import os
from typing import Dict

URL: str = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
CONTEXT_MODE: str = "file"
DATA_SOURCE: str = "pandas"
DATA_ASSET_NAME: str = "Taxi Asset"
SITE_NAME: str = "taxi_site"
BASE_DIRECTORY: str = "uncommitted/data_docs/local_site/"
BATCH_DEFINITION: str = "taxi_batch_definition"
SUITE_NAME: str = "taxi_suite_checks"

SITE_CONFIG: Dict[str, str] = {
    "class_name": "SiteBuilder",
    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    "show_how_to_buttons": False,
    "store_backend": {
        "class_name": "TupleFilesystemStoreBackend",
        "base_directory": BASE_DIRECTORY,
    },
}
