import pandas as pd
from typing import Dict

from src.utils.data_extractor import TaxiDataExtractor
from src.utils.data_loader import DataLoader
from src.great_expectations.expectations import GreatExpectationsChecker


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


def load_taxi_data(url: str) -> pd.DataFrame:
    """Load data from the provided URL and return as a pandas DataFrame."""
    taxi_data_extractor = TaxiDataExtractor(url)
    taxi_data_extractor.load_data()
    return taxi_data_extractor.get_data()


def load_data_to_sql(df: pd.DataFrame):
    """Load DataFrame into SQL."""
    data_loader = DataLoader(df)
    data_loader.write_to_sql(
        table_name="stg_taxi_data",
        schema="stage",
        if_exists="append",
        index=False,
    )


def run_expectations(df: pd.DataFrame):
    """Run Great Expectations checks and generate data docs."""
    great_expectations_checker = GreatExpectationsChecker(df, CONTEXT_MODE)
    great_expectations_checker.set_data_source(DATA_SOURCE)
    great_expectations_checker.set_data_asset(DATA_ASSET_NAME)
    great_expectations_checker.set_data_docs_site(SITE_NAME, SITE_CONFIG)
    great_expectations_checker.set_batch_definition(BATCH_DEFINITION)
    great_expectations_checker.set_batch({"dataframe": df})
    great_expectations_checker.set_suite(SUITE_NAME)
    
    great_expectations_checker.create_expectations()
    great_expectations_checker.run_checkpoint(SITE_NAME)
    great_expectations_checker.generate_data_docs(SITE_NAME)
    great_expectations_checker.open_report()


def main():
    try:
        df = load_taxi_data(URL)

        load_data_to_sql(df)

        run_expectations(df)

    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    main()
