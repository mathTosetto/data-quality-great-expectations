import pandas as pd

from typing import Dict
from src.utils.data_extractor import TaxiDataExtractor
from src.utils.data_loader import DataLoader
from src.great_expectations.expectations import GreatExpectationsChecker


if __name__ == "__main__":

    url: str = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    context_mode: str = "file"
    data_source: str = "pandas"
    data_asset_name: str = "Taxi Asset"
    site_name: str = "taxi_site"
    base_directory: str = "uncommitted/data_docs/local_site/"
    batch_definition: str = "taxi_batch_definition"
    suite_name: str = "taxi_suite_checks"

    site_config: Dict[str, str] = {
        "class_name": "SiteBuilder",
        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        "show_how_to_buttons": False,
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": base_directory,
        },
    }

    taxi_data_extractor: TaxiDataExtractor = TaxiDataExtractor(url)
    taxi_data_extractor.load_data()
    df: pd.DataFrame = taxi_data_extractor.get_data()

    data_loader: DataLoader = DataLoader(df)
    data_loader.write_to_sql(
        table_name="stg_taxi_data",
        schema="stage",
        if_exists="append",
        index=False,
    )

    great_expectations_checker: GreatExpectationsChecker = GreatExpectationsChecker(df, context_mode)
    great_expectations_checker.set_data_source(data_source)
    great_expectations_checker.set_data_asset(data_asset_name)
    great_expectations_checker.set_data_docs_site(site_name, site_config)
    great_expectations_checker.set_batch_definition(batch_definition)
    great_expectations_checker.set_batch({"dataframe": df})
    great_expectations_checker.set_suite(suite_name)
    great_expectations_checker.create_expectations()
    great_expectations_checker.run_checkpoint(site_name)
    great_expectations_checker.generate_data_docs(site_name)
    great_expectations_checker.open_report()
