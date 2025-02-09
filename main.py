import os
import logging
import pandas as pd

from src.utils.data_extractor import TaxiDataExtractor
from src.utils.data_loader import DataLoader
from src.great_expectations.expectations import GreatExpectationsPostgresChecker
from src.config.config import (
    URL,
    CONTEXT_MODE,
    SITE_NAME,
    BATCH_DEFINITION,
    SUITE_NAME,
    SITE_CONFIG,
)

logger: logging = logging.getLogger("class Main")


def load_taxi_data(url: str) -> pd.DataFrame:
    """Load data from the provided URL and return as a pandas DataFrame."""
    logger.info("Extracting taxi data from URL...")
    taxi_data_extractor = TaxiDataExtractor(url)
    taxi_data_extractor.load_data()
    return taxi_data_extractor.get_data()


def load_data_to_sql(df: pd.DataFrame) -> DataLoader:
    """Load DataFrame into SQL."""
    logger.info("Loading data into staging SQL table...")
    data_loader = DataLoader(df)
    data_loader.write_to_sql(
        table_name="stg_taxi_data",
        schema="stage",
        if_exists="append",
        index=False,
    )
    return data_loader


def run_expectations(df: pd.DataFrame) -> bool:
    """Run Great Expectations checks and generate data docs."""
    logger.info("Running Great Expectations validation...")

    great_expectations_checker = GreatExpectationsPostgresChecker(CONTEXT_MODE)
    great_expectations_checker.set_data_source(
        "taxi_data_data_source", os.getenv("CONNECTION_STRING")
    )
    great_expectations_checker.set_data_asset(
        "postgres_stg_taxi_data", "stg_taxi_data", "stage"
    )
    great_expectations_checker.set_data_docs_site(SITE_NAME, SITE_CONFIG)
    great_expectations_checker.set_batch_definition(BATCH_DEFINITION)
    great_expectations_checker.set_suite(SUITE_NAME)

    great_expectations_checker.create_expectations()
    result = great_expectations_checker.run_checkpoint(SITE_NAME)

    if result.success:
        logger.info("Great Expectations validation passed ✅")
    else:
        logger.warning("Great Expectations validation failed ❌")

    great_expectations_checker.generate_data_docs(SITE_NAME)

    return result.success


def validate_expectations(data_loader: DataLoader, is_expectation_validate: bool):
    """Validate data expectations and move data accordingly."""
    if is_expectation_validate:
        logger.info("Expectations passed, moving data to production table...")
        data_loader.write_to_sql(
            table_name="taxi_data",
            schema="production",
            if_exists="append",
            index=False,
        )
    else:
        logger.error("Expectations failed! Opening validation report.")
        GreatExpectationsPostgresChecker(CONTEXT_MODE).open_report()
        raise ValueError("Data validation failed! Please review your expectations.")


def main():
    """Main execution pipeline."""
    try:
        df: pd.DataFrame = load_taxi_data(URL)

        data_loader: DataLoader = load_data_to_sql(df)

        is_expectation_validate: bool = run_expectations(df)

        validate_expectations(data_loader, is_expectation_validate)

    except Exception as e:
        logger.exception(f"Error in pipeline execution: {e}")
        raise


if __name__ == "__main__":
    main()
