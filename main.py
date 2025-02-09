import os
import logging
import pandas as pd

from src.utils.data_extractor import TaxiDataExtractor
from src.utils.data_loader import DataLoader
from src.great_expectations_checker.postgres_checker import (
    GreatExpectationsPostgresChecker,
)
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
    logger.info("Extracting taxi data from URL: %s", url)
    extractor = TaxiDataExtractor(url)
    extractor.load_data()
    return extractor.get_data()


def load_data_to_sql(df: pd.DataFrame) -> DataLoader:
    """Load DataFrame into SQL staging table."""
    logger.info("Loading data into staging SQL table...")
    data_loader = DataLoader(df)
    data_loader.write_to_sql(
        table_name="stg_taxi_data",
        schema="stage",
        if_exists="append",
        index=False,
    )
    return data_loader


def run_expectations() -> bool:
    """Run Great Expectations checks and generate data docs."""
    logger.info("Running Great Expectations validation...")

    connection_string = os.getenv("CONNECTION_STRING")
    if not connection_string:
        logger.error(
            "Missing database connection string. Check your environment variables."
        )
        raise ValueError("Database connection string not set.")

    ge_checker = GreatExpectationsPostgresChecker(CONTEXT_MODE)
    ge_checker.set_data_source("taxi_data_source", connection_string)
    ge_checker.set_data_asset("postgres_stg_taxi_data", "stg_taxi_data", "stage")
    ge_checker.set_data_docs_site(SITE_NAME, SITE_CONFIG)
    ge_checker.set_batch_definition(BATCH_DEFINITION)
    ge_checker.set_suite(SUITE_NAME)

    ge_checker.create_expectations()
    result = ge_checker.run_checkpoint(SITE_NAME)

    if result.success:
        logger.info("✅ Great Expectations validation passed.")
    else:
        logger.warning("❌ Great Expectations validation failed.")

    ge_checker.generate_data_docs(SITE_NAME)

    return result.success


def validate_expectations(data_loader: DataLoader, expectations_passed: bool):
    """Validate expectations and move data accordingly."""
    if expectations_passed:
        logger.info("✅ Expectations passed. Moving data to production table...")
        data_loader.write_to_sql(
            table_name="taxi_data",
            schema="production",
            if_exists="append",
            index=False,
        )
    else:
        logger.error("❌ Expectations failed! Opening validation report.")
        GreatExpectationsPostgresChecker(CONTEXT_MODE).open_report()
        raise ValueError("Data validation failed! Please review your expectations.")


def main():
    """Main execution pipeline."""
    try:
        df = load_taxi_data(URL)
        data_loader = load_data_to_sql(df)
        expectations_passed = run_expectations()
        validate_expectations(data_loader, expectations_passed)

    except Exception as e:
        logger.exception("🚨 Error in pipeline execution: %s", e)
        raise


if __name__ == "__main__":
    main()
