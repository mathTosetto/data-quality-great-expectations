import logging
import pandas as pd

from src.utils.my_logger import LoggerSetup

logger: logging.Logger = logging.getLogger("class TaxiDataExtractor")


class TaxiDataExtractor:
    """Extracts and processes NYC Taxi data from a given URL."""

    def __init__(self, url: str) -> None:
        """
        Initialize the extractor with a data URL.

        Args:
            url (str): The URL pointing to the CSV data file.

        Raises:
            ValueError: If the provided URL is not a string.
        """
        LoggerSetup()

        if isinstance(url, str):
            self.url = url
            self.df: pd.DataFrame | None = None
        else:
            logger.error("Input value for URL is not a string.")
            raise ValueError("URL must be a string")

    def _processsed_data(self) -> pd.DataFrame:
        """
        Convert datetime columns to pandas datetime format.

        Returns:
            pd.DataFrame: The DataFrame with transformed datetime columns.
        """
        if self.df is not None:
            transform_to_date = ["pickup_datetime", "dropoff_datetime"]
            for col in transform_to_date:
                self.df[col] = pd.to_datetime(self.df[col])
        return self.df

    def load_data(self) -> None:
        """
        Load data from the provided URL into a pandas DataFrame.

        Raises:
            Exception: If an error occurs while loading the data.
        """
        try:
            self.df = pd.read_csv(self.url)
            self.df = self._processsed_data()
        except Exception as e:
            logger.error(f"Error loading data: {e}.")
            self.df = None

    def get_data(self) -> pd.DataFrame:
        """
        Retrieve the processed DataFrame.

        Returns:
            pd.DataFrame: The loaded and processed DataFrame.

        Raises:
            ValueError: If data has not been loaded or processed.
        """
        if self.df is not None:
            return self.df
        else:
            logger.error("Dataframe is empty.")
            raise ValueError("Data has not been loaded or processed yet.")
