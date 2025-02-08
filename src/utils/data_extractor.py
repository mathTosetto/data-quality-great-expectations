import logging.config
import logging.handlers

import pandas as pd

from src.utils.my_logger import LoggerSetup

logger: logging = logging.getLogger("class TaxiDataExtractor")


class TaxiDataExtractor:
    """
    A class to load and process taxi data from a given URL.

    This class is responsible for loading taxi data from a CSV file located at the provided URL.
    It processes the data by converting specific columns (e.g., "pickup_datetime" and "dropoff_datetime")
    to datetime format and provides access to the loaded data.

    Attributes:
        url (str): The URL where the taxi data CSV file is located.
        df (pd.DataFrame or None): The DataFrame that holds the processed taxi data.

    Methods:
        __init__(url: str) -> None:
            Initializes the TaxiDataExtractor with the provided URL.

        _processsed_data() -> pd.DataFrame:
            Processes the loaded data by converting specific columns to datetime format.

        load_data() -> None:
            Loads data from the URL and processes it using the `_processsed_data()` method.

        get_data() -> pd.DataFrame:
            Returns the loaded and processed data if available.
    """

    def __init__(self, url: str) -> None:
        """
        Initializes the TaxiDataExtractor with the URL to load the data from.

        Args:
            url (str): The URL where the taxi data CSV file is located.

        Raises:
            ValueError: If the provided URL is not a string.
        """
        LoggerSetup()

        if isinstance(url, str):
            self.url = url
            self.df = None
        else:
            logger.error("Input value for URL is not a string.")
            raise ValueError("URL must be a string")

    def _processsed_data(self) -> pd.DataFrame:
        """
        Processes the loaded data by converting specific columns to datetime format.

        This method looks for the columns "pickup_datetime" and "dropoff_datetime" and
        converts their values to pandas datetime objects.

        Returns:
            pd.DataFrame: The processed DataFrame with updated datetime columns.
        """
        if self.df is not None:
            transform_to_date = ["pickup_datetime", "dropoff_datetime"]
            for col in transform_to_date:
                self.df[col] = pd.to_datetime(self.df[col])
        return self.df

    def load_data(self) -> None:
        """
        Loads data from the URL and processes it using the `_processsed_data()` method.

        This method attempts to load data from the provided URL using `pd.read_csv()`.
        If successful, it then processes the data by converting datetime columns.

        If an error occurs during data loading, an error message is printed, and `df` is set to `None`.
        """
        try:
            self.df = pd.read_csv(self.url)
            self.df = self._processsed_data()
        except Exception as e:
            logger.error(f"Error loading data: {e}.")
            self.df = None

    def get_data(self) -> pd.DataFrame:
        """
        Returns the loaded and processed data.

        Returns:
            pd.DataFrame: The processed taxi data if available.

        Raises:
            ValueError: If the data has not been loaded or processed yet.
        """
        if self.df is not None:
            return self.df
        else:
            logger.error("Dataframe is empty.")
            raise ValueError("Data has not been loaded or processed yet.")
