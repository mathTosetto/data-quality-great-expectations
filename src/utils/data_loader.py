import os
import logging.config
import logging.handlers

import pandas as pd
import sqlalchemy as sa

from dotenv import load_dotenv
from src.utils.my_logger import LoggerSetup

load_dotenv()

logger: logging.Logger = logging.getLogger("class DataLoader")


class DataLoader:
    """A class to handle loading pandas DataFrames into a SQL database."""

    def __init__(self, df: pd.DataFrame):
        """
        Initializes the DataLoader with a pandas DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame containing the data to be loaded.

        Raises:
            ValueError: If the input is not a pandas DataFrame.
        """
        LoggerSetup()

        if isinstance(df, pd.DataFrame):
            self.df = df
            self.engine = sa.create_engine(os.getenv("CONNECTION_STRING"))
        else:
            logger.error("The input value is not a Dataframe.")
            raise ValueError("The input value is not a Dataframe.")

    def write_to_sql(self, table_name: str, **kwargs) -> None:
        """
        Writes the DataFrame to a SQL table.

        Args:
            table_name (str): Name of the target table in the database.
            **kwargs: Additional arguments for `pandas.DataFrame.to_sql`.
        """
        self.df.to_sql(name=table_name, con=self.engine, **kwargs)

        total_rows: int = len(self.df)
        logger.info(f"{total_rows} rows written to {table_name}.")
        print(f"{total_rows} rows written to {table_name}.")
