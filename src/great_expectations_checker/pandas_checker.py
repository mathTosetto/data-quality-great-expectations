import pandas as pd
import great_expectations.expectations as gxe

from .base_checker import GreatExpectationsChecker


class GreatExpectationsPandasChecker(GreatExpectationsChecker):
    """Handles Great Expectations validation for Pandas DataFrames."""

    def __init__(self, df: pd.DataFrame, context_mode: str):
        """
        Initializes the checker with a Pandas DataFrame and the context mode.

        Args:
            df (pd.DataFrame): The Pandas DataFrame to validate.
            context_mode (str): The mode for initializing the Great Expectations context (e.g., 'local', 'cloud').

        """
        super().__init__(context_mode)
        self.df = df

    def set_data_source(self, data_source: str) -> None:
        """
        Sets up a Pandas data source in the Great Expectations context.

        Args:
            data_source (str): The name of the data source to add or update.
        """
        self.data_source = self.context.data_sources.add_or_update_pandas(data_source)

    def set_data_asset(self, data_asset_name: str) -> None:
        """
        Sets up a Pandas data asset in the Great Expectations context.

        Args:
            data_asset_name (str): The name of the data asset to add for the DataFrame.
        """
        self.data_asset = self.data_source.add_dataframe_asset(name=data_asset_name)

    def set_batch_definition(self, batch_definition) -> None:
        """
        Defines the batch for the provided Pandas DataFrame.

        Args:
            batch_definition: The batch definition that specifies how the batch is to be loaded.
        """
        self.batch_definition = self.data_asset.add_batch_definition_whole_dataframe(
            batch_definition
        )

    def create_expectations(self):
        """Defines and updates expectations for the Pandas DataFrame."""
        self.suite.expectations.clear()

        expectations = [
            gxe.ExpectTableColumnsToMatchOrderedList(
                column_list=[
                    "vendor_id",
                    "pickup_datetime",
                    "dropoff_datetime",
                    "passenger_count",
                    "trip_distance",
                    "rate_code_id",
                    "store_and_fwd_flag",
                    "pickup_location_id",
                    "dropoff_location_id",
                    "payment_type",
                    "fare_amount",
                    "extra",
                    "mta_tax",
                    "tip_amount",
                    "tolls_amount",
                    "improvement_surcharge",
                    "total_amount",
                    "congestion_surcharge",
                ]
            ),
            gxe.ExpectColumnValuesToNotBeNull(column="vendor_id"),
            gxe.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1),
            gxe.ExpectColumnValuesToBeInSet(
                column="store_and_fwd_flag", value_set=["Y", "N"]
            ),
            gxe.ExpectTableRowCountToBeBetween(min_value=100, max_value=10000),
        ]

        column_types = {
            "vendor_id": "int",
            "passenger_count": "int",
            "payment_type": "int",
            "total_amount": "float",
            "store_and_fwd_flag": "object",
        }

        expectations.extend(
            [
                gxe.ExpectColumnValuesToBeOfType(column=col, type_=dtype)
                for col, dtype in column_types.items()
            ]
        )

        self.suite.expectations.extend(expectations)

        self._update_suite()

    def _update_suite(self):
        """Persists the updated expectation suite in the Great Expectations context and rebuilds data docs."""
        self.context.suites.add_or_update(self.suite)
        self.context.suites.save(self.suite)
        self.context.build_data_docs()
