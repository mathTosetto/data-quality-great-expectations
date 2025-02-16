import logging
import great_expectations.expectations as gxe

from .base_checker import GreatExpectationsChecker

logger: logging.Logger = logging.getLogger("class GreatExpectationsPostgresChecker")


class GreatExpectationsPostgresChecker(GreatExpectationsChecker):
    """Handles Great Expectations validation for PostgreSQL databases."""

    def __init__(self, context_mode: str):
        """
        Initializes the PostgreSQL checker with the specified context mode.

        Args:
            context_mode (str): The mode for initializing the Great Expectations context (e.g., 'local', 'cloud').
        """
        super().__init__(context_mode)

    def set_data_source(self, data_source: str, connection_string: str) -> None:
        """
        Sets up the PostgreSQL data source in the Great Expectations context.

        Args:
            data_source (str): The name of the data source to add or update.
            connection_string (str): The connection string for connecting to the PostgreSQL database.
        """
        self.data_source = self.context.data_sources.add_or_update_postgres(
            name=data_source, connection_string=connection_string
        )

    def set_data_asset(
        self, data_asset_name: str, table_name: str, schema_name: str
    ) -> None:
        """
        Sets up a PostgreSQL data asset (table) in the Great Expectations context.

        Args:
            data_asset_name (str): The name of the data asset to add for the PostgreSQL table.
            table_name (str): The name of the table in the PostgreSQL database.
            schema_name (str): The schema name in which the table resides.
        """
        self.data_asset = self.data_source.add_table_asset(
            name=data_asset_name, table_name=table_name, schema_name=schema_name
        )

    def set_batch_definition(self, batch_definition) -> None:
        """
        Defines the batch for the specified PostgreSQL table.

        Args:
            batch_definition: The batch definition that specifies how the batch is to be loaded.
        """
        self.batch_definition = self.data_asset.add_batch_definition_whole_table(
            batch_definition
        )

    def create_expectations(self):
        """Defines and updates expectations for the PostgreSQL dataset."""
        self.suite.expectations.clear()

        expectations = [
            gxe.ExpectColumnValuesToBeOfType(column="vendor_id", type_="Integer"),
            # gxe.ExpectColumnValuesToBeUnique(column="vendor_id"),
        ]

        self.suite.expectations.extend(expectations)

        self._update_suite()

    def _update_suite(self):
        """Persists the updated expectation suite in the Great Expectations context and rebuilds data docs."""
        self.context.suites.add_or_update(self.suite)
        self.suite.save()
        self.context.build_data_docs()
