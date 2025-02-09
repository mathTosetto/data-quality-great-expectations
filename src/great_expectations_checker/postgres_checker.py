import great_expectations.expectations as gxe

from .base_checker import GreatExpectationsChecker


class GreatExpectationsPostgresChecker(GreatExpectationsChecker):
    """Handles Great Expectations validation for PostgreSQL databases."""

    def __init__(self, context_mode: str):
        super().__init__(context_mode)

    def set_data_source(self, data_source: str, connection_string: str) -> None:
        """Set up PostgreSQL data source."""
        self.data_source = self.context.data_sources.add_or_update_postgres(
            name=data_source, connection_string=connection_string
        )

    def set_data_asset(
        self, data_asset_name: str, table_name: str, schema_name: str
    ) -> None:
        """Set up PostgreSQL data asset (table)."""
        self.data_asset = self.data_source.add_table_asset(
            name=data_asset_name, table_name=table_name, schema_name=schema_name
        )

    def set_batch_definition(self, batch_definition) -> None:
        """Define batch for PostgreSQL table."""
        self.batch_definition = self.data_asset.add_batch_definition_whole_table(
            batch_definition
        )

    def create_expectations(self):
        """Define and update expectations for the PostgreSQL dataset."""
        self.suite.expectations.clear()

        expectations = [
            gxe.ExpectColumnValuesToBeOfType(column="vendor_id", type_="Integer"),
            # gxe.ExpectColumnValuesToBeUnique(column="vendor_id"),
        ]

        self.suite.expectations.extend(expectations)

        self._update_suite()

    def _update_suite(self):
        """Persist the updated suite."""
        self.context.suites.add_or_update(self.suite)
        self.suite.save()
        self.context.build_data_docs()
