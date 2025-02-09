import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe

from typing import Dict


class GreatExpectationsChecker:
    """Base class for handling Great Expectations validation."""

    def __init__(self, context_mode: str):
        """Initialize Great Expectations context."""
        self.context = gx.get_context(mode=context_mode)
        self.suite = None
        self.batch_definition = None

    def set_data_docs_site(self, site_name: str, site_config: Dict[str, str]) -> None:
        """Set or update a data docs site."""
        try:
            self.context.add_data_docs_site(
                site_name=site_name, site_config=site_config
            )
        except Exception:
            self.context.update_data_docs_site(
                site_name=site_name, site_config=site_config
            )

    def set_suite(self, suite_name: str) -> None:
        """Retrieve or create an expectation suite."""
        try:
            self.suite = self.context.suites.get(
                suite_name
            ) or self.context.suites.add_or_update(
                gx.core.expectation_suite.ExpectationSuite(name=suite_name)
            )
        except Exception:
            self.suite = self.context.suites.add(
                gx.core.expectation_suite.ExpectationSuite(name=suite_name)
            )

    def create_validation_definition(self):
        """Create a validation definition for the batch and suite."""
        return self.context.validation_definitions.add_or_update(
            gx.core.validation_definition.ValidationDefinition(
                name="validation definition",
                data=self.batch_definition,
                suite=self.suite,
            )
        )

    def create_checkpoint(self, validation_definition, site_name: str):
        """Create a checkpoint for validation."""
        actions = [
            gx.checkpoint.actions.UpdateDataDocsAction(
                name="update_my_site", site_names=[site_name]
            )
        ]
        return self.context.checkpoints.add_or_update(
            gx.checkpoint.checkpoint.Checkpoint(
                name="checkpoint",
                validation_definitions=[validation_definition],
                actions=actions,
                result_format="COMPLETE",
            )
        )

    def run_checkpoint(self, site_name: str):
        """Run validation using a checkpoint."""
        validation_definition = self.create_validation_definition()
        checkpoint = self.create_checkpoint(validation_definition, site_name)
        return checkpoint.run()

    def generate_data_docs(self, site_name: str):
        """Generate and build data docs."""
        self.context.build_data_docs(site_names=site_name)

    def open_report(self):
        """Open the generated report in a browser."""
        self.context.open_data_docs()


class GreatExpectationsPandasChecker(GreatExpectationsChecker):
    """Handles Great Expectations validation for Pandas DataFrames."""

    def __init__(self, df: pd.DataFrame, context_mode: str):
        super().__init__(context_mode)
        self.df = df

    def set_data_source(self, data_source: str) -> None:
        """Set up Pandas data source."""
        self.data_source = self.context.data_sources.add_or_update_pandas(data_source)

    def set_data_asset(self, data_asset_name: str) -> None:
        """Set up a Pandas data asset."""
        self.data_source = self.data_source.add_dataframe_asset(name=data_asset_name)

    def set_batch_definition(self, batch_definition) -> None:
        """Define the batch for Pandas DataFrame."""
        self.batch_definition = self.data_source.add_batch_definition_whole_dataframe(
            batch_definition
        )

    def create_expectations(self):
        """Define and update expectations for the dataset."""
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
        """Persist the updated suite."""
        self.context.suites.add_or_update(self.suite)
        self.context.suites.save(self.suite)
        self.context.build_data_docs()


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
