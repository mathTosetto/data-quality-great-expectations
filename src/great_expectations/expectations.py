import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe

from typing import Dict


class GreatExpectationsChecker:
    def __init__(self, df: pd.DataFrame, context_mode: str):
        self.df: pd.DataFrame = df
        self.context = gx.get_context(mode=context_mode)

    def set_data_source(self, data_source: str) -> None:
        self.data_source = self.context.data_sources.add_or_update_pandas(data_source)

    def set_data_asset(self, data_asset_name: str) -> None:
        self.data_asset = self.data_source.add_dataframe_asset(name=data_asset_name)

    def set_data_docs_site(self, site_name: str, site_config: Dict[str, str]) -> None:
        try:
            self.context.add_data_docs_site(
                site_name=site_name, site_config=site_config
            )
        except Exception as e:
            self.context.update_data_docs_site(
                site_name=site_name, site_config=site_config
            )

    def set_batch_definition(self, batch_definition) -> None:
        self.batch_definition = self.data_asset.add_batch_definition_whole_dataframe(
            batch_definition
        )

    def set_batch(self, batch_parameters: Dict[str, pd.DataFrame]) -> None:
        self.batch = self.batch_definition.get_batch(batch_parameters=batch_parameters)

    def set_suite(self, suite_name: str) -> None:
        try:
            self.suite = self.context.suites.get(suite_name)
            if not self.suite:
                self.suite = self.context.suites.add_or_update(
                    gx.core.expectation_suite.ExpectationSuite(name=suite_name)
                )
        except Exception as e:
            self.suite = self.context.suites.add(
                gx.core.expectation_suite.ExpectationSuite(
                    name=suite_name,
                )
            )

    def create_expectations(self):
        self.suite.add_expectation(
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
                ],
            )
        )

        check_columns_type: Dict[str, str] = {
            "vendor_id": "int",
            "passenger_count": "int",
            "payment_type": "int",
            "total_amount": "float",
            "store_and_fwd_flag": "object",
        }

        for key, value in check_columns_type.items():
            self.suite.add_expectation(
                gxe.ExpectColumnValuesToBeOfType(column=key, type_=value)
            )

        self.suite.add_expectation(
            gxe.ExpectColumnValuesToNotBeNull(column="vendor_id")
        )

        self.suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(
                column="passenger_count",
                min_value=1,
            )
        )

        self.suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(
                column="store_and_fwd_flag", value_set=["Y", "N"]
            )
        )

        self.suite.add_expectation(
            gxe.ExpectTableRowCountToBeBetween(min_value=100, max_value=10000)
        )

    def create_validation_definition(self):
        validation_definition = self.context.validation_definitions.add_or_update(
            gx.core.validation_definition.ValidationDefinition(
                name="validation definition",
                data=self.batch_definition,
                suite=self.suite,
            )
        )
        return validation_definition

    def create_checkpoint(self, validation_definition, site_name: str):
        actions = [
            gx.checkpoint.actions.UpdateDataDocsAction(
                name="update_my_site", site_names=[site_name]
            ),
        ]

        checkpoint = self.context.checkpoints.add_or_update(
            gx.checkpoint.checkpoint.Checkpoint(
                name="checkpoint",
                validation_definitions=[validation_definition],
                actions=actions,
                result_format="COMPLETE",
            )
        )
        return checkpoint

    def run_checkpoint(self, site_name: str):
        validation_definition = self.create_validation_definition()
        checkpoint = self.create_checkpoint(validation_definition, site_name)

        checkpoint_result = checkpoint.run({"dataframe": self.df})
        return checkpoint_result

    def generate_data_docs(self, site_name: str):
        self.context.build_data_docs(site_names=site_name)

    def open_report(self):
        self.context.open_data_docs()
