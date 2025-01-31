import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe

from typing import Dict


class GreatExpectationsChecker:
    """
    A class to manage and create expectations for a Great Expectations context.

    This class is responsible for interacting with the Great Expectations context, loading data,
    setting data sources, assets, suites, and expectations. It helps define data validation
    pipelines, create checkpoints, and generate data documentation in the context of data
    quality assurance.

    Attributes:
        df (pd.DataFrame): The DataFrame containing the data to be validated.
        context (gx.core.data_context.DataContext): The Great Expectations context used to manage
        expectations, validations, and checkpoints.
        data_source (gx.core.data_asset.DataAsset): The data source for the DataFrame.
        data_asset (gx.core.data_asset.DataAsset): The specific data asset associated with the DataFrame.
        batch_definition (gx.core.batch.BatchDefinition): The batch definition used for validation.
        batch (gx.core.batch.Batch): The batch associated with the validation.
        suite (gx.core.expectation_suite.ExpectationSuite): The expectation suite containing
        the defined expectations.

    Methods:
        __init__(df: pd.DataFrame, context_mode: str) -> None:
            Initializes the GreatExpectationsChecker with a DataFrame and the context mode.

        set_data_source(data_source: str) -> None:
            Sets the data source for the DataFrame within the Great Expectations context.

        set_data_asset(data_asset_name: str) -> None:
            Creates or updates the data asset for the specified name in the context.

        set_data_docs_site(site_name: str, site_config: Dict[str, str]) -> None:
            Sets up a data docs site with the given configuration, adding or updating the site
            in the context.

        set_batch_definition(batch_definition) -> None:
            Creates or updates a batch definition for the DataFrame in the context.

        set_batch(batch_parameters: Dict[str, pd.DataFrame]) -> None:
            Sets the batch associated with the defined batch definition and parameters.

        set_suite(suite_name: str) -> None:
            Creates or retrieves an expectation suite in the Great Expectations context by name.
            If the suite does not exist, it is created.

        create_expectations() -> None:
            Defines a series of expectations for the DataFrame's columns, such as column types,
            non-null values, and column values to be within certain sets or ranges.

        create_validation_definition() -> None:
            Creates a validation definition based on the current batch and expectation suite
            to be used for validation.

        create_checkpoint(validation_definition, site_name: str) -> None:
            Creates a checkpoint in the Great Expectations context, which links the validation
            definition to actions that update the data docs site.

        run_checkpoint(site_name: str) -> None:
            Runs the defined checkpoint, which validates the batch and updates the data docs site.

        generate_data_docs(site_name: str) -> None:
            Generates the data documentation for the provided site name.

        open_report() -> None:
            Opens the generated data documentation in a web browser.
    """

    def __init__(self, df: pd.DataFrame, context_mode: str):
        """
        Initializes the GreatExpectationsChecker with the given DataFrame and context mode.

        Args:
            df (pd.DataFrame): The DataFrame containing the data to validate.
            context_mode (str): The mode for creating the Great Expectations context (e.g., "mock_mode").
        """
        self.df: pd.DataFrame = df
        self.context = gx.get_context(mode=context_mode)

    def set_data_source(self, data_source: str) -> None:
        """
        Sets the data source in the Great Expectations context.

        Args:
            data_source (str): The name of the data source to add or update.
        """
        self.data_source = self.context.data_sources.add_or_update_pandas(data_source)

    def set_data_asset(self, data_asset_name: str) -> None:
        """
        Sets the data asset (i.e., a DataFrame asset) for the data source.

        Args:
            data_asset_name (str): The name of the data asset to create.
        """
        self.data_asset = self.data_source.add_dataframe_asset(name=data_asset_name)

    def set_data_docs_site(self, site_name: str, site_config: Dict[str, str]) -> None:
        """
        Sets or updates a data docs site in the Great Expectations context.

        If the site already exists, it updates the site configuration. If not, it creates a new site.

        Args:
            site_name (str): The name of the data docs site.
            site_config (Dict[str, str]): The configuration settings for the data docs site.

        Raises:
            Exception: If there is any issue with adding or updating the data docs site.
        """
        try:
            self.context.add_data_docs_site(
                site_name=site_name, site_config=site_config
            )
        except Exception as e:
            self.context.update_data_docs_site(
                site_name=site_name, site_config=site_config
            )

    def set_batch_definition(self, batch_definition) -> None:
        """
        Sets the batch definition for the data asset.

        Args:
            batch_definition: The batch definition to associate with the data asset.
        """
        self.batch_definition = self.data_asset.add_batch_definition_whole_dataframe(
            batch_definition
        )

    def set_batch(self, batch_parameters: Dict[str, pd.DataFrame]) -> None:
        """
        Sets the batch using the provided batch parameters.

        Args:
            batch_parameters (Dict[str, pd.DataFrame]): The parameters defining the batch to be fetched.
        """
        self.batch = self.batch_definition.get_batch(batch_parameters=batch_parameters)

    def set_suite(self, suite_name: str) -> None:
        """
        Sets the expectation suite, creating a new suite if it doesn't already exist.

        Args:
            suite_name (str): The name of the expectation suite.

        Raises:
            Exception: If there is an issue with retrieving or creating the suite.
        """
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
        """
        Creates a series of expectations for the columns and row counts in the data asset.

        This includes expectations for column types, null values, row counts, and specific column values.
        """
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
        """
        Creates a validation definition for the current batch and suite.

        Returns:
            validation_definition: The validation definition for the current validation process.
        """ 
        return self.context.validation_definitions.add_or_update(
            gx.core.validation_definition.ValidationDefinition(
                name="validation definition",
                data=self.batch_definition,
                suite=self.suite,
            )
        )

    def create_checkpoint(self, validation_definition, site_name: str):
        """
        Creates a checkpoint for validation.

        Args:
            validation_definition: The validation definition to use.
            site_name (str): The name of the data docs site where results will be stored.

        Returns:
            checkpoint: The created checkpoint object.
        """
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
        """
        Runs a checkpoint and validates the data using the provided site name.

        Args:
            site_name (str): The name of the site to run the checkpoint for.

        Returns:
            checkpoint_result: The result of the checkpoint run.
        """
        validation_definition = self.create_validation_definition()
        checkpoint = self.create_checkpoint(validation_definition, site_name)

        return checkpoint.run({"dataframe": self.df})

    def generate_data_docs(self, site_name: str):
        """
        Generates data documentation for the provided site name.

        Args:
            site_name (str): The name of the site for which to generate data docs.
        """
        self.context.build_data_docs(site_names=site_name)

    def open_report(self):
        """
        Opens the generated data docs site in the browser for review.
        """
        self.context.open_data_docs()
