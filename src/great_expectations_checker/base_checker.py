import logging
import great_expectations as gx

from typing import Dict

logger: logging.Logger = logging.getLogger("class GreatExpectationsChecker")


class GreatExpectationsChecker:
    """Base class for handling Great Expectations validation."""

    def __init__(self, context_mode: str):
        """
        Initializes the Great Expectations context for validation.

        Args:
            context_mode (str): The mode for initializing the Great Expectations context.
            Possible values might include 'local', 'cloud', etc.
        """
        self.context = gx.get_context(mode=context_mode)
        self.suite = None
        self.batch_definition = None

    def set_data_docs_site(self, site_name: str, site_config: Dict[str, str]) -> None:
        """
        Sets or updates a data docs site configuration in the context.

        Args:
            site_name (str): The name of the data docs site to be set or updated.
            site_config (Dict[str, str]): A dictionary containing the configuration for the data docs site.
        """
        try:
            self.context.add_data_docs_site(
                site_name=site_name, site_config=site_config
            )
        except Exception:
            self.context.update_data_docs_site(
                site_name=site_name, site_config=site_config
            )

    def set_suite(self, suite_name: str) -> None:
        """
        Retrieves or creates an expectation suite by the provided name.

        Args:
            suite_name (str): The name of the expectation suite to be retrieved or created.
        """
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
        """
        Creates a validation definition for the current batch and suite.

        Returns:
            gx.core.validation_definition.ValidationDefinition: The created or updated validation definition object.
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
        Creates a checkpoint that will validate data using the provided validation definition.

        Args:
            validation_definition (gx.core.validation_definition.ValidationDefinition): The validation definition to use.
            site_name (str): The name of the data docs site associated with the checkpoint.

        Returns:
            gx.checkpoint.checkpoint.Checkpoint: The created or updated checkpoint object.
        """
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
        """
        Runs a checkpoint for validation using the provided site name.

        Args:
            site_name (str): The name of the data docs site to associate with the checkpoint.

        Returns:
            gx.checkpoint.checkpoint.CheckpointResult: The result of the checkpoint run.
        """
        validation_definition = self.create_validation_definition()
        checkpoint = self.create_checkpoint(validation_definition, site_name)
        return checkpoint.run()

    def generate_data_docs(self, site_name: str):
        """
        Generates and builds the data documentation for the provided site name.

        Args:
            site_name (str): The name of the data docs site to generate.
        """
        self.context.build_data_docs(site_names=site_name)

    def open_report(self):
        """Opens the generated data docs report in a web browser."""
        self.context.open_data_docs()
