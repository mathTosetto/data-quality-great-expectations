import great_expectations as gx

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
