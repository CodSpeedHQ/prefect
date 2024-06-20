# isort: skip_file

# Setup version and path constants

from . import _version
import pathlib

__version_info__ = _version.get_versions()
__version__ = __version_info__["version"]

# The absolute path to this module
__module_path__ = pathlib.Path(__file__).parent
# The absolute path to the root of the repository, only valid for use during development
__development_base_path__ = __module_path__.parents[1]

# The absolute path to the built UI within the Python module, used by
# `prefect server start` to serve a dynamic build of the UI
__ui_static_subpath__ = __module_path__ / "server" / "ui_build"

# The absolute path to the built UI within the Python module
__ui_static_path__ = __module_path__ / "server" / "ui"

del _version, pathlib


# Import user-facing API
from prefect.deployments import deploy
from prefect.logging import get_run_logger
from prefect.flows import flow, serve
from prefect.transactions import Transaction
from prefect.tasks import task
from prefect.context import tags
from prefect.client.orchestration import get_client
import prefect.variables
import prefect.runtime

# Import modules that register types
import prefect.serializers
import prefect.blocks.notifications
import prefect.blocks.system

# Initialize the process-wide profile and registry at import time
import prefect.context

# Perform any forward-ref updates needed for Pydantic models
import prefect.client.schemas

prefect.context.FlowRunContext.model_rebuild()
prefect.context.TaskRunContext.model_rebuild()
prefect.client.schemas.State.model_rebuild()
prefect.client.schemas.StateCreate.model_rebuild()
Transaction.model_rebuild()


prefect.plugins.load_extra_entrypoints()

# Configure logging
import prefect.logging.configuration

prefect.logging.configuration.setup_logging()
prefect.logging.get_logger("profiles").debug(
    f"Using profile {prefect.context.get_settings_context().profile.name!r}"
)

# Declare API for type-checkers
__all__ = ["flow", "get_client", "get_run_logger", "tags", "task", "serve", "deploy"]
