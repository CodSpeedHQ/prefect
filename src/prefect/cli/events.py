import asyncio
from enum import Enum

import orjson
import typer
import websockets
from anyio import open_file

from prefect.cli._types import PrefectTyper
from prefect.cli._utilities import exit_with_error
from prefect.cli.root import app
from prefect.events.clients import PrefectCloudEventSubscriber
from prefect.events.filters import EventFilter

events_app = PrefectTyper(name="events", help="Commands for working with events.")
app.add_typer(events_app, aliases=["event"])


class StreamFormat(str, Enum):
    json = "json"
    text = "text"


@events_app.command()
async def stream(
    format: StreamFormat = typer.Option(
        StreamFormat.json, "--format", help="Output format (json or text)"
    ),
    output_file: str = typer.Option(
        None, "--output-file", help="File to write events to"
    ),
    event_filter: str = typer.Option(None, "--event-filter", help="Event filter"),
):
    """Subscribes to the event stream of a workspace, printing each event
    as it is received. By default, events are printed as JSON, but can be
    printed as text by passing `--format text`.
    """
    app.console.print("Subscribing to event stream...")

    try:
        if event_filter:
            try:
                filter_dict = orjson.loads(event_filter)
                constructed_event_filter = EventFilter(
                    **filter_dict
                )  # Construct the filter object
            except orjson.JSONDecodeError:
                exit_with_error("Invalid JSON format for filter specification")
            except AttributeError:
                exit_with_error("Invalid filter specification")

        async with PrefectCloudEventSubscriber(
            filter=constructed_event_filter
        ) as subscriber:
            async for event in subscriber:
                await handle_event(event, format, output_file)
    except Exception as exc:
        handle_error(exc)


async def handle_event(event, format, output_file):
    if format == StreamFormat.json:
        event_data = orjson.dumps(event.dict(), default=str).decode()
    elif format == StreamFormat.text:
        event_data = f"{event.occurred.isoformat()} {event.event} {event.resource.id}"
    if output_file:
        async with open_file(output_file, "a") as f:
            await f.write(event_data + "\n")
    else:
        print(event_data)


def handle_error(exc):
    if isinstance(exc, websockets.exceptions.ConnectionClosedError):
        exit_with_error(f"Connection closed, retrying... ({exc})")
    elif isinstance(exc, (KeyboardInterrupt, asyncio.exceptions.CancelledError)):
        exit_with_error("Exiting...")
    elif isinstance(exc, (PermissionError)):
        exit_with_error(f"Error writing to file: {exc}")
    else:
        exit_with_error(f"An unexpected error occurred: {exc}")
