from typing import Optional
from uuid import UUID

from pydantic import Field
from typing_extensions import Self

from prefect.client.utilities import get_or_create_client
from prefect.events.schemas.automations import (
    AutomationCore,
    CompositeTrigger,
    CompoundTrigger,
    EventTrigger,
    MetricTrigger,
    MetricTriggerOperator,
    MetricTriggerQuery,
    Posture,
    PrefectMetric,
    ResourceSpecification,
    ResourceTrigger,
    SequenceTrigger,
    Trigger,
)
from prefect.exceptions import PrefectHTTPStatusError
from prefect.utilities.asyncutils import sync_compatible

__all__ = [
    "AutomationCore",
    "EventTrigger",
    "ResourceTrigger",
    "Posture",
    "Trigger",
    "ResourceSpecification",
    "MetricTriggerOperator",
    "MetricTrigger",
    "PrefectMetric",
    "CompositeTrigger",
    "SequenceTrigger",
    "CompoundTrigger",
    "MetricTriggerQuery",
]


class Automation(AutomationCore):
    id: Optional[UUID] = Field(default=None, description="The ID of this automation")

    @classmethod
    @sync_compatible
    async def create(cls: Self, automation: Self) -> Self:
        """
        Create a new automation.

        auto_to_create = Automation(
            name="woodchonk",
            trigger=EventTrigger(
                expect={"animal.walked"},
                match={
                    "genus": "Marmota",
                    "species": "monax",
                },
                posture="Reactive",
                threshold=3,
                within=timedelta(seconds=10),
            ),
            actions=[CancelFlowRun()]
        )
        created_automation = Automation.create(auto_to_create)
        """
        client, _ = get_or_create_client()
        automation = AutomationCore(**automation.dict(exclude={"id"}))
        automation_id = await client.create_automation(automation=automation)
        automation = await client.read_automation(automation_id=automation_id)
        automation = Automation(**automation.dict())
        return automation if automation else None

    @sync_compatible
    async def update(self: Self):
        """
        Updates an existing automation.
        auto = Automation.read(id = 123)
        auto.name = "new name"
        auto.update()
        """

        client, _ = get_or_create_client()
        automation = AutomationCore(**self.dict(exclude={"id", "owner_resource"}))
        await client.update_automation(automation_id=self.id, automation=automation)

    @classmethod
    @sync_compatible
    async def read(
        cls: Self, id: Optional[UUID] = None, name: Optional[str] = None
    ) -> Self:
        """
        Read an automation by ID or name.
        automation = Automation.read(name="woodchonk")

        or

        automation = Automation.read(id=UUID("b3514963-02b1-47a5-93d1-6eeb131041cb"))
        """
        if id and name:
            raise ValueError("Only one of id or name can be provided")
        if not id and not name:
            raise ValueError("One of id or name must be provided")
        client, _ = get_or_create_client()
        if id:
            automation = await client.read_automation(automation_id=id)
            return Automation(**automation.dict()) if automation else None
        else:
            automation = await client.read_automations_by_name(name=name)
            return Automation(**automation[0].dict()) if automation else None

    @sync_compatible
    async def delete(self: Self) -> bool:
        """
        auto = Automation.read(id = 123)
        auto.delete()
        """
        try:
            client, _ = get_or_create_client()
            await client.delete_automation(self.id)
            return True
        except PrefectHTTPStatusError as exc:
            if exc.response.status_code == 404:
                return False
            raise

    @sync_compatible
    async def disable(self: Self) -> bool:
        """
        Disable an automation.
        auto = Automation.read(id = 123)
        auto.disable()
        """
        try:
            client, _ = get_or_create_client()
            await client.pause_automation(self.id)
            return True
        except PrefectHTTPStatusError as exc:
            if exc.response.status_code == 404:
                return False
            raise

    @sync_compatible
    async def enable(self: Self) -> bool:
        """
        Enable an automation.
        auto = Automation.read(id = 123)
        auto.enable()
        """
        try:
            client, _ = get_or_create_client()
            await client.resume_automation("asd")
            return True
        except PrefectHTTPStatusError as exc:
            if exc.response.status_code == 404:
                return False
            raise
