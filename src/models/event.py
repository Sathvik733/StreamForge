from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field


class EventType(str, Enum):
    ORDER_CREATED = "order_created"
    PAYMENT_COMPLETED = "payment_completed"
    PAYMENT_FAILED = "payment_failed"
    ORDER_CANCELLED = "order_cancelled"
    SHIPMENT_CREATED = "shipment_created"
    SHIPMENT_DELIVERED = "shipment_delivered"


class OrderEvent(BaseModel):
    event_id: str = Field(
        default_factory=lambda: f"evt_{uuid4().hex[:12]}"
    )

    event_type: EventType

    order_id: str
    customer_id: str
    product_id: str

    amount: float = Field(gt=0)

    currency: str = "EUR"
    country: str

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )