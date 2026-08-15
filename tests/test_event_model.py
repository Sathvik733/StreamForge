import pytest
from pydantic import ValidationError

from src.models.event import EventType, OrderEvent


def test_valid_order_event():
    event = OrderEvent(
        event_type=EventType.ORDER_CREATED,
        order_id="ORD-1001",
        customer_id="CUST-1001",
        product_id="PROD-101",
        amount=100.0,
        country="FR",
    )

    assert event.amount == 100.0
    assert event.country == "FR"


def test_negative_amount_is_rejected():
    with pytest.raises(ValidationError):
        OrderEvent(
            event_type=EventType.ORDER_CREATED,
            order_id="ORD-1001",
            customer_id="CUST-1001",
            product_id="PROD-101",
            amount=-100.0,
            country="FR",
        )