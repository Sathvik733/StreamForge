from src.models.event import EventType
from src.services.event_generator import COUNTRIES, EventGenerator


def test_generate_event():
    generator = EventGenerator()

    event = generator.generate_event()

    assert event.event_id.startswith("evt_")
    assert event.order_id.startswith("ORD-")
    assert event.customer_id.startswith("CUST-")
    assert event.product_id.startswith("PROD-")

    assert event.amount > 0
    assert event.country in COUNTRIES
    assert event.event_type in EventType