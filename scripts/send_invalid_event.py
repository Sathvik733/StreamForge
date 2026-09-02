import json

from confluent_kafka import Producer


producer = Producer(
    {
        "bootstrap.servers": "localhost:9092",
    }
)


invalid_event = {
    "event_type": "created",
    "order_id": "ORD-BROKEN-001",
    "customer_id": "CUST-9999",
    "product_id": "PROD-999",

    # Deliberately invalid
    "amount": "THIS_IS_NOT_A_NUMBER",

    "currency": "EUR",
    "country": "FR",
}


producer.produce(
    topic="events.raw",
    key=b"ORD-BROKEN-001",
    value=json.dumps(invalid_event).encode("utf-8"),
)

producer.flush()

print("💥 Invalid test event published to events.raw")