import json

from confluent_kafka import Consumer, Producer
from pydantic import ValidationError

from src.models.event import OrderEvent


class ValidationConsumer:
    """
    Consumes raw events, validates them against the OrderEvent
    schema, and routes them to either events.valid or events.dlq.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "streamforge-validation",
    ):
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
            }
        )

    def subscribe(self):
        self.consumer.subscribe(["events.raw"])

    def publish(self, topic: str, event: dict, key: str | None = None):
        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key else None,
            value=json.dumps(event).encode("utf-8"),
        )

        self.producer.poll(0)

    def process_event(self, message):
        try:
            raw_event = json.loads(
                message.value().decode("utf-8")
            )

            validated_event = OrderEvent.model_validate(raw_event)

            event_data = validated_event.model_dump(mode="json")

            order_id = event_data["order_id"]

            self.publish(
                topic="events.valid",
                event=event_data,
                key=order_id,
            )

            print(
                f"✅ VALID EVENT\n"
                f"Order ID: {order_id}\n"
                f"Source partition: {message.partition()}\n"
                f"Source offset: {message.offset()}\n"
                f"Destination: events.valid\n"
                f"{'-' * 60}"
            )

        except (ValidationError, json.JSONDecodeError) as error:

            dlq_event = {
                "error": str(error),
                "source_topic": message.topic(),
                "source_partition": message.partition(),
                "source_offset": message.offset(),
                "original_message": message.value().decode(
                    "utf-8",
                    errors="replace",
                ),
            }

            self.publish(
                topic="events.dlq",
                event=dlq_event,
            )

            print(
                f"❌ INVALID EVENT\n"
                f"Reason: {error}\n"
                f"Destination: events.dlq\n"
                f"{'-' * 60}"
            )

    def run(self):
        self.subscribe()

        print("🚀 StreamForge validation consumer started...")
        print("Listening to events.raw...\n")

        try:
            while True:
                message = self.consumer.poll(1.0)

                if message is None:
                    continue

                if message.error():
                    print(
                        f"❌ Kafka consumer error: "
                        f"{message.error()}"
                    )
                    continue

                self.process_event(message)

        except KeyboardInterrupt:
            print("\n🛑 Validation consumer stopped.")

        finally:
            self.producer.flush()
            self.consumer.close()


if __name__ == "__main__":
    validator = ValidationConsumer()
    validator.run()