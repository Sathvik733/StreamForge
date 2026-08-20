import json

from confluent_kafka import Producer

from src.services.event_generator import EventGenerator


class KafkaEventProducer:
    """
    Publishes StreamForge events to Apache Kafka.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
    ):
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
            }
        )

    @staticmethod
    def delivery_report(err, msg):
        """
        Called when Kafka confirms whether an event
        was successfully delivered.
        """
        if err is not None:
            print(f"❌ Event delivery failed: {err}")
        else:
            print(
                f"✅ Event delivered | "
                f"topic={msg.topic()} "
                f"partition={msg.partition()} "
                f"offset={msg.offset()}"
            )

    def publish(self, topic: str, event):
        """
        Convert the event into JSON and publish it to Kafka.
        """

        # If event is a Pydantic model, convert it to a dict first
        if hasattr(event, "model_dump"):
            event = event.model_dump(mode="json")

        event_json = json.dumps(event)

        self.producer.produce(
            topic=topic,
            value=event_json.encode("utf-8"),
            callback=self.delivery_report,
        )

        # Trigger delivery callbacks
        self.producer.poll(0)

    def flush(self):
        """
        Wait for all pending events to be delivered.
        """
        self.producer.flush()


if __name__ == "__main__":
    producer = KafkaEventProducer()
    generator = EventGenerator()

    for _ in range(10):
        event = generator.generate_event()

        print(f"📦 Generated event: {event}")

        producer.publish(
            topic="events.raw",
            event=event,
        )

    producer.flush()

    print("✅ Finished publishing events.")