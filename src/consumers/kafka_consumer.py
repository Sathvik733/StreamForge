import json
import sys

from confluent_kafka import Consumer, KafkaError


class KafkaEventConsumer:
    def __init__(
        self,
        consumer_name: str,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "streamforge-consumers",
    ):
        self.consumer_name = consumer_name

        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

    def subscribe(self, topics: list[str]):
        self.consumer.subscribe(topics)

    def consume(self):
        print(f"🚀 {self.consumer_name} started...")
        print("Waiting for events...\n")

        try:
            while True:
                message = self.consumer.poll(1.0)

                if message is None:
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue

                    print(f"❌ Kafka error: {message.error()}")
                    continue

                event = json.loads(
                    message.value().decode("utf-8")
                )

                print(
                    f"\n👤 Consumer: {self.consumer_name}\n"
                    f"📥 Topic: {message.topic()}\n"
                    f"📦 Partition: {message.partition()}\n"
                    f"🔢 Offset: {message.offset()}\n"
                    f"📄 Event: {event}\n"
                    f"{'-' * 70}"
                )

        except KeyboardInterrupt:
            print(f"\n🛑 {self.consumer_name} stopped.")

        finally:
            self.consumer.close()


if __name__ == "__main__":
    consumer_name = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "consumer-1"
    )

    consumer = KafkaEventConsumer(
        consumer_name=consumer_name
    )

    consumer.subscribe(
        ["events.raw"]
    )

    consumer.consume()