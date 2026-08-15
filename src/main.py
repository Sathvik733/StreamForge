import time

from src.services.event_generator import EventGenerator
from src.utils.config import EVENTS_PER_SECOND
from src.utils.logger import get_logger


logger = get_logger(__name__)


def main() -> None:
    logger.info("Starting StreamForge event generator")

    generator = EventGenerator()

    delay = 1 / EVENTS_PER_SECOND

    logger.info(
        "Generating %.2f events per second",
        EVENTS_PER_SECOND,
    )

    try:
        while True:
            event = generator.generate_event()

            logger.info(
                "Generated event: %s",
                event.model_dump_json(),
            )

            time.sleep(delay)

    except KeyboardInterrupt:
        logger.info("StreamForge event generator stopped")


if __name__ == "__main__":
    main()