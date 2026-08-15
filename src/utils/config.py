import os

from dotenv import load_dotenv


load_dotenv()


EVENTS_PER_SECOND = float(
    os.getenv("EVENTS_PER_SECOND", "1")
)