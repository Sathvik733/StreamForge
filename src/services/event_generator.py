import random

from src.models.event import EventType, OrderEvent


COUNTRIES = [
    "FR",
    "DE",
    "ES",
    "IT",
    "GB",
    "US",
    "IN",
]

CURRENCIES = {
    "FR": "EUR",
    "DE": "EUR",
    "ES": "EUR",
    "IT": "EUR",
    "GB": "GBP",
    "US": "USD",
    "IN": "INR",
}


class EventGenerator:
    def generate_event(self) -> OrderEvent:
        country = random.choice(COUNTRIES)

        return OrderEvent(
            event_type=random.choice(list(EventType)),
            order_id=f"ORD-{random.randint(10000, 99999)}",
            customer_id=f"CUST-{random.randint(1000, 9999)}",
            product_id=f"PROD-{random.randint(100, 999)}",
            amount=round(random.uniform(10.0, 500.0), 2),
            currency=CURRENCIES[country],
            country=country,
        )