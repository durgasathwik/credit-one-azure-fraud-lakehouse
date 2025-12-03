import argparse
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Reuse your existing generator
from transaction_generator import generate_transactions


def create_producer(bootstrap_servers: str):
    """
    Create a Kafka producer that sends JSON-encoded messages.
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # optional: small linger/batch to keep things snappy for dev
        linger_ms=50,
        api_version=(0, 10, 2),
    )
    return producer


def main():
    parser = argparse.ArgumentParser(description="Stream fake transactions into Kafka.")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:19092",
        help="Kafka bootstrap servers (default: localhost:19092).",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="transactions",
        help="Kafka topic name (default: transactions).",
    )
    parser.add_argument(
        "--n_rows",
        type=int,
        default=100,
        help="Number of transactions to send (default: 100).",
    )
    parser.add_argument(
        "--sleep_ms",
        type=int,
        default=100,
        help="Sleep time between messages in milliseconds (default: 100ms).",
    )
    args = parser.parse_args()

    producer = create_producer(args.bootstrap_servers)

    print(f"Producing {args.n_rows} messages to topic '{args.topic}' on {args.bootstrap_servers} ...")

    # Generate the fake transactions
    rows = generate_transactions(n_rows=args.n_rows)

    sent = 0
    for row in rows:
        try:
            future = producer.send(args.topic, value=row)
            # We won't wait on every message, but let's occasionally flush
            sent += 1
            if sent % 10 == 0:
                producer.flush()
            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)
        except KafkaError as e:
            print(f"Error sending message: {e}")
            break

    # Final flush to ensure everything is sent
    producer.flush()
    print(f"Done. Sent {sent} messages to topic '{args.topic}'.")


if __name__ == "__main__":
    main()
