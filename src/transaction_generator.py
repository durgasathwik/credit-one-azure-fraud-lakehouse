import argparse
import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
from dateutil import tz
from faker import Faker

fake = Faker()


def generate_transactions(
    n_rows: int,
    start_minutes_ago: int = 60,
    end_minutes_ago: int = 0,
    fraud_rate: float = 0.03,
):
    """
    Generate a list of fake transaction dictionaries.

    Args:
        n_rows: number of rows to generate
        start_minutes_ago: earliest timestamp (relative to now, in minutes)
        end_minutes_ago: latest timestamp (relative to now, in minutes)
        fraud_rate: fraction of transactions marked as high_risk (0–1)
    """
    now_utc = datetime.now(tz.UTC)
    start_time = now_utc - timedelta(minutes=start_minutes_ago)
    end_time = now_utc - timedelta(minutes=end_minutes_ago)

    merchants = [
        "Amazon",
        "Walmart",
        "BestBuy",
        "Target",
        "Costco",
        "Starbucks",
        "Uber",
        "Netflix",
        "DoorDash",
        "Apple Store",
    ]
    channels = ["POS", "ONLINE", "MOBILE"]
    countries = ["US", "CA", "IN", "GB", "DE", "FR"]

    rows = []

    for _ in range(n_rows):
        tx_time = fake.date_time_between(
            start_date=start_time,
            end_date=end_time,
            tzinfo=tz.UTC,
        )

        amount = round(random.uniform(1.0, 5000.0), 2)

        # Basic "risk" heuristic: big amount + foreign country + online
        country = random.choice(countries)
        channel = random.choice(channels)
        merchant = random.choice(merchants)

        # base risk
        risk_score = 0.0
        if amount > 1000:
            risk_score += 0.4
        if country != "US":
            risk_score += 0.3
        if channel == "ONLINE":
            risk_score += 0.2
        if random.random() < 0.1:
            # Random weird extra boost
            risk_score += 0.3

        # clamp
        risk_score = min(risk_score, 1.0)

        # convert risk_score + global fraud_rate into a binary label
        is_high_risk = risk_score > (1.0 - fraud_rate)

        row = {
            "transaction_id": str(uuid.uuid4()),
            "card_id": f"C{random.randint(100000, 999999)}",
            "customer_id": f"U{random.randint(10000, 99999)}",
            "merchant": merchant,
            "amount": amount,
            "currency": "USD",
            "country": country,
            "channel": channel,
            "event_time_utc": tx_time.isoformat(),
            "is_high_risk": int(is_high_risk),
            "risk_score": round(risk_score, 2),
        }
        rows.append(row)

    return rows


def main():
    parser = argparse.ArgumentParser(description="Generate fake credit-card transactions.")
    parser.add_argument(
        "--n_rows",
        type=int,
        default=1000,
        help="Number of transactions to generate (default: 1000).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output CSV file path. If not set, uses data/raw/transactions_<timestamp>.csv",
    )
    args = parser.parse_args()

    rows = generate_transactions(n_rows=args.n_rows)

    df = pd.DataFrame(rows)

    # decide output path
    if args.output is None:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/raw/transactions_{timestamp}.csv"
    else:
        output_path = args.output

    df.to_csv(output_path, index=False)
    print(f"Generated {len(df)} transactions -> {output_path}")

    # show a small sample
    print(df.head(5))


if __name__ == "__main__":
    main()
