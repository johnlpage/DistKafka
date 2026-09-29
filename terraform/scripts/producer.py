#!/usr/bin/env python3
"""
Insert a document into bank.payments with a local machine timestamp and hostname.
Usage:
  python3 producer.py

Requires:
  pymongo
  kafka-python (not used here, but installed as a dependency)

Environment variables (from .env):
  MONGO_URI - MongoDB connection string
  MONGO_DB_NAME - database name (default: bank)
  MONGO_COLLECTION - collection name (default: payments)
"""

import os
import sys
import json
import socket
from datetime import datetime, timezone

try:
    from pymongo import MongoClient
except ImportError:
    print("ERROR: pymongo not installed. Run: pip3 install pymongo")
    sys.exit(1)


def load_env(env_path="/home/ec2-user/.env"):
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    # .env values are quoted (KEY="value") for safe `source`-ing
                    # in bash; strip matching quotes here since this parser
                    # isn't shell-aware.
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                        value = value[1:-1]
                    os.environ.setdefault(key, value)


def main():
    load_env()

    mongo_uri = os.environ.get("MONGO_URI")
    db_name = os.environ.get("MONGO_DB_NAME", "bank")
    collection_name = os.environ.get("MONGO_COLLECTION", "payments")
    hostname = socket.gethostname()
    timestamp = datetime.now(timezone.utc)

    if not mongo_uri:
        print("ERROR: MONGO_URI environment variable not set")
        print("Create a .env file or export MONGO_URI")
        sys.exit(1)

    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    doc = {
        "host_created": hostname,
        "time_created": timestamp.isoformat(),
        "message": f"Test document from {hostname} at {timestamp.isoformat()}",
        "source": "producer.py",
    }

    result = collection.insert_one(doc)
    print(json.dumps({
        "status": "inserted",
        "id": str(result.inserted_id),
        "host_created": hostname,
        "time_created": timestamp.isoformat(),
    }))


if __name__ == "__main__":
    main()