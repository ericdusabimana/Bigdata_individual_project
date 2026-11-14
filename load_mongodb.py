
#!/usr/bin/env python3
"""
AUCA E-commerce Project - Part 1: MongoDB Bulk Loader

Expected files in the same directory:

    users.json
    categories.json
    products.json
    transactions.json
    sessions_0.json
    sessions_1.json
    ...
    sessions_19.json

Features:
    - Supports JSON arrays, JSON Lines and single JSON objects.
    - Streams large JSON arrays using ijson.
    - Converts Python Decimal values to BSON Decimal128.
    - Preserves existing users.
    - Uses upsert to prevent duplicate records.
    - Supports safe reruns after interrupted imports.
    - Imports all session files into one sessions collection.
    - Creates MongoDB indexes.

Usage:

    python load_mongodb.py

    python load_mongodb.py --replace

WARNING:
    --replace DROPS all five target collections.
"""

import argparse
import json
import os
import re
import sys

from pathlib import Path
from decimal import Decimal

try:
    from pymongo import (
        MongoClient,
        ASCENDING,
        DESCENDING,
        ReplaceOne
    )

    from bson.decimal128 import Decimal128

except ImportError:

    print(
        "Missing MongoDB packages.\n"
        "Install using:\n"
        "python -m pip install pymongo"
    )

    sys.exit(1)


try:
    import ijson

except ImportError:

    ijson = None


# ==================================================
# CONFIGURATION
# ==================================================

BATCH_SIZE = 5000

DATABASE_NAME = "ecommerce_analytics"

MONGO_URI = os.getenv(
    "MONGODB_URI",
    "mongodb://localhost:27017/"
)

BASE_DIR = Path(__file__).resolve().parent


# File name and matching key
DATA_FILES = {

    "users": (
        "users.json",
        "user_id"
    ),

    "categories": (
        "categories.json",
        "category_id"
    ),

    "products": (
        "products.json",
        "product_id"
    ),

    "transactions": (
        "transactions.json",
        "transaction_id"
    )
}


SESSION_KEY = "session_id"


COLLECTION_NAMES = [

    "users",
    "categories",
    "products",
    "transactions",
    "sessions"

]


# ==================================================
# DECIMAL CONVERSION
# ==================================================

def convert_decimals(value):
    """
    Recursively convert Python Decimal values
    into BSON Decimal128.

    This preserves decimal precision for prices,
    transaction amounts and other financial fields.
    """

    if isinstance(value, Decimal):

        return Decimal128(value)

    elif isinstance(value, dict):

        return {
            key: convert_decimals(item)
            for key, item in value.items()
        }

    elif isinstance(value, list):

        return [
            convert_decimals(item)
            for item in value
        ]

    elif isinstance(value, tuple):

        return [
            convert_decimals(item)
            for item in value
        ]

    return value


# ==================================================
# SESSION FILE SORTING
# ==================================================

def session_sort_key(path):
    """
    Sort session files numerically.

    sessions_0.json
    sessions_1.json
    ...
    sessions_19.json
    """

    match = re.search(
        r"sessions_(\d+)\.json$",
        path.name
    )

    if match:

        return int(match.group(1))

    return 999999


# ==================================================
# JSON FILE READER
# ==================================================

def first_non_whitespace(path, block_size=65536):

    with open(
        path,
        "r",
        encoding="utf-8-sig"
    ) as f:

        while True:

            chunk = f.read(block_size)

            if not chunk:

                return ""

            for char in chunk:

                if not char.isspace():

                    return char


def iter_json_documents(path):
    """
    Read JSON documents without loading large
    JSON arrays into memory.
    """

    first = first_non_whitespace(path)

    # ----------------------------------------------
    # JSON ARRAY
    # ----------------------------------------------

    if first == "[":

        if ijson is None:

            raise RuntimeError(
                f"{path.name} is a JSON array.\n"
                "Install ijson using:\n"
                "python -m pip install ijson"
            )

        with open(path, "rb") as f:

            for document in ijson.items(f, "item"):

                if isinstance(document, dict):

                    yield document

        return

    # ----------------------------------------------
    # JSON OBJECT OR JSON LINES
    # ----------------------------------------------

    if first == "{":

        with open(
            path,
            "r",
            encoding="utf-8-sig"
        ) as f:

            first_line = f.readline()

        try:

            first_obj = json.loads(first_line)

            is_jsonl = isinstance(first_obj, dict)

        except json.JSONDecodeError:

            is_jsonl = False

        # ------------------------------------------
        # JSON LINES
        # ------------------------------------------

        if is_jsonl:

            with open(
                path,
                "r",
                encoding="utf-8-sig"
            ) as f:

                for line_number, line in enumerate(
                    f,
                    start=1
                ):

                    line = line.strip()

                    if not line:

                        continue

                    try:

                        document = json.loads(line)

                    except json.JSONDecodeError as exc:

                        raise ValueError(
                            f"Invalid JSON in "
                            f"{path.name}, "
                            f"line {line_number}: {exc}"
                        ) from exc

                    if isinstance(document, dict):

                        yield document

            return

        # ------------------------------------------
        # PRETTY-PRINTED JSON OBJECT OR LIST
        # ------------------------------------------

        with open(
            path,
            "r",
            encoding="utf-8-sig"
        ) as f:

            parsed = json.load(f)

        if isinstance(parsed, dict):

            yield parsed

        elif isinstance(parsed, list):

            for document in parsed:

                if isinstance(document, dict):

                    yield document

        return

    raise ValueError(
        f"Unsupported or empty JSON file: {path}"
    )


# ==================================================
# MONGODB BULK IMPORT
# ==================================================

def import_file(collection, path, key_field):

    processed = 0

    upserted = 0

    modified = 0

    batch = []

    def flush_batch():

        nonlocal processed
        nonlocal upserted
        nonlocal modified
        nonlocal batch

        if not batch:

            return

        operations = []

        for document in batch:

            key_value = document.get(key_field)

            if key_value is None:

                raise ValueError(
                    f"{path.name}: missing required "
                    f"field '{key_field}' in a document."
                )

            operations.append(

                ReplaceOne(
                    {
                        key_field: key_value
                    },

                    document,

                    upsert=True
                )

            )

        result = collection.bulk_write(
            operations,
            ordered=False
        )

        processed += len(operations)

        upserted += result.upserted_count

        modified += result.modified_count

        print(
            f"  {path.name}: "
            f"processed={processed:,} | "
            f"new={upserted:,} | "
            f"updated={modified:,}",
            flush=True
        )

        batch = []

    # ----------------------------------------------
    # READ AND PROCESS DOCUMENTS
    # ----------------------------------------------

    for document in iter_json_documents(path):

        # Convert Decimal values recursively
        document = convert_decimals(document)

        batch.append(document)

        if len(batch) >= BATCH_SIZE:

            flush_batch()

    # Remaining documents
    flush_batch()

    return {

        "processed": processed,

        "upserted": upserted,

        "modified": modified

    }


# ==================================================
# INDEX CREATION
# ==================================================

def create_index_if_field_exists(
    collection,
    field,
    direction=ASCENDING
):

    top_level_field = field.split(".")[0]

    sample = collection.find_one(
        {},
        {
            top_level_field: 1
        }
    )

    if (
        sample is not None
        and top_level_field in sample
    ):

        collection.create_index(
            [
                (field, direction)
            ],
            background=True
        )

        print(
            f"  Index created: "
            f"{collection.name}.{field}"
        )


def create_indexes(db):

    print("\nCreating indexes...")

    index_plan = {

        "users": [

            ("user_id", ASCENDING)

        ],

        "categories": [

            ("category_id", ASCENDING)

        ],

        "products": [

            ("product_id", ASCENDING),

            ("category_id", ASCENDING),

            ("subcategory_id", ASCENDING),

            ("is_active", ASCENDING)

        ],

        "transactions": [

            ("transaction_id", ASCENDING),

            ("user_id", ASCENDING),

            ("timestamp", DESCENDING),

            ("transaction_date", DESCENDING)

        ],

        "sessions": [

            ("session_id", ASCENDING),

            ("user_id", ASCENDING),

            ("start_time", DESCENDING),

            ("viewed_products", ASCENDING)

        ]

    }

    # ----------------------------------------------
    # SINGLE FIELD INDEXES
    # ----------------------------------------------

    for collection_name, fields in index_plan.items():

        collection = db[collection_name]

        for field, direction in fields:

            create_index_if_field_exists(
                collection,
                field,
                direction
            )

    # ----------------------------------------------
    # COMPOUND INDEXES
    # ----------------------------------------------

    compound_indexes = [

        (
            "transactions",
            [
                "user_id",
                "timestamp"
            ]
        ),

        (
            "sessions",
            [
                "user_id",
                "start_time"
            ]
        ),

        (
            "products",
            [
                "category_id",
                "is_active"
            ]
        )

    ]

    for collection_name, fields in compound_indexes:

        collection = db[collection_name]

        sample = collection.find_one(
            {},
            {
                field: 1
                for field in fields
            }
        )

        if (
            sample is not None
            and all(
                field in sample
                for field in fields
            )
        ):

            collection.create_index(

                [
                    (field, ASCENDING)
                    for field in fields
                ],

                background=True

            )

            print(
                f"  Compound index created: "
                f"{collection_name} {fields}"
            )


# ==================================================
# MAIN
# ==================================================

def main():

    parser = argparse.ArgumentParser(

        description=(
            "AUCA E-commerce MongoDB bulk loader"
        )

    )

    parser.add_argument(

        "--replace",

        action="store_true",

        help=(
            "Drop all target collections "
            "before importing. DESTRUCTIVE."
        )

    )

    args = parser.parse_args()

    # ----------------------------------------------
    # VERIFY REQUIRED FILES
    # ----------------------------------------------

    required_files = {}

    for name, (filename, key_field) in DATA_FILES.items():

        path = BASE_DIR / filename

        if not path.is_file():

            print(
                f"Missing required file: {path}"
            )

            sys.exit(2)

        required_files[name] = path

    # ----------------------------------------------
    # DETECT SESSION FILES
    # ----------------------------------------------

    session_files = sorted(

        BASE_DIR.glob("sessions_*.json"),

        key=session_sort_key

    )

    if not session_files:

        print(
            "No sessions_*.json files found."
        )

        sys.exit(2)

    print(
        f"\nSession files detected: "
        f"{len(session_files)}"
    )

    for path in session_files:

        print(
            f"  {path.name}"
        )

    # ----------------------------------------------
    # CONNECT TO MONGODB
    # ----------------------------------------------

    client = MongoClient(

        MONGO_URI,

        serverSelectionTimeoutMS=5000

    )

    try:

        client.admin.command("ping")

        db = client[DATABASE_NAME]

        print(
            f"\nMongoDB connected: "
            f"{DATABASE_NAME}"
        )

        print(
            f"Data directory: {BASE_DIR}"
        )

        # ------------------------------------------
        # OPTIONAL DESTRUCTIVE RELOAD
        # ------------------------------------------

        if args.replace:

            print(
                "\nWARNING: --replace specified."
            )

            print(
                "Dropping all target collections..."
            )

            for name in COLLECTION_NAMES:

                db[name].drop()

        # ------------------------------------------
        # EXISTING RECORD COUNTS
        # ------------------------------------------

        existing = {

            name: db[name].count_documents({})

            for name in COLLECTION_NAMES

        }

        print(
            "\nExisting MongoDB records:"
        )

        for name, count in existing.items():

            print(
                f"  {name:15s}: {count:,}"
            )

        # ------------------------------------------
        # PRESERVE EXISTING USERS
        # ------------------------------------------

        skip_users = existing["users"] > 0

        if skip_users:

            print(
                "\nExisting users collection "
                "will be preserved."
            )

            print(
                "users.json will NOT be imported."
            )

        # ------------------------------------------
        # BUILD IMPORT LIST
        # ------------------------------------------

        inputs = []

        if not skip_users:

            inputs.append(

                (
                    "users",
                    required_files["users"],
                    "user_id"
                )

            )

        for name in [

            "categories",
            "products",
            "transactions"

        ]:

            inputs.append(

                (
                    name,
                    required_files[name],
                    DATA_FILES[name][1]
                )

            )

        # All session files go into one collection
        for path in session_files:

            inputs.append(

                (
                    "sessions",
                    path,
                    SESSION_KEY
                )

            )

        # ------------------------------------------
        # IMPORT TOTALS
        # ------------------------------------------

        totals = {

            name: {

                "processed": 0,

                "upserted": 0,

                "modified": 0

            }

            for name in COLLECTION_NAMES

        }

        # ------------------------------------------
        # EXECUTE IMPORT
        # ------------------------------------------

        for collection_name, path, key_field in inputs:

            print(
                f"\nImporting {path.name} "
                f"-> {collection_name}"
            )

            print(
                f"Matching field: {key_field}"
            )

            stats = import_file(

                db[collection_name],

                path,

                key_field

            )

            for metric, value in stats.items():

                totals[collection_name][metric] += value

            print(

                f"Finished {path.name}: "

                f"processed={stats['processed']:,}, "

                f"new={stats['upserted']:,}, "

                f"updated={stats['modified']:,}"

            )

        # ------------------------------------------
        # CREATE INDEXES
        # ------------------------------------------

        create_indexes(db)

        # ------------------------------------------
        # FINAL SUMMARY
        # ------------------------------------------

        print(
            "\n========== IMPORT SUMMARY =========="
        )

        for name in COLLECTION_NAMES:

            actual = db[name].count_documents({})

            stats = totals[name]

            print(

                f"{name:15s} "

                f"processed={stats['processed']:>12,} "

                f"new={stats['upserted']:>12,} "

                f"updated={stats['modified']:>12,} "

                f"total_in_db={actual:>12,}"

            )

        print(
            "\nImport completed successfully."
        )

    finally:

        client.close()


# ==================================================
# ENTRY POINT
# ==================================================

if __name__ == "__main__":

    main()
