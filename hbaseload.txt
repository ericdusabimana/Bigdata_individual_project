
#!/usr/bin/env python3
"""
AUCA E-commerce Project - Part 1
HBase Session Data Loader

Input:
    sessions_0.json
    sessions_1.json
    ...
    sessions_19.json

HBase table:
    user_sessions

Row key:
    user_id#start_time#session_id

Column families:
    session
    user
    device
    location
    activity

Features:
    - Streams large JSON arrays using ijson.
    - Supports JSON Lines and JSON arrays.
    - Imports all session files into one HBase table.
    - Preserves nested device and location attributes.
    - Uses deterministic row keys to avoid duplicate rows.
    - Supports rerunning interrupted imports.
    - Displays import progress.
"""

import argparse
import json
import os
import re
import sys

from pathlib import Path
from decimal import Decimal
from datetime import datetime, date

import happybase
import ijson

try:
    import happybase
except ImportError:
    print("Install happybase: python -m pip install happybase")
    sys.exit(1)

try:
    import ijson
except ImportError:
    ijson = None


# ==================================================
# CONFIGURATION
# ==================================================

HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))

TABLE_NAME = "user_sessions"

BATCH_SIZE = 1000

BASE_DIR = Path(__file__).resolve().parent

SESSION_FILES_PATTERN = "sessions_*.json"

COLUMN_FAMILIES = {
    "session": {},
    "user": {},
    "device": {},
    "location": {},
    "activity": {}
}


# ==================================================
# SESSION FILE SORTING
# ==================================================

def session_sort_key(path):

    match = re.search(
        r"sessions_(\d+)\.json$",
        path.name
    )

    if match:
        return int(match.group(1))

    return 999999


# ==================================================
# JSON SERIALIZATION
# ==================================================

def json_serializer(value):

    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        separators=(",", ":")
    )


def to_bytes(value):

    if value is None:
        return None

    if isinstance(value, bytes):
        return value

    if isinstance(value, (dict, list, tuple)):
        value = json_serializer(value)

    elif isinstance(value, (Decimal, datetime, date)):
        value = str(value)

    elif isinstance(value, bool):
        value = "true" if value else "false"

    else:
        value = str(value)

    return value.encode("utf-8")


# ==================================================
# JSON FILE READER
# ==================================================

def first_non_whitespace(path, block_size=65536):

    with open(path, "r", encoding="utf-8-sig") as f:

        while True:

            chunk = f.read(block_size)

            if not chunk:
                return ""

            for char in chunk:

                if not char.isspace():
                    return char


def iter_json_documents(path):

    first = first_non_whitespace(path)

    # JSON ARRAY
    if first == "[":

        if ijson is None:

            raise RuntimeError(
                f"{path.name} requires ijson. "
                "Install with: python -m pip install ijson"
            )

        with open(path, "rb") as f:

            for document in ijson.items(f, "item"):

                if isinstance(document, dict):
                    yield document

        return

    # JSON OBJECT OR JSON LINES
    if first == "{":

        with open(path, "r", encoding="utf-8-sig") as f:
            first_line = f.readline()

        try:

            first_obj = json.loads(
                first_line,
                parse_float=Decimal
            )

            is_jsonl = isinstance(first_obj, dict)

        except json.JSONDecodeError:

            is_jsonl = False

        # JSON LINES
        if is_jsonl:

            with open(path, "r", encoding="utf-8-sig") as f:

                for line_number, line in enumerate(f, start=1):

                    line = line.strip()

                    if not line:
                        continue

                    try:

                        document = json.loads(
                            line,
                            parse_float=Decimal
                        )

                    except json.JSONDecodeError as exc:

                        raise ValueError(
                            f"Invalid JSON in {path.name}, "
                            f"line {line_number}: {exc}"
                        ) from exc

                    if isinstance(document, dict):
                        yield document

            return

        # PRETTY-PRINTED JSON OBJECT OR LIST
        with open(path, "r", encoding="utf-8-sig") as f:

            parsed = json.load(
                f,
                parse_float=Decimal
            )

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
# HBASE CONNECTION
# ==================================================

def connect_hbase():

    print(
        f"Connecting to HBase Thrift server "
        f"{HBASE_HOST}:{HBASE_PORT}..."
    )

    connection = happybase.Connection(
        host=HBASE_HOST,
        port=HBASE_PORT,
        timeout=120000,
        autoconnect=False
    )

    connection.open()

    # Verify that HBase responds
    connection.tables()

    print("HBase connection successful.")

    return connection


# ==================================================
# CREATE HBASE TABLE
# ==================================================

def create_table(connection):

    existing_tables = connection.tables()

    table_name_bytes = TABLE_NAME.encode("utf-8")

    if table_name_bytes in existing_tables:

        print(
            f"HBase table '{TABLE_NAME}' already exists."
        )

        return connection.table(TABLE_NAME)

    print(
        f"Creating HBase table '{TABLE_NAME}'..."
    )

    connection.create_table(
        TABLE_NAME,
        COLUMN_FAMILIES
    )

    print("HBase table created successfully.")

    return connection.table(TABLE_NAME)


# ==================================================
# FLATTEN NESTED DICTIONARIES
# ==================================================

def flatten_dictionary(data, prefix=""):

    result = {}

    if not isinstance(data, dict):
        return result

    for key, value in data.items():

        key = str(key)

        safe_key = re.sub(
            r"[^a-zA-Z0-9_]",
            "_",
            key
        )

        new_key = (
            f"{prefix}_{safe_key}"
            if prefix
            else safe_key
        )

        if isinstance(value, dict):

            result.update(
                flatten_dictionary(
                    value,
                    new_key
                )
            )

        elif isinstance(value, list):

            result[new_key] = json_serializer(value)

        else:

            result[new_key] = value

    return result


# ==================================================
# BUILD HBASE ROW KEY
# ==================================================

def build_row_key(document):

    user_id = document.get("user_id")

    session_id = document.get("session_id")

    start_time = document.get("start_time")

    if user_id is None:
        raise ValueError("Session is missing user_id.")

    if session_id is None:
        raise ValueError("Session is missing session_id.")

    if start_time is None:
        raise ValueError("Session is missing start_time.")

    # Keep the timestamp representation stable
    if isinstance(start_time, (dict, list)):
        start_time = json_serializer(start_time)

    row_key = (
        f"{user_id}#"
        f"{start_time}#"
        f"{session_id}"
    )

    return row_key.encode("utf-8")


# ==================================================
# BUILD HBASE COLUMN DATA
# ==================================================

def build_hbase_record(document):

    cells = {}

    # ----------------------------------------------
    # SESSION COLUMN FAMILY
    # ----------------------------------------------

    session_fields = [
        "session_id",
        "start_time",
        "end_time",
        "duration_seconds"
    ]

    for field in session_fields:

        if field in document:

            cells[
                f"session:{field}".encode("utf-8")
            ] = to_bytes(document[field])

    # Store complete source record for full fidelity
    cells[
        b"session:record_json"
    ] = to_bytes(document)

    # ----------------------------------------------
    # USER COLUMN FAMILY
    # ----------------------------------------------

    if "user_id" in document:

        cells[
            b"user:user_id"
        ] = to_bytes(document["user_id"])

    # ----------------------------------------------
    # DEVICE COLUMN FAMILY
    # ----------------------------------------------

    device_profile = document.get(
        "device_profile",
        {}
    )

    if isinstance(device_profile, dict):

        cells[
            b"device:profile_json"
        ] = to_bytes(device_profile)

        flattened = flatten_dictionary(
            device_profile
        )

        for field, value in flattened.items():

            qualifier = (
                f"device:{field}"
            ).encode("utf-8")

            cells[qualifier] = to_bytes(value)

    # ----------------------------------------------
    # LOCATION COLUMN FAMILY
    # ----------------------------------------------

    geo_data = document.get(
        "geo_data",
        {}
    )

    if isinstance(geo_data, dict):

        cells[
            b"location:geo_json"
        ] = to_bytes(geo_data)

        flattened = flatten_dictionary(
            geo_data
        )

        for field, value in flattened.items():

            qualifier = (
                f"location:{field}"
            ).encode("utf-8")

            cells[qualifier] = to_bytes(value)

    # ----------------------------------------------
    # ACTIVITY COLUMN FAMILY
    # ----------------------------------------------

    activity_fields = [
        "viewed_products",
        "page_views"
    ]

    for field in activity_fields:

        if field in document:

            cells[
                f"activity:{field}".encode("utf-8")
            ] = to_bytes(document[field])

    return cells


# ==================================================
# IMPORT ONE SESSION FILE
# ==================================================

def import_session_file(table, path):

    processed = 0

    batch = table.batch(
        batch_size=BATCH_SIZE
    )

    for document in iter_json_documents(path):

        row_key = build_row_key(document)

        cells = build_hbase_record(document)

        batch.put(
            row_key,
            cells
        )

        processed += 1

        if processed % BATCH_SIZE == 0:

            print(
                f"  {path.name}: "
                f"{processed:,} sessions processed",
                flush=True
            )

    # Flush remaining records
    batch.send()

    return processed


# ==================================================
# VERIFY HBASE DATA
# ==================================================

def verify_table(table):

    print("\nVerifying HBase table...")

    count = 0

    for row_key, data in table.scan(
        batch_size=1000
    ):

        count += 1

        if count <= 3:

            print(
                f"\nSample row {count}:"
            )

            print(
                "Row key:",
                row_key.decode("utf-8")
            )

            print(
                "Columns:",
                [
                    key.decode("utf-8")
                    for key in data.keys()
                ]
            )

        if count % 100000 == 0:

            print(
                f"Verified {count:,} rows...",
                flush=True
            )

    print(
        f"\nTotal HBase session rows: {count:,}"
    )

    return count


# ==================================================
# MAIN
# ==================================================

def main():

    parser = argparse.ArgumentParser(
        description="AUCA HBase session data loader"
    )

    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip full table scan after importing."
    )

    args = parser.parse_args()

    # ----------------------------------------------
    # FIND SESSION FILES
    # ----------------------------------------------

    session_files = sorted(
        BASE_DIR.glob(SESSION_FILES_PATTERN),
        key=session_sort_key
    )

    if not session_files:

        print(
            "No sessions_*.json files found."
        )

        sys.exit(2)

    print(
        f"\nSession files detected: {len(session_files)}"
    )

    for path in session_files:

        print(
            f"  {path.name}"
        )

    # ----------------------------------------------
    # CONNECT
    # ----------------------------------------------

    connection = None

    try:

        connection = connect_hbase()

        table = create_table(connection)

        # ------------------------------------------
        # IMPORT
        # ------------------------------------------

        total_processed = 0

        print(
            "\n========== HBASE SESSION IMPORT =========="
        )

        for path in session_files:

            print(
                f"\nImporting {path.name} "
                f"-> {TABLE_NAME}"
            )

            count = import_session_file(
                table,
                path
            )

            total_processed += count

            print(
                f"Finished {path.name}: "
                f"{count:,} sessions"
            )

        # ------------------------------------------
        # FINAL SUMMARY
        # ------------------------------------------

        print(
            "\n========== IMPORT SUMMARY =========="
        )

        print(
            f"Session files: {len(session_files)}"
        )

        print(
            f"Sessions processed: {total_processed:,}"
        )

        print(
            f"HBase table: {TABLE_NAME}"
        )

        print(
            f"Row key: user_id#start_time#session_id"
        )

        # ------------------------------------------
        # VERIFY
        # ------------------------------------------

        if not args.skip_verify:

            actual_count = verify_table(table)

            print(
                "\nExpected processed records:",
                f"{total_processed:,}"
            )

            print(
                "Actual unique HBase rows:",
                f"{actual_count:,}"
            )

        print(
            "\nHBase session import completed."
        )

    except Exception as exc:

        print(
            f"\nHBase import failed: {exc}"
        )

        raise

    finally:

        if connection is not None:

            connection.close()


if __name__ == "__main__":

    main()
