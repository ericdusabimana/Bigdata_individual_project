#!/usr/bin/env python3
"""AUCA E-commerce Analytics - Part 3: MongoDB + HBase + Spark integration.

Streams a fixed MongoDB transaction snapshot and the HBase user_sessions table
into JSONL files, then uses PySpark to calculate customer value and engagement.
Designed for a Windows/PyCharm workstation with 16 GB RAM (local[2], 4 GB driver).

Install Python packages in the project's venv:
    pip install pymongo happybase thrift
PySpark and Java should already be configured for Part 2.

Defaults can be overridden with environment variables:
    MONGO_URI=mongodb://localhost:27017/
    MONGO_DB=ecommerce_analytics
    HBASE_HOST=localhost
    HBASE_PORT=9090
    HBASE_TABLE=user_sessions
    PART3_OUTPUT=part3_output
"""

import os
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

from pymongo import MongoClient, DESCENDING, ASCENDING
from bson import ObjectId
from bson.decimal128 import Decimal128

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DecimalType
)

# ----------------------------- Configuration -----------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "ecommerce_analytics")
HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))
HBASE_TABLE = os.getenv("HBASE_TABLE", "user_sessions")
OUTPUT_DIR = os.getenv("PART3_OUTPUT", "part3_output")
RAW_DIR = os.path.join(OUTPUT_DIR, "raw_snapshot")
os.makedirs(RAW_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [PART3] %(message)s"
)
log = logging.getLogger("part3")

# ------------------------------ Serialization ----------------------------
def json_safe(value):
    """Convert MongoDB/BSON values to JSON-safe values without losing money."""
    if isinstance(value, Decimal128):
        return str(value.to_decimal())
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items() if k != "_id"}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return value


def write_json_line(handle, value):
    """Write one UTF-8 JSONL record to either a text or binary file.

    MongoDB exports use text mode (w); resumable HBase exports use binary
    append mode (ab) so checkpoint offsets are measured in bytes. Encode only
    for binary handles to avoid the bytes-like-object TypeError.
    """
    line = json.dumps(
        json_safe(value),
        ensure_ascii=False,
        separators=(",", ":")
    ) + "\n"

    mode = getattr(handle, "mode", "")
    if "b" in mode:
        handle.write(line.encode("utf-8"))
    else:
        handle.write(line)


def export_mongodb_snapshot():
    """Export users and a fixed transaction cohort bounded by the max _id at start."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    db = client[MONGO_DB]
    users_col = db["users"]
    tx_col = db["transactions"]

    users_path = os.path.join(RAW_DIR, "users.jsonl")
    tx_path = os.path.join(RAW_DIR, "transactions.jsonl")

    log.info("MongoDB connected: database=%s", MONGO_DB)
    user_count = users_col.count_documents({})
    with open(users_path, "w", encoding="utf-8") as out:
        cursor = users_col.find(
            {},
            {"_id": 0, "user_id": 1, "geo_data": 1,
             "registration_date": 1, "last_active": 1}
        ).sort("user_id", ASCENDING).batch_size(2000)
        exported_users = 0
        for doc in cursor:
            geo = doc.get("geo_data") or {}
            row = {
                "user_id": doc.get("user_id"),
                "city": geo.get("city"),
                "state": geo.get("state"),
                "country": geo.get("country"),
                "registration_date": doc.get("registration_date"),
                "last_active": doc.get("last_active")
            }
            write_json_line(out, row)
            exported_users += 1

    # Establish a stable upper _id boundary before reading transactions. This
    # excludes documents inserted after the snapshot boundary while the export runs.
    max_doc = tx_col.find_one({}, {"_id": 1}, sort=[("_id", DESCENDING)])
    if not max_doc:
        client.close()
        raise RuntimeError("MongoDB transactions collection is empty.")
    cutoff_id = max_doc["_id"]
    query = {"_id": {"$lte": cutoff_id}}
    snapshot_tx_count = tx_col.count_documents(query)
    log.info("Transaction snapshot boundary _id=%s; documents=%s", cutoff_id, snapshot_tx_count)

    projection = {
        "_id": 0, "transaction_id": 1, "session_id": 1, "user_id": 1,
        "timestamp": 1, "total": 1, "subtotal": 1, "discount": 1,
        "status": 1, "payment_method": 1, "items": 1
    }
    exported_tx = 0
    with open(tx_path, "w", encoding="utf-8") as out:
        cursor = tx_col.find(query, projection).sort("_id", ASCENDING).limit(snapshot_tx_count).batch_size(2000)
        for doc in cursor:
            items = doc.get("items") or []
            quantity = 0
            for item in items:
                try:
                    quantity += int(item.get("quantity", 0) or 0)
                except (TypeError, ValueError):
                    pass
            row = {
                "transaction_id": doc.get("transaction_id"),
                "session_id": doc.get("session_id"),
                "user_id": doc.get("user_id"),
                "timestamp": doc.get("timestamp"),
                "total": doc.get("total"),
                "subtotal": doc.get("subtotal"),
                "discount": doc.get("discount"),
                "status": doc.get("status"),
                "payment_method": doc.get("payment_method"),
                "item_quantity": quantity
            }
            write_json_line(out, row)
            exported_tx += 1
            if exported_tx and exported_tx % 50000 == 0:
                log.info("Exported %s/%s transactions", exported_tx, snapshot_tx_count)

    client.close()
    log.info("MongoDB export complete: users=%s, transactions=%s", exported_users, exported_tx)
    return {
        "mongo_database": MONGO_DB,
        "users_count_at_export_start": user_count,
        "users_exported": exported_users,
        "transactions_snapshot_count": snapshot_tx_count,
        "transactions_exported": exported_tx,
        "transaction_snapshot_max_object_id": str(cutoff_id),
        "transaction_snapshot_created_at": datetime.now().astimezone().isoformat(),
        "users_file": users_path,
        "transactions_file": tx_path
    }

# ------------------------------- HBase export ----------------------------
def decode_bytes(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def parse_json_if_possible(value):
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            return json.loads(stripped)
        except Exception:
            return value
    return value


def recursive_find(obj, aliases):
    """Find the first matching key recursively, case-insensitively."""
    wanted = {a.lower() for a in aliases}
    if isinstance(obj, dict):
        for key, value in obj.items():
            if str(key).lower() in wanted and value not in (None, ""):
                return value
        for value in obj.values():
            found = recursive_find(value, aliases)
            if found not in (None, ""):
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = recursive_find(value, aliases)
            if found not in (None, ""):
                return found
    return None


def as_int(value, default=0):
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return len(value)
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def export_hbase_sessions():
    """Export HBase sessions with resumable checkpoints and scanner retries.

    Checkpoint stores the last fully committed HBase row key and byte offset
    in the JSONL file. If a Thrift scanner fails, the output is rolled back to
    that offset and scanning resumes after the committed row key.
    """
    try:
        import happybase
    except ImportError as exc:
        raise RuntimeError("Missing happybase. Install with: pip install happybase thrift") from exc

    import time

    path = os.path.join(RAW_DIR, "hbase_sessions.jsonl")
    checkpoint_path = os.path.join(RAW_DIR, "hbase_sessions.checkpoint.json")
    log.info("Connecting to HBase Thrift at %s:%s, table=%s", HBASE_HOST, HBASE_PORT, HBASE_TABLE)

    def save_checkpoint(last_key, file_offset, total_exported):
        tmp = checkpoint_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as cp:
            json.dump({
                "last_row_key": last_key,
                "file_offset": file_offset,
                "exported": total_exported,
                "updated_at": datetime.now().astimezone().isoformat()
            }, cp)
            cp.flush()
            os.fsync(cp.fileno())
        os.replace(tmp, checkpoint_path)

    # Recover the last committed point. If a prior run used the older exporter
    # without a checkpoint, derive the row key from the last JSONL record.
    last_key = None
    committed_offset = 0
    total_exported = 0
    if os.path.exists(checkpoint_path):
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as cp:
                saved = json.load(cp)
            last_key = saved.get("last_row_key")
            committed_offset = int(saved.get("file_offset", 0))
            total_exported = int(saved.get("exported", 0))
            log.info("Resuming from checkpoint: exported=%s, last_row_key=%s", total_exported, last_key)
        except Exception as exc:
            log.warning("Could not read HBase checkpoint (%s); recovering from JSONL tail", exc)
            last_key = None
            committed_offset = 0
            total_exported = 0

    if last_key is None and os.path.exists(path) and os.path.getsize(path) > 0:
        # Existing output from the first attempt: recover last valid complete line.
        with open(path, "rb+") as existing:
            existing.seek(0, os.SEEK_END)
            file_size = existing.tell()
            existing.seek(0)
            last_good_offset = 0
            last_record = None
            while True:
                line = existing.readline()
                if not line:
                    break
                try:
                    last_record = json.loads(line.decode("utf-8"))
                    last_good_offset = existing.tell()
                except Exception:
                    break
            if last_record:
                parts = [last_record.get("user_id"), last_record.get("start_time"), last_record.get("session_id")]
                if all(x not in (None, "") for x in parts):
                    last_key = "#".join(str(x) for x in parts)
                    committed_offset = last_good_offset
                    # Count complete exported records in the existing file.
                    existing.seek(0)
                    total_exported = sum(1 for _ in existing if _.strip())
                    log.info("Recovered existing HBase export: %s rows; resume key=%s", total_exported, last_key)
            if last_key is None:
                # No usable tail: safely start a fresh export.
                committed_offset = 0
                total_exported = 0
            existing.truncate(committed_offset)

    # Ensure file exists and discard any uncommitted bytes from a prior failure.
    with open(path, "ab") as out:
        out.truncate(committed_offset)

    retry_delay = 5
    max_retries = 12
    retries = 0
    batch_size = 250  # smaller Thrift batches reduce scanner response size

    while True:
        connection = None
        try:
            connection = happybase.Connection(
                host=HBASE_HOST,
                port=HBASE_PORT,
                timeout=300000,
                autoconnect=False,
                table_prefix=None
            )
            connection.open()
            table = connection.table(HBASE_TABLE)
            log.info("Starting HBase scan from %s", last_key or "beginning")

            rows_since_checkpoint = 0
            with open(path, "ab") as out:
                scan_kwargs = {"batch_size": batch_size}
                if last_key is not None:
                    # HappyBase row_start is inclusive; skip the checkpoint row below.
                    scan_kwargs["row_start"] = last_key.encode("utf-8")

                for row_key, cells in table.scan(**scan_kwargs):
                    key_text = decode_bytes(row_key) or ""
                    if last_key is not None and key_text <= last_key:
                        continue

                    parts = key_text.split("#", 2)
                    cell_map = {}
                    for raw_key, raw_value in cells.items():
                        col = decode_bytes(raw_key) or ""
                        family, _, qualifier = col.partition(":")
                        cell_map.setdefault(family, {})[qualifier] = parse_json_if_possible(decode_bytes(raw_value))

                    record = cell_map.get("session", {}).get("record_json")
                    if not isinstance(record, (dict, list)):
                        record = {}
                    user_data = cell_map.get("user", {})
                    activity_data = cell_map.get("activity", {})
                    device_data = cell_map.get("device", {})
                    location_data = cell_map.get("location", {})

                    user_id = (user_data.get("user_id") or recursive_find(record, ["user_id", "userId"])
                               or (parts[0] if parts else None))
                    session_id = (cell_map.get("session", {}).get("session_id")
                                  or recursive_find(record, ["session_id", "sessionId"])
                                  or (parts[2] if len(parts) > 2 else None))
                    start_time = (cell_map.get("session", {}).get("start_time")
                                  or recursive_find(record, ["start_time", "startTime", "timestamp"])
                                  or (parts[1] if len(parts) > 1 else None))
                    end_time = (cell_map.get("session", {}).get("end_time")
                                or recursive_find(record, ["end_time", "endTime"]))
                    duration = (cell_map.get("session", {}).get("duration_seconds")
                                or recursive_find(record, ["duration_seconds", "duration", "session_duration"]))
                    page_views = (activity_data.get("page_views")
                                  or recursive_find(record, ["page_views", "pageViews", "views_count"]))
                    viewed_products = (activity_data.get("viewed_products")
                                       or recursive_find(record, ["viewed_products", "viewedProducts", "products_viewed"]))
                    if isinstance(viewed_products, str):
                        viewed_products = parse_json_if_possible(viewed_products)
                    if isinstance(viewed_products, list):
                        viewed_product_count = len(viewed_products)
                    elif viewed_products in (None, ""):
                        viewed_product_count = 0
                    else:
                        viewed_product_count = as_int(viewed_products, 0)

                    if duration in (None, "") and start_time and end_time:
                        try:
                            start_dt = datetime.fromisoformat(str(start_time).replace("Z", "+00:00"))
                            end_dt = datetime.fromisoformat(str(end_time).replace("Z", "+00:00"))
                            duration = max(0, (end_dt - start_dt).total_seconds())
                        except Exception:
                            duration = 0

                    row = {
                        "user_id": str(user_id) if user_id is not None else None,
                        "session_id": str(session_id) if session_id is not None else None,
                        "start_time": str(start_time) if start_time is not None else None,
                        "end_time": str(end_time) if end_time is not None else None,
                        "duration_seconds": as_int(duration, 0),
                        "page_views": as_int(page_views, 0),
                        "viewed_product_count": viewed_product_count,
                        "device_type": str(recursive_find(device_data, ["device_type", "type", "device"]) or
                                            recursive_find(record, ["device_type", "deviceType"]) or "unknown"),
                        "city": str(recursive_find(location_data, ["city"]) or recursive_find(record, ["city"]) or "unknown"),
                        "country": str(recursive_find(location_data, ["country"]) or recursive_find(record, ["country"]) or "unknown")
                    }
                    if row["user_id"]:
                        write_json_line(out, row)
                        total_exported += 1
                        rows_since_checkpoint += 1

                    # Commit in chunks: flush file first, then atomically checkpoint it.
                    if rows_since_checkpoint >= 1000:
                        out.flush()
                        os.fsync(out.fileno())
                        committed_offset = out.tell()
                        last_key = key_text
                        save_checkpoint(last_key, committed_offset, total_exported)
                        rows_since_checkpoint = 0
                        if total_exported % 100000 < 1000:
                            log.info("Exported %s HBase sessions", total_exported)

                # Normal end of scan: commit the final partial batch.
                out.flush()
                os.fsync(out.fileno())
                committed_offset = out.tell()
                # A scan with no new rows still finishes normally.
                if 'key_text' in locals() and key_text:
                    last_key = key_text
                save_checkpoint(last_key, committed_offset, total_exported)

            log.info("HBase export complete: %s sessions", total_exported)
            break

        except Exception as exc:
            retries += 1
            log.exception("HBase scan failed (attempt %s/%s): %s", retries, max_retries, exc)
            if retries > max_retries:
                raise RuntimeError(
                    "HBase Thrift scan failed after repeated retries. The partial JSONL export and "
                    "checkpoint have been preserved; rerun this script to resume. Check the HBase "
                    "Thrift server logs and confirm it is still listening."
                ) from exc
            # Roll back any records written after the last durable checkpoint.
            with open(path, "ab") as out:
                out.truncate(committed_offset)
            log.warning("Retrying HBase scan in %s seconds from row key %s", retry_delay, last_key)
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass

    return {
        "hbase_table": HBASE_TABLE,
        "hbase_sessions_exported": total_exported,
        "hbase_sessions_file": path,
        "hbase_resume_checkpoint": checkpoint_path
    }

# --------------------------------- Spark ----------------------------------
def write_csv(df, name):
    target = os.path.join(OUTPUT_DIR, name)
    (df.coalesce(1).write.mode("overwrite").option("header", "true").csv(target))
    log.info("Wrote CSV directory: %s", target)


def run_spark_analytics(snapshot_meta, hbase_meta):
    spark = (SparkSession.builder
             .appName("AUCA-Part3-MongoDB-HBase-Customer-Analytics")
             .master("local[2]")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.default.parallelism", "2")
             .config("spark.sql.session.timeZone", "Africa/Kigali")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    users_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("last_active", StringType(), True)
    ])
    tx_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("total", StringType(), True),
        StructField("subtotal", StringType(), True),
        StructField("discount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("item_quantity", IntegerType(), True)
    ])
    sessions_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("page_views", IntegerType(), True),
        StructField("viewed_product_count", IntegerType(), True),
        StructField("device_type", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True)
    ])

    users_path = snapshot_meta["users_file"]
    tx_path = snapshot_meta["transactions_file"]
    sessions_path = hbase_meta["hbase_sessions_file"]

    log.info("Spark reading exported MongoDB and HBase JSONL files")
    users = (spark.read.schema(users_schema).json(users_path)
             .filter(F.col("user_id").isNotNull()).dropDuplicates(["user_id"]))
    tx = (spark.read.schema(tx_schema).json(tx_path)
          .filter(F.col("user_id").isNotNull())
          .dropDuplicates(["transaction_id"])
          .withColumn("total_decimal", F.col("total").cast(DecimalType(18, 2)))
          .withColumn("transaction_time", F.to_timestamp("timestamp")))
    sessions = (spark.read.schema(sessions_schema).json(sessions_path)
                .filter(F.col("user_id").isNotNull())
                .dropDuplicates(["session_id"]))

    # Only completed purchases contribute to realized customer revenue.
    completed = tx.filter(F.lower(F.coalesce(F.col("status"), F.lit(""))) == "completed")
    tx_metrics = completed.groupBy("user_id").agg(
        F.countDistinct("transaction_id").alias("completed_orders"),
        F.sum("total_decimal").cast(DecimalType(20, 2)).alias("lifetime_revenue"),
        F.avg("total_decimal").cast(DecimalType(18, 2)).alias("average_order_value"),
        F.min("transaction_time").alias("first_purchase"),
        F.max("transaction_time").alias("last_purchase"),
        F.sum("item_quantity").alias("items_purchased")
    )

    session_metrics = sessions.groupBy("user_id").agg(
        F.countDistinct("session_id").alias("session_count"),
        F.sum("duration_seconds").alias("total_session_seconds"),
        F.avg("duration_seconds").cast("double").alias("average_session_seconds"),
        F.sum("page_views").alias("total_page_views"),
        F.sum("viewed_product_count").alias("total_products_viewed"),
        F.countDistinct("device_type").alias("device_types_used")
    )

    integrated = (users.join(tx_metrics, "user_id", "left")
                  .join(session_metrics, "user_id", "left")
                  .fillna({
                      "completed_orders": 0, "lifetime_revenue": 0,
                      "average_order_value": 0, "items_purchased": 0,
                      "session_count": 0, "total_session_seconds": 0,
                      "average_session_seconds": 0.0, "total_page_views": 0,
                      "total_products_viewed": 0, "device_types_used": 0
                  }))

    # Calculate robust percentile thresholds from the observed cohort.
    revenue_values = [float(x) for x in integrated.approxQuantile("lifetime_revenue", [0.25, 0.75], 0.02)]
    session_values = [float(x) for x in integrated.approxQuantile("session_count", [0.25, 0.75], 0.02)]
    revenue_q25, revenue_q75 = revenue_values if len(revenue_values) == 2 else (0.0, 0.0)
    session_q25, session_q75 = session_values if len(session_values) == 2 else (0.0, 0.0)

    integrated = (integrated
        .withColumn("engagement_level",
            F.when(F.col("session_count") >= F.lit(session_q75), "High")
             .when(F.col("session_count") >= F.lit(session_q25), "Medium")
             .when(F.col("session_count") > 0, "Low")
             .otherwise("No sessions"))
        .withColumn("value_level",
            F.when(F.col("lifetime_revenue") >= F.lit(revenue_q75), "High value")
             .when(F.col("lifetime_revenue") >= F.lit(revenue_q25), "Medium value")
             .when(F.col("lifetime_revenue") > 0, "Low value")
             .otherwise("No completed purchase"))
        .withColumn("customer_segment",
            F.when((F.col("lifetime_revenue") > 0) & (F.col("session_count") > 0) &
                   (F.col("lifetime_revenue") >= F.lit(revenue_q75)) &
                   (F.col("session_count") >= F.lit(session_q75)), "VIP - high value & engaged")
             .when(F.col("lifetime_revenue") >= F.lit(revenue_q75), "High-value buyer")
             .when((F.col("session_count") > 0) & (F.col("completed_orders") == 0), "Browser - no completed purchase")
             .when(F.col("completed_orders") > 0, "Regular/other buyer")
             .when(F.col("session_count") > 0, "Engaged browser")
             .otherwise("Inactive - no observed activity")))

    # Helpful recency field; reference date is the latest transaction timestamp in this snapshot.
    max_tx_time = completed.agg(F.max("transaction_time").alias("max_time")).first()["max_time"]
    if max_tx_time is not None:
        integrated = integrated.withColumn(
            "days_since_last_purchase",
            F.when(F.col("last_purchase").isNotNull(), F.datediff(F.lit(max_tx_time), F.to_date("last_purchase")))
             .otherwise(F.lit(None).cast(IntegerType()))
        )

    integrated = integrated.withColumn("lifetime_revenue", F.col("lifetime_revenue").cast(DecimalType(20, 2)))
    integrated.cache()

    # Persist core integrated result and user-facing analytical extracts.
    parquet_path = os.path.join(OUTPUT_DIR, "integrated_customer_analytics.parquet")
    integrated.write.mode("overwrite").parquet(parquet_path)
    write_csv(integrated.orderBy(F.desc("lifetime_revenue")), "customer_clv_engagement")

    segment_summary = (integrated.groupBy("customer_segment").agg(
        F.count("*").alias("customers"),
        F.sum("completed_orders").alias("completed_orders"),
        F.sum("lifetime_revenue").alias("revenue"),
        F.avg("session_count").alias("average_sessions_per_customer"),
        F.avg("total_page_views").alias("average_page_views_per_customer")
    ).orderBy(F.desc("revenue")))
    write_csv(segment_summary, "customer_segment_summary")

    country_summary = (integrated.groupBy("country").agg(
        F.count("*").alias("customers"),
        F.sum("completed_orders").alias("completed_orders"),
        F.sum("lifetime_revenue").alias("revenue"),
        F.avg("session_count").alias("average_sessions_per_customer")
    ).orderBy(F.desc("revenue")))
    write_csv(country_summary, "country_performance")

    status_summary = (tx.groupBy("status").agg(
        F.count("*").alias("transactions"),
        F.sum("total_decimal").cast(DecimalType(20, 2)).alias("transaction_value")
    ).orderBy(F.desc("transactions")))
    write_csv(status_summary, "transaction_status_summary")

    engagement_summary = (integrated.groupBy("engagement_level").agg(
        F.count("*").alias("customers"),
        F.sum("completed_orders").alias("completed_orders"),
        F.sum("lifetime_revenue").alias("revenue"),
        F.avg("lifetime_revenue").alias("average_customer_revenue"),
        F.avg("total_page_views").alias("average_page_views")
    ).orderBy(F.desc("revenue")))
    write_csv(engagement_summary, "engagement_vs_purchases")

    correlation = integrated.stat.corr("total_page_views", "lifetime_revenue")
    summary = {
        "users_in_integrated_result": integrated.count(),
        "transactions_in_snapshot": tx.count(),
        "completed_transactions": completed.count(),
        "unique_hbase_sessions": sessions.count(),
        "customers_with_completed_orders": tx_metrics.count(),
        "customers_with_sessions": session_metrics.count(),
        "revenue_session_pageview_pearson_correlation": correlation,
        "revenue_percentile_25": revenue_q25,
        "revenue_percentile_75": revenue_q75,
        "session_count_percentile_25": session_q25,
        "session_count_percentile_75": session_q75,
        "max_transaction_timestamp_in_snapshot": str(max_tx_time) if max_tx_time else None,
        "outputs": [
            "integrated_customer_analytics.parquet",
            "customer_clv_engagement/",
            "customer_segment_summary/",
            "country_performance/",
            "transaction_status_summary/",
            "engagement_vs_purchases/"
        ]
    }
    with open(os.path.join(OUTPUT_DIR, "part3_summary.json"), "w", encoding="utf-8") as out:
        json.dump(summary, out, indent=2, default=str)

    log.info("PART 3 SUMMARY: %s", json.dumps(summary, default=str))
    integrated.unpersist()
    spark.stop()
    return summary

# ---------------------------------- Main ----------------------------------
def main():
    log.info("Starting Part 3 integration pipeline")
    log.info("Output directory: %s", os.path.abspath(OUTPUT_DIR))

    mongo_meta = export_mongodb_snapshot()
    hbase_meta = export_hbase_sessions()

    metadata = {**mongo_meta, **hbase_meta}
    with open(os.path.join(OUTPUT_DIR, "source_snapshot_metadata.json"), "w", encoding="utf-8") as out:
        json.dump(metadata, out, indent=2, default=str)

    log.info("Source extraction finished. Starting Spark analytics.")
    summary = run_spark_analytics(mongo_meta, hbase_meta)

    log.info("=" * 70)
    log.info("PART 3 PROCESSING COMPLETED")
    log.info("MongoDB transaction snapshot: %s", mongo_meta["transactions_exported"])
    log.info("HBase sessions exported: %s", hbase_meta["hbase_sessions_exported"])
    log.info("Integrated customers: %s", summary["users_in_integrated_result"])
    log.info("Results: %s", os.path.abspath(OUTPUT_DIR))
    log.info("=" * 70)


if __name__ == "__main__":
    main()
