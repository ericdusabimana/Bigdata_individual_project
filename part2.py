#!/usr/bin/env python3
from pathlib import Path
import os
import glob
import json
import shutil
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR
OUTPUT_DIR = BASE_DIR / "part2_output"

BATCH_OUTPUT = OUTPUT_DIR / "cleaned"
RECOMMENDATION_OUTPUT = OUTPUT_DIR / "recommendations"
SQL_OUTPUT = OUTPUT_DIR / "sql_analytics"

# 16 GB physical RAM: reserve memory for Windows, PyCharm and Python.
DRIVER_MEMORY = "4g"
LOCAL_THREADS = 2
SHUFFLE_PARTITIONS = 16
SESSION_RECORDS_PER_FILE = 10000
MIN_COPURCHASE_COUNT = 2

SESSION_JSONL_DIR = BASE_DIR / "sessions_jsonl"
SESSION_READY_MARKER = SESSION_JSONL_DIR / "_SUCCESS"


for directory in [
    BATCH_OUTPUT,
    RECOMMENDATION_OUTPUT,
    SQL_OUTPUT
]:
    directory.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("AUCA_PART2")


# ============================================================
# 3. CREATE SPARK SESSION
# ============================================================


def create_spark_session():
    # Apply driver memory before the Java gateway starts.
    # In local mode, the driver JVM also runs executor tasks.
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-memory {DRIVER_MEMORY} pyspark-shell"
    )

    spark = (
        SparkSession.builder
        .appName("AUCA_Ecommerce_Part2_Analytics")
        .master(f"local[{LOCAL_THREADS}]")
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.default.parallelism", str(SHUFFLE_PARTITIONS))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "Africa/Kigali")
        .config("spark.network.timeout", "300s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.debug.maxToStringFields", "100")
        .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark version: %s", spark.version)
    logger.info("Spark driver memory target: %s", DRIVER_MEMORY)
    logger.info("Spark local threads: %s", LOCAL_THREADS)
    logger.info("Spark application started")
    return spark



# ============================================================
# 4. JSON DATA LOADING
# ============================================================

def load_json(spark, filename):

    file_path = DATA_DIR / filename

    if not file_path.exists():
        logger.warning("File not found: %s", file_path)
        return None

    logger.info("Loading %s", filename)

    # The generated datasets are JSON arrays.
    df = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .json(str(file_path))
    )

    # Avoid count() here: it would trigger an unnecessary full source scan.
    logger.info("Schema inferred for %s", filename)
    return df


def prepare_sessions_jsonl():
    """
    Convert the generated session JSON arrays into smaller JSON Lines files.

    The conversion runs once. A _SUCCESS marker is written only after all
    source files have been converted successfully.
    """
    source_files = sorted(
        DATA_DIR.glob("sessions_*.json"),
        key=lambda p: int(p.stem.split("_")[-1])
    )

    if not source_files:
        raise FileNotFoundError("No sessions_*.json files found")

    if SESSION_READY_MARKER.exists():
        logger.info("Using existing converted sessions: %s", SESSION_JSONL_DIR)
        return

    logger.info(
        "Preparing session JSON Lines files from %s source files",
        len(source_files)
    )

    if SESSION_JSONL_DIR.exists():
        shutil.rmtree(SESSION_JSONL_DIR)
    SESSION_JSONL_DIR.mkdir(parents=True, exist_ok=True)

    output_index = 0
    total_records = 0
    buffer = []

    def flush_buffer():
        nonlocal output_index
        if not buffer:
            return

        output_file = SESSION_JSONL_DIR / f"sessions_{output_index:05d}.jsonl"
        with output_file.open("w", encoding="utf-8") as out:
            for record in buffer:
                out.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False))
                out.write("\n")

        logger.info(
            "Created %s with %s records",
            output_file.name,
            len(buffer)
        )
        buffer.clear()
        output_index += 1

    for source_file in source_files:
        logger.info("Converting %s", source_file.name)

        # Only one 100,000-record source file is held by Python at a time.
        with source_file.open("r", encoding="utf-8") as src:
            source_records = json.load(src)

        if isinstance(source_records, dict):
            source_records = source_records.get("sessions", [])

        if not isinstance(source_records, list):
            raise ValueError(f"Expected a JSON array in {source_file.name}")

        for record in source_records:
            buffer.append(record)
            total_records += 1

            if len(buffer) >= SESSION_RECORDS_PER_FILE:
                flush_buffer()

        del source_records

    flush_buffer()

    with SESSION_READY_MARKER.open("w", encoding="utf-8") as marker:
        marker.write(f"records={total_records}\n")
        marker.write(f"files={output_index}\n")

    logger.info(
        "Session conversion completed: %s records in %s JSONL files",
        total_records,
        output_index
    )





def load_sessions(spark):
    logger.info("Loading converted session JSON Lines data")

    session_dir = Path(SESSION_JSONL_DIR)

    # Resolve the actual JSONL files using Python
    jsonl_files = sorted(session_dir.glob("*.jsonl"))

    if not jsonl_files:
        raise FileNotFoundError(
            f"No JSONL files found in: {session_dir.resolve()}"
        )

    logger.info(
        "Found %s session JSONL files",
        len(jsonl_files)
    )

    # Pass explicit file paths to Spark instead of a wildcard
    file_paths = [
        file.resolve().as_posix()
        for file in jsonl_files
    ]

    sessions = (
        spark.read
        .option("multiLine", "false")
        .json(file_paths)
    )

    logger.info("Session JSONL files loaded into Spark")

    return sessions




# ============================================================
# 5. DATA CLEANING AND NORMALIZATION
# ============================================================

def clean_products(products):

    logger.info("Cleaning products")

    products = products.dropDuplicates(["product_id"])

    products = (
        products
        .withColumn(
            "base_price",
            F.col("base_price").cast("double")
        )
        .withColumn(
            "current_stock",
            F.col("current_stock").cast("integer")
        )
        .withColumn(
            "creation_date",
            F.to_timestamp("creation_date")
        )
        .withColumn(
            "is_active",
            F.coalesce(
                F.col("is_active").cast("boolean"),
                F.lit(False)
            )
        )
        .withColumn(
            "name",
            F.trim(F.col("name"))
        )
        .withColumn(
            "category_id",
            F.trim(F.col("category_id"))
        )
    )

    products = products.filter(
        F.col("product_id").isNotNull()
        & F.col("name").isNotNull()
        & (F.col("base_price") >= 0)
    )

    products = products.fillna({
        "current_stock": 0
    })

    return products


def clean_transactions(transactions):

    logger.info("Cleaning transactions")

    transactions = transactions.dropDuplicates(
        ["transaction_id"]
    )

    transactions = (
        transactions
        .withColumn(
            "timestamp",
            F.to_timestamp("timestamp")
        )
        .withColumn(
            "subtotal",
            F.col("subtotal").cast("double")
        )
        .withColumn(
            "discount",
            F.col("discount").cast("double")
        )
        .withColumn(
            "total",
            F.col("total").cast("double")
        )
        .withColumn(
            "status",
            F.lower(F.trim(F.col("status")))
        )
        .withColumn(
            "payment_method",
            F.lower(F.trim(F.col("payment_method")))
        )
    )

    transactions = transactions.filter(
        F.col("transaction_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("timestamp").isNotNull()
        & F.col("items").isNotNull()
    )

    transactions = transactions.filter(
        F.col("total").isNotNull()
        & (F.col("total") >= 0)
    )

    return transactions


def clean_sessions(sessions):

    logger.info("Cleaning sessions")

    sessions = sessions.dropDuplicates(
        ["session_id"]
    )

    sessions = (
        sessions
        .withColumn(
            "start_time",
            F.to_timestamp("start_time")
        )
        .withColumn(
            "end_time",
            F.to_timestamp("end_time")
        )
        .withColumn(
            "duration_seconds",
            F.col("duration_seconds").cast("long")
        )
    )

    sessions = sessions.filter(
        F.col("session_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("start_time").isNotNull()
    )

    sessions = sessions.withColumn(
        "duration_seconds",
        F.when(
            F.col("duration_seconds").isNull(),
            F.lit(0)
        ).otherwise(
            F.greatest(F.col("duration_seconds"), F.lit(0))
        )
    )

    return sessions


# ============================================================
# 6. NORMALIZE TRANSACTION LINE ITEMS
# ============================================================

def create_transaction_items(transactions):

    logger.info("Normalizing transaction items")

    items = (
        transactions
        .select(
            "transaction_id",
            "user_id",
            "timestamp",
            "status",
            F.explode("items").alias("item")
        )
        .select(
            "transaction_id",
            "user_id",
            "timestamp",
            "status",
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").cast("integer").alias("quantity"),
            F.col("item.unit_price").cast("double").alias("unit_price"),
            F.col("item.subtotal").cast("double").alias("subtotal")
        )
    )

    # Recalculate missing line-item subtotal.
    items = items.withColumn(
        "subtotal",
        F.coalesce(
            F.col("subtotal"),
            F.col("quantity") * F.col("unit_price")
        )
    )

    items = items.filter(
        F.col("product_id").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") >= 0)
        & (F.col("subtotal") >= 0)
    )

    return items


# ============================================================
# 7. PRODUCT CO-PURCHASE RECOMMENDATION ENGINE
# ============================================================

def calculate_recommendations(transactions, items):

    logger.info("Calculating product recommendations")

    # Use completed transactions only.
    completed = transactions.filter(
        F.col("status") == "completed"
    )

    completed_items = (
        items
        .filter(F.col("status") == "completed")
        .select(
            "transaction_id",
            "product_id"
        )
        .dropDuplicates([
            "transaction_id",
            "product_id"
        ])
    )

    total_transactions = completed.select(
        "transaction_id"
    ).distinct().count()

    logger.info(
        "Completed transactions: %s",
        total_transactions
    )

    # Number of transactions containing each product.
    product_counts = (
        completed_items
        .groupBy("product_id")
        .agg(
            F.count("*").alias("product_transaction_count")
        )
    )

    # Find products purchased together in the same transaction.
    first = completed_items.alias("a")
    second = completed_items.alias("b")

    pairs = (
        first.join(
            second,
            (
                (F.col("a.transaction_id") ==
                 F.col("b.transaction_id"))
                &
                (F.col("a.product_id") <
                 F.col("b.product_id"))
            ),
            "inner"
        )
        .select(
            F.col("a.product_id").alias("product_a"),
            F.col("b.product_id").alias("product_b"),
            F.col("a.transaction_id").alias("transaction_id")
        )
    )

    pair_counts = (
        pairs
        .groupBy("product_a", "product_b")
        .agg(
            F.count("*").alias("co_purchase_count")
        )
    )

    # Generate both recommendation directions:
    # A -> B and B -> A.
    forward = pair_counts.select(
        F.col("product_a").alias("product_a"),
        F.col("product_b").alias("product_b"),
        "co_purchase_count"
    )

    reverse = pair_counts.select(
        F.col("product_b").alias("product_a"),
        F.col("product_a").alias("product_b"),
        "co_purchase_count"
    )

    directional_pairs = forward.unionByName(reverse)

    # Calculate support, confidence and lift.
    recommendations = (
        directional_pairs
        .join(
            product_counts.alias("a"),
            F.col("product_a") == F.col("a.product_id")
        )
        .join(
            product_counts.alias("b"),
            F.col("product_b") == F.col("b.product_id")
        )
        .select(
            "product_a",
            "product_b",
            "co_purchase_count",
            F.col("a.product_transaction_count")
            .alias("product_a_count"),
            F.col("b.product_transaction_count")
            .alias("product_b_count")
        )
        .filter(
            F.col("co_purchase_count") >= MIN_COPURCHASE_COUNT
        )
        .withColumn(
            "support",
            F.col("co_purchase_count") /
            F.lit(max(total_transactions, 1))
        )
        .withColumn(
            "confidence",
            F.col("co_purchase_count") /
            F.col("product_a_count")
        )
        .withColumn(
            "lift",
            F.col("confidence") /
            (
                F.col("product_b_count") /
                F.lit(max(total_transactions, 1))
            )
        )
        .orderBy(
            F.desc("lift"),
            F.desc("co_purchase_count")
        )
    )

    return recommendations


# ============================================================
# 8. SPARK SQL ANALYTICS
# ============================================================

def run_spark_sql(
    spark,
    users,
    products,
    transactions,
    items,
    sessions
):

    logger.info("Registering Spark SQL views")

    products.createOrReplaceTempView("products")
    transactions.createOrReplaceTempView("transactions")
    items.createOrReplaceTempView("transaction_items")
    sessions.createOrReplaceTempView("sessions")

    # --------------------------------------------------------
    # QUERY 1: Product and category sales performance
    # --------------------------------------------------------

    category_sales = spark.sql("""
        SELECT
            p.category_id,
            p.product_id,
            p.name AS product_name,

            SUM(i.quantity) AS units_sold,

            ROUND(SUM(i.subtotal), 2)
                AS total_item_revenue,

            COUNT(DISTINCT i.transaction_id)
                AS number_of_transactions

        FROM transaction_items i

        INNER JOIN products p
            ON i.product_id = p.product_id

        WHERE i.status = 'completed'

        GROUP BY
            p.category_id,
            p.product_id,
            p.name

        ORDER BY total_item_revenue DESC
    """)

    category_sales.show(20, truncate=False)

    category_sales.write.mode("overwrite").parquet(
        str(SQL_OUTPUT / "product_sales")
    )

    # --------------------------------------------------------
    # QUERY 2: Category-level revenue
    # --------------------------------------------------------

    category_summary = spark.sql("""
        SELECT
            p.category_id,

            SUM(i.quantity) AS total_units_sold,

            ROUND(SUM(i.subtotal), 2)
                AS category_revenue,

            COUNT(DISTINCT i.transaction_id)
                AS transaction_count,

            COUNT(DISTINCT i.product_id)
                AS products_sold

        FROM transaction_items i

        INNER JOIN products p
            ON i.product_id = p.product_id

        WHERE i.status = 'completed'

        GROUP BY p.category_id

        ORDER BY category_revenue DESC
    """)

    category_summary.show(25, truncate=False)

    category_summary.write.mode("overwrite").parquet(
        str(SQL_OUTPUT / "category_summary")
    )

    # --------------------------------------------------------
    # QUERY 3: User purchasing and browsing engagement
    # --------------------------------------------------------

    transaction_summary = spark.sql("""
        SELECT
            user_id,

            COUNT(DISTINCT transaction_id)
                AS total_transactions,

            ROUND(SUM(total), 2)
                AS total_spending,

            ROUND(AVG(total), 2)
                AS average_transaction_value

        FROM transactions

        WHERE status = 'completed'

        GROUP BY user_id
    """)

    transaction_summary.createOrReplaceTempView(
        "user_transaction_summary"
    )

    session_summary = spark.sql("""
        SELECT
            user_id,

            COUNT(DISTINCT session_id)
                AS total_sessions,

            ROUND(
                AVG(duration_seconds), 2
            ) AS average_session_duration,

            SUM(duration_seconds)
                AS total_session_duration

        FROM sessions

        GROUP BY user_id
    """)

    session_summary.createOrReplaceTempView(
        "user_session_summary"
    )

    # If the users dataset exists, join it to the summaries.
    if users is not None and "user_id" in users.columns:

        users.createOrReplaceTempView("users")

        engagement = spark.sql("""
            SELECT
                u.user_id,

                COALESCE(t.total_transactions, 0)
                    AS total_transactions,

                COALESCE(t.total_spending, 0)
                    AS total_spending,

                COALESCE(t.average_transaction_value, 0)
                    AS average_transaction_value,

                COALESCE(s.total_sessions, 0)
                    AS total_sessions,

                COALESCE(s.average_session_duration, 0)
                    AS average_session_duration

            FROM users u

            LEFT JOIN user_transaction_summary t
                ON u.user_id = t.user_id

            LEFT JOIN user_session_summary s
                ON u.user_id = s.user_id
        """)

    else:

        engagement = spark.sql("""
            SELECT
                COALESCE(
                    t.user_id,
                    s.user_id
                ) AS user_id,

                COALESCE(t.total_transactions, 0)
                    AS total_transactions,

                COALESCE(t.total_spending, 0)
                    AS total_spending,

                COALESCE(t.average_transaction_value, 0)
                    AS average_transaction_value,

                COALESCE(s.total_sessions, 0)
                    AS total_sessions,

                COALESCE(s.average_session_duration, 0)
                    AS average_session_duration

            FROM user_transaction_summary t

            FULL OUTER JOIN user_session_summary s
                ON t.user_id = s.user_id
        """)

    engagement.show(20, truncate=False)

    engagement.write.mode("overwrite").parquet(
        str(SQL_OUTPUT / "user_engagement")
    )

    return category_sales, category_summary, engagement


# ============================================================
# 9. SAVE DATASETS
# ============================================================

def save_cleaned_data(
    products,
    transactions,
    items,
    sessions
):

    logger.info("Saving cleaned datasets")

    products.write.mode("overwrite").parquet(
        str(BATCH_OUTPUT / "products")
    )

    transactions.write.mode("overwrite").parquet(
        str(BATCH_OUTPUT / "transactions")
    )

    items.write.mode("overwrite").parquet(
        str(BATCH_OUTPUT / "transaction_items")
    )

    logger.info("Writing cleaned sessions in distributed Parquet partitions")

    (
        sessions
        .repartition(SHUFFLE_PARTITIONS)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .option("maxRecordsPerFile", 50000)
        .parquet(str(BATCH_OUTPUT / "sessions"))
    )


# ============================================================
# 10. MAIN EXECUTION
# ============================================================

def main():

    spark = create_spark_session()

    try:

        logger.info("=" * 60)
        logger.info("AUCA PROJECT - PART 2 STARTED")
        logger.info("=" * 60)

        # Load source datasets.
        products_raw = load_json(spark, "products.json")
        transactions_raw = load_json(spark, "transactions.json")
        users = load_json(spark, "users.json")
        sessions_raw = load_sessions(spark)

        if products_raw is None:
            raise FileNotFoundError("products.json missing")

        if transactions_raw is None:
            raise FileNotFoundError("transactions.json missing")

        # Clean and normalize.
        products = clean_products(products_raw)

        transactions = clean_transactions(
            transactions_raw
        )

        sessions = clean_sessions(sessions_raw)

        # Normalize nested transaction items.
        items = create_transaction_items(
            transactions
        )

        # Save cleaned datasets.
        save_cleaned_data(
            products,
            transactions,
            items,
            sessions
        )

        # Product recommendation analysis.
        recommendations = calculate_recommendations(
            transactions,
            items
        )

        recommendations.show(30, truncate=False)

        recommendations.write.mode("overwrite").parquet(
            str(RECOMMENDATION_OUTPUT / "product_recommendations")
        )

        recommendations.write.mode(
            "overwrite"
        ).option(
            "header", "true"
        ).option(
            "maxRecordsPerFile", 100000
        ).csv(
            str(RECOMMENDATION_OUTPUT / "csv")
        )

        # Spark SQL analytics.
        run_spark_sql(
            spark,
            users,
            products,
            transactions,
            items,
            sessions
        )

        logger.info("=" * 60)
        logger.info("PART 2 PROCESSING COMPLETED")
        logger.info("Output directory: %s", OUTPUT_DIR)
        logger.info("=" * 60)

    except Exception:

        logger.exception("Part 2 processing failed")
        raise

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
