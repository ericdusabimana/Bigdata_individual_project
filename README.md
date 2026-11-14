# AUCA E-commerce Big Data Project

## README: MongoDB, HBase and Apache Spark Queries

This guide documents example queries for the project's three main data
technologies. MongoDB stores customer, product and transaction
documents; HBase stores browsing-session records; Spark integrates and
analyzes the exported data.

## 1. Project architecture

  -----------------------------------------------------------------------
  Technology              Role in the project     Main data
  ----------------------- ----------------------- -----------------------
  MongoDB                 Document database and   `users`, `products`,
                          source extraction       `transactions`

  HBase                   Distributed storage and `user_sessions`
                          retrieval of session    
                          activity                

  Apache Spark            Data integration,       MongoDB extracts and
                          transformation and      HBase session exports
                          analytics               
  -----------------------------------------------------------------------

Part 3 joins customer, purchase and browsing information using
`user_id`. Spark calculates customer revenue, completed orders, average
order value, session statistics, page views, engagement levels and
customer segments.

------------------------------------------------------------------------

## 2. MongoDB queries

Run these commands in `mongosh`.

### 2.1 Select the database and list collections

``` javascript
use ecommerce_analytics
show collections
```

**Explanation:** Selects the project's MongoDB database and lists its
collections, including customer profiles, products and transactions.

### 2.2 Inspect customer profiles

``` javascript
db.users.find(
  {},
  {
    _id: 0,
    user_id: 1,
    city: 1,
    state: 1,
    country: 1,
    registration_date: 1,
    last_active: 1
  }
).limit(5)
```

**Explanation:** Displays five customer examples and only the profile
fields relevant to customer analysis. The projection hides MongoDB's
internal `_id`.

### 2.3 Count customers

``` javascript
db.users.countDocuments({})
```

**Explanation:** Counts all documents in the customer collection. The
Part 3 run integrated 10,000 customer profiles.

### 2.4 Inspect transactions

``` javascript
db.transactions.find(
  {},
  {
    _id: 0,
    transaction_id: 1,
    session_id: 1,
    user_id: 1,
    timestamp: 1,
    subtotal: 1,
    discount: 1,
    total: 1,
    payment_method: 1,
    status: 1
  }
).limit(5)
```

**Explanation:** Displays a sample of transactions, including the
customer identifier, timestamp, monetary fields, payment method and
status.

### 2.5 Count transactions by status

``` javascript
db.transactions.aggregate([
  {
    $group: {
      _id: "$status",
      transaction_count: { $sum: 1 },
      transaction_value: { $sum: "$total" }
    }
  },
  { $sort: { transaction_count: -1 } }
])
```

**Explanation:** Groups transactions by status and calculates the number
and total value of transactions in each group. This distinguishes
completed purchases from other states.

### 2.6 Calculate completed orders and revenue per customer

``` javascript
db.transactions.aggregate([
  { $match: { status: "completed" } },
  {
    $group: {
      _id: "$user_id",
      completed_orders: { $sum: 1 },
      customer_revenue: { $sum: "$total" },
      average_order_value: { $avg: "$total" }
    }
  },
  { $sort: { customer_revenue: -1 } },
  { $limit: 10 }
])
```

**Explanation:** Filters to completed transactions, groups them by
customer and calculates order count, revenue and average order value.
Sorting identifies the ten customers with the highest observed-period
revenue.

### 2.7 Revenue by payment method

``` javascript
db.transactions.aggregate([
  { $match: { status: "completed" } },
  {
    $group: {
      _id: "$payment_method",
      transaction_count: { $sum: 1 },
      revenue: { $sum: "$total" }
    }
  },
  { $sort: { revenue: -1 } }
])
```

**Explanation:** Compares completed transaction counts and revenue
across payment methods.

### 2.8 Top-selling products from transaction items

``` javascript
db.transactions.aggregate([
  { $match: { status: "completed" } },
  { $unwind: "$items" },
  {
    $group: {
      _id: "$items.product_id",
      units_sold: { $sum: "$items.quantity" },
      product_revenue: { $sum: "$items.subtotal" }
    }
  },
  { $sort: { units_sold: -1 } },
  { $limit: 10 }
])
```

**Explanation:** `$unwind` separates the `items` array so each purchased
product can be grouped independently. The query calculates units sold
and item subtotal revenue per product.

------------------------------------------------------------------------

## 3. HBase queries

Run these commands in the HBase shell.

### 3.1 Start HBase shell and list tables

``` bash
hbase shell
```

``` ruby
list
```

**Explanation:** Starts the interactive HBase command-line interface and
lists available tables. The project's session table is `user_sessions`.

### 3.2 Inspect the table structure

``` ruby
describe 'user_sessions'
```

**Explanation:** Displays the table schema. The project table has five
known column families:

-   `activity`
-   `device`
-   `location`
-   `session`
-   `user`

A column family groups related columns in HBase. The exact qualifiers
can be inspected from the scan output.

### 3.3 Scan a small number of rows

``` ruby
scan 'user_sessions', { LIMIT => 5 }
```

**Explanation:** Returns up to five rows and shows their row keys and
stored columns. This is the recommended first query when inspecting the
actual HBase data layout.

### 3.4 Scan one column family at a time

``` ruby
scan 'user_sessions', { COLUMNS => ['activity'], LIMIT => 5 }
scan 'user_sessions', { COLUMNS => ['device'], LIMIT => 5 }
scan 'user_sessions', { COLUMNS => ['location'], LIMIT => 5 }
scan 'user_sessions', { COLUMNS => ['session'], LIMIT => 5 }
scan 'user_sessions', { COLUMNS => ['user'], LIMIT => 5 }
```

**Explanation:** Restricts the scan to one known column family at a
time. This helps inspect activity, device, location, session and user
attributes separately without assuming specific column qualifiers.

### 3.5 Retrieve one row by row key

The project's row-key format is:

``` text
user_id#start_time#session_id
```

Copy a real row key from the `scan` output and use it here:

``` ruby
get 'user_sessions', 'PASTE_AN_ACTUAL_ROW_KEY_HERE'
```

**Explanation:** Retrieves one specific HBase row. Replace the sample
text with a row key that actually exists in the table.

### 3.6 Count rows

``` ruby
count 'user_sessions'
```

**Explanation:** Counts rows in the table. The Part 3 export completed
with 2,000,000 HBase rows. Spark reported 1,999,999 unique sessions; row
count and distinct session count are not necessarily the same measure.

### 3.7 Why HBase is used

HBase provides distributed storage for the large volume of
browsing-session records. Its row-key design supports targeted
retrieval, while column families organize related session attributes. In
this project, HBase session data is exported and processed by Spark to
calculate customer engagement measures.

------------------------------------------------------------------------

## 4. Apache Spark queries

The following examples use PySpark and the Part 3 CSV outputs. Run them
from the project root directory, where the `part3_output` folder is
located.

### 4.1 Create a Spark session

``` python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("AUCA_Ecommerce_Query_Analysis")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
```

**Explanation:** Creates a Spark application using two local worker
threads. This is a reasonable local configuration for the project's 16
GB RAM computer.

### 4.2 Load the Part 3 analytical outputs

``` python
base = "part3_output"

customer_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{base}/customer_clv_engagement")
)

segment_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{base}/customer_segment_summary")
)

country_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{base}/country_performance")
)

engagement_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{base}/engagement_vs_purchases")
)
```

**Explanation:** Reads the CSV result folders created by Part 3 into
Spark DataFrames. Schema inference identifies numeric and text columns
so they can be queried.

### 4.3 Inspect the customer analytics data

``` python
customer_df.printSchema()
customer_df.show(5, truncate=False)
```

**Explanation:** Displays the DataFrame's column names and data types,
followed by five complete sample records. This is useful for validating
the loaded data.

### 4.4 Find the ten highest-revenue customers

``` python
from pyspark.sql.functions import col

(
    customer_df
    .select(
        "user_id",
        "country",
        "completed_orders",
        "lifetime_revenue",
        "average_order_value",
        "session_count",
        "total_page_views",
        "customer_segment"
    )
    .orderBy(col("lifetime_revenue").desc())
    .show(10, truncate=False)
)
```

**Explanation:** Sorts customers by revenue in descending order and
displays the top ten. Purchase and engagement fields are shown together
to support customer-value analysis.

### 4.5 Summarize customer segments

``` python
(
    segment_df
    .select(
        "customer_segment",
        "customers",
        "completed_orders",
        "revenue",
        "average_sessions_per_customer",
        "average_page_views_per_customer"
    )
    .orderBy(col("revenue").desc())
    .show(truncate=False)
)
```

**Explanation:** Compares the customer segments created by Part 3. It
reports customer count, completed orders, revenue and average engagement
measures.

### 4.6 Rank countries by revenue

``` python
(
    country_df
    .select(
        "country",
        "customers",
        "completed_orders",
        "revenue",
        "average_sessions_per_customer"
    )
    .orderBy(col("revenue").desc())
    .show(10, truncate=False)
)
```

**Explanation:** Ranks countries by observed-period revenue and displays
supporting customer, order and session measures.

### 4.7 Compare engagement and purchasing

``` python
(
    engagement_df
    .select(
        "engagement_level",
        "customers",
        "completed_orders",
        "revenue",
        "average_customer_revenue",
        "average_page_views"
    )
    .orderBy(col("average_customer_revenue").desc())
    .show(truncate=False)
)
```

**Explanation:** Compares average customer revenue and page views for
each engagement level. This supports analysis of the relationship
between browsing and purchasing behavior.

### 4.8 Calculate revenue/page-view correlation

``` python
correlation = customer_df.stat.corr(
    "lifetime_revenue",
    "total_page_views"
)

print("Revenue/page-view Pearson correlation:", correlation)
```

**Explanation:** Calculates the Pearson correlation coefficient between
customer revenue and total page views. The Part 3 run reported
approximately `0.2914`, a positive but relatively modest linear
association. Correlation does not prove that browsing causes purchases.

### 4.9 Count customers in each segment

``` python
(
    customer_df
    .groupBy("customer_segment")
    .count()
    .orderBy(col("count").desc())
    .show(truncate=False)
)
```

**Explanation:** Groups customers by segment and counts the records in
each group. This provides a direct check of the customer-segment
distribution.

### 4.10 Use Spark SQL

``` python
customer_df.createOrReplaceTempView("customer_analytics")

query = """
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    SUM(completed_orders) AS completed_orders,
    ROUND(SUM(lifetime_revenue), 2) AS total_revenue,
    ROUND(AVG(total_page_views), 2) AS average_page_views
FROM customer_analytics
GROUP BY customer_segment
ORDER BY total_revenue DESC
"""

spark.sql(query).show(truncate=False)
```

**Explanation:** Registers the DataFrame as a temporary SQL view and
executes a SQL aggregation. It calculates customer count, completed
orders, total revenue and average page views per segment. This
demonstrates Spark SQL as well as the DataFrame API.

### 4.11 Stop Spark

``` python
spark.stop()
```

**Explanation:** Stops the Spark session and releases its resources
after the analysis is complete.

------------------------------------------------------------------------

## 5. How MongoDB, HBase and Spark work together

1.  **MongoDB extraction:** Customer profiles and transaction documents
    are read from `ecommerce_analytics`. The transaction export uses a
    defined snapshot boundary.
2.  **HBase extraction:** Session rows are scanned from `user_sessions`
    and exported to JSON Lines files. The export process can resume from
    its checkpoint.
3.  **Spark integration:** Spark reads the extracted customer,
    transaction and session data and combines customer-related records
    using `user_id`.
4.  **Analytics:** Spark calculates completed orders, observed-period
    revenue, average order value, session count, session duration, page
    views, engagement levels and customer segments.
5.  **Output:** Spark writes an integrated Parquet dataset and
    analytical CSV folders. The Part 4 Python script reads the CSV
    folders to generate static PNG charts.

### Why each technology fits

-   **MongoDB:** Document storage is suitable for customer profiles and
    transaction documents, including the transaction `items` array.
-   **HBase:** Distributed, column-family storage is suitable for the
    large session dataset and row-key-based retrieval.
-   **Apache Spark:** Distributed processing is suitable for joining
    extracted datasets, performing aggregations and calculating
    analytical measures.

------------------------------------------------------------------------

## 6. Part 3 execution summary

The successful Part 3 execution produced these results:

  Measure                                        Result
  ----------------------------------------- -----------
  Integrated customers                           10,000
  Transactions in extracted snapshot            449,474
  Completed transactions                        284,011
  HBase rows exported                         2,000,000
  Unique HBase sessions reported by Spark     1,999,999
  Customers with completed orders                10,000
  Customers with sessions                        10,000
  Revenue/page-view Pearson correlation          0.2914

**Snapshot limitation:** The transaction count is the extracted snapshot
of 449,474 records, not the expected final 500,000 generated records.
Use the actual snapshot count when reporting results.

**HBase count check:** The export contains 2,000,000 rows while Spark
reports 1,999,999 unique sessions. Treat row count and distinct session
count as separate measures and investigate the one-record difference if
required.

**CLV terminology:** The project calculates observed-period customer
revenue as a CLV proxy. It is not a predictive lifetime-value model
because it does not forecast future customer purchases.

------------------------------------------------------------------------

## 7. Part 4 visualization execution

Run the Part 4 script from the project directory:

``` bash
python part4_visualizations.py --input part3_output --output part4_output
```

Expected output:

``` text
part4_output/
    01_customer_revenue_distribution.png
    02_customer_segment_comparison.png
    03_top_countries_by_revenue.png
    04_engagement_vs_average_revenue.png
    part4_interpretation.txt
```

Insert the four PNG charts into the final report and explain the
findings shown by each chart.

------------------------------------------------------------------------

## 8. Running notes

-   Run MongoDB commands in `mongosh`.
-   Run HBase commands in `hbase shell`.
-   Run PySpark code with the Python environment configured for the
    project.
-   Run Spark examples from the project root so the relative
    `part3_output` path resolves correctly. If necessary, replace it
    with the full output-folder path.
-   The HBase `get` example requires a real row key copied from `scan`.
-   The HBase examples use the five known family names. Inspect `scan`
    output before writing queries that refer to individual qualifiers.
-   Start the relevant MongoDB and HBase services before running their
    shell commands.
