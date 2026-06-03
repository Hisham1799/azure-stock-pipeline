# ============================================================
# Notebook 02: Incremental Load + SCD Type 2 using Delta MERGE
# Reads from silver, merges into a new scd2_stock_prices table
# ============================================================

# Storage access config
spark.conf.set(
    "fs.azure.account.key.hishamdelake01.dfs.core.windows.net",
    "<STORAGE_ACCOUNT_KEY>"
)

from pyspark.sql.functions import current_date, lit
from delta.tables import DeltaTable

# ── 1. Read today's silver data (the "incoming" batch) ──────
incoming_df = spark.read.format("delta").load(
    "abfss://silver@hishamdelake01.dfs.core.windows.net/twelvedata/stock_prices"
)

# ── 2. Compute today's aggregates (same as gold, but per-day snapshot) ──
snapshot_df = incoming_df.groupBy("symbol").agg(
    {"close": "avg", "high": "max", "volume": "sum"}
).withColumnRenamed("avg(close)", "avg_closing_price") \
 .withColumnRenamed("max(high)", "highest_peak") \
 .withColumnRenamed("sum(volume)", "total_volume") \
 .withColumn("valid_from", current_date()) \
 .withColumn("valid_to", lit(None).cast("date")) \
 .withColumn("is_current", lit(True))

snapshot_df.show()

from delta.tables import DeltaTable

scd2_path = "abfss://silver@hishamdelake01.dfs.core.windows.net/twelvedata/scd2_stock_prices"

# ── 3. Write initial table if it doesn't exist, else MERGE ──
if not DeltaTable.isDeltaTable(spark, scd2_path):
    # First run — just write the snapshot as-is
    snapshot_df.write.format("delta").mode("overwrite").save(scd2_path)
    print("Initial table created.")
else:
    # Subsequent runs — SCD2 MERGE logic
    scd2_table = DeltaTable.forPath(spark, scd2_path)
    
    # Step 1: Close old records where symbol matches and value changed
    scd2_table.update(
        condition="scd2.symbol = snapshot.symbol AND scd2.is_current = true AND scd2.avg_closing_price != snapshot.avg_closing_price",
        set={"is_current": "false", "valid_to": "snapshot.valid_from"}
    )
    
    # Step 2: Insert new records
    snapshot_df.write.format("delta").mode("append").save(scd2_path)
    print("MERGE complete — history preserved.")

# Verify
spark.read.format("delta").load(scd2_path).show()

from pyspark.sql.functions import lit
from pyspark.sql import Row
from delta.tables import DeltaTable

# Simulate next day's data with slightly different prices
simulated_new_data = spark.createDataFrame([
    Row(symbol="AAPL",  avg_closing_price=275.00, highest_peak=320.00, total_volume=4800000000),
    Row(symbol="GOOGL", avg_closing_price=334.08, highest_peak=408.60, total_volume=3160513966),
    Row(symbol="MSFT",  avg_closing_price=420.00, highest_peak=490.00, total_volume=3700000000),
]).withColumn("valid_from", lit("2026-06-04").cast("date")) \
  .withColumn("valid_to", lit(None).cast("date")) \
  .withColumn("is_current", lit(True))

# Step 1: Close changed records
scd2_table = DeltaTable.forPath(spark, scd2_path)

scd2_table.update(
    condition="is_current = true",
    set={
        "is_current": lit(False),
        "valid_to": lit("2026-06-04").cast("date")
    }
)

# Step 2: Append new records
simulated_new_data.write.format("delta").mode("append").save(scd2_path)

# Show full history
print("Full SCD2 history:")
spark.read.format("delta").load(scd2_path).orderBy("symbol", "valid_from").show()
