# Cell 1: Configure Spark Session to Access ADLS Gen2 directly
storage_account_name = "hishamdelake01"
storage_account_key = "<STORAGE_ACCOUNT_KEY>" 

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
print("Spark session successfully configured with storage account credentials!")

# Cell 2: Read the raw JSON stock data from the bronze container
dbutils.widgets.text("symbols", "AAPL,MSFT,GOOGL") # Match your default testing schema

# Construct the standard ABFSS path (Azure Blob File System Secured)
input_path = "abfss://bronze@hishamdelake01.dfs.core.windows.net/twelvedata/stocks.json"

# Read the JSON payload into a PySpark DataFrame
df_raw = spark.read.option("multiline", "true").json(input_path)

# Print the schema structure to see how Twelve Data nests the data
print("Raw JSON Schema Structure:")
df_raw.printSchema()

# Show the top rows of our raw read
display(df_raw)

# Cell 3: Dynamically flatten and union all stock payloads
from pyspark.sql.functions import col, explode, lit

# 1. Get the list of stock symbols present dynamically from the JSON columns
discovered_tickers = df_raw.columns
print(f"Dynamically discovered tickers in raw file: {discovered_tickers}")

# 2. Base empty DataFrame to hold our combined results
final_flat_df = None

# 3. Loop through each ticker, extract meta/values, and flatten them
for ticker in discovered_tickers:
    # Select the nested array of values and give it a clean handle
    ticker_df = df_raw.select(
        lit(ticker).alias("symbol"),
        explode(col(f"`{ticker}`.values")).alias("record")
    )
    
    # Flatten the individual dictionary items out of the exploded record struct
    ticker_flat_df = ticker_df.select(
        col("symbol"),
        col("record.datetime").alias("date_string"),
        col("record.open").cast("double").alias("open"),
        col("record.high").cast("double").alias("high"),
        col("record.low").cast("double").alias("low"),
        col("record.close").cast("double").alias("close"),
        col("record.volume").cast("long").alias("volume")
    )
    
    # Combine data frames using union
    if final_flat_df is None:
        final_flat_df = ticker_flat_df
    else:
        final_flat_df = final_flat_df.union(ticker_flat_df)

# 4. Display our flat, clean staging dataset
print("\nTransformation complete! Standardized table view:")
display(final_flat_df)

# Cell 4: Final formatting and writing out to the Delta Silver layer
from pyspark.sql.functions import to_date

# 1. Transform the raw string into a proper Date data type
final_silver_df = final_flat_df.withColumn("date", to_date("date_string", "yyyy-MM-dd")) \
                               .drop("date_string")

# 2. Reorder columns for optimized directory partitioning
final_silver_df = final_silver_df.select("symbol", "date", "open", "high", "low", "close", "volume")

# 3. Define our landing zone target in the storage lake
silver_output_path = "abfss://silver@hishamdelake01.dfs.core.windows.net/twelvedata/stock_prices"

# 4. Write the dataframe using the delta format
print("Writing data to the Silver Lake container in Delta format...")
final_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_output_path)

print("Write complete! Data safely landed in the Silver container.")

# Cell 5: Aggregate clean Silver records into business-facing Gold tables
from pyspark.sql.functions import avg, max, sum, round

print("Reading optimized data back from the Silver layer...")
# Read the Delta data we just saved
silver_df = spark.read.format("delta").load("abfss://silver@hishamdelake01.dfs.core.windows.net/twelvedata/stock_prices")

print("Computing high-value business KPIs for the Gold Layer...")
# Run aggregations grouped by each stock ticker symbol
gold_metrics_df = silver_df.groupBy("symbol").agg(
    round(avg("close"), 2).alias("average_closing_price"),
    max("high").alias("highest_all_time_peak"),
    sum("volume").alias("total_volume_traded")
)

# Define our Gold target landing zone
gold_output_path = "abfss://gold@hishamdelake01.dfs.core.windows.net/twelvedata/stock_summary"

print("Writing finalized metrics out to the Gold container...")
# Save the aggregated results as a highly optimized Delta asset
gold_metrics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_output_path)

print("Gold Layer processing complete! Showing final executive summary table:")
display(gold_metrics_df)
