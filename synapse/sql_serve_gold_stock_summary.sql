-- ============================================================
-- Synapse Serverless SQL: Serve Gold Delta Layer
-- Workspace: synapse-hisham-de
-- Storage:   hishamdelake01 / gold container
-- ============================================================

-- Step 1: Create database (run once)
CREATE DATABASE gold_db;

-- Step 2: Create Delta file format (run once)
USE gold_db;

CREATE EXTERNAL FILE FORMAT DeltaFormat
WITH (FORMAT_TYPE = DELTA);

-- Step 3: Create external data source pointing at gold container (run once)
CREATE EXTERNAL DATA SOURCE ExternalDataSourceADLS
WITH (
    LOCATION = 'abfss://gold@hishamdelake01.dfs.core.windows.net'
);

-- Step 4: Create external table over gold Delta table (run once)
CREATE EXTERNAL TABLE stock_summary (
    symbol               VARCHAR(10),
    average_closing_price FLOAT,
    highest_all_time_peak FLOAT,
    total_volume_traded  BIGINT
)
WITH (
    LOCATION    = 'twelvedata/stock_summary/',
    DATA_SOURCE = ExternalDataSourceADLS,
    FILE_FORMAT = DeltaFormat
);

-- Step 5: Query (run anytime)
SELECT * FROM stock_summary;
