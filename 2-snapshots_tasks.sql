USE ROLE VHOL_BUILD_TLV_DE;
USE WAREHOUSE VW_VHOL_BUILD_TLV;
USE DATABASE VHOL_BUILD_TLV;
USE SCHEMA TRANSFORMED;
CREATE OR REPLACE TABLE YTD_SNAPSHOT (
    TICKER VARCHAR(16777216),
    START_OF_YEAR_DATE DATE,
    START_OF_YEAR_PRICE FLOAT,
    LATEST_DATE DATE,
    LATEST_PRICE FLOAT,
    PERCENTAGE_CHANGE_YTD FLOAT
);

CREATE OR REPLACE TABLE QTD_SNAPSHOT (
    TICKER VARCHAR(16777216),
    START_OF_QUARTER_DATE DATE,
    START_OF_QUARTER_PRICE FLOAT,
    LATEST_DATE DATE,
    LATEST_PRICE FLOAT,
    PERCENTAGE_CHANGE_QTD FLOAT
);

CREATE OR REPLACE TABLE MTD_SNAPSHOT (
    TICKER VARCHAR(16777216),
    START_OF_MONTH_DATE DATE,
    START_OF_MONTH_PRICE FLOAT,
    LATEST_DATE DATE,
    LATEST_PRICE FLOAT,
    PERCENTAGE_CHANGE_MTD FLOAT
);

CREATE OR REPLACE TASK SNAPSHOTS_START
WAREHOUSE=VW_VHOL_BUILD_TLV
SCHEDULE = 'USING CRON  0 * * * * America/New_York'
  AS
    SELECT CURRENT_TIMESTAMP();

CREATE OR REPLACE TASK YTD_SNAPSHOT_TASK
WAREHOUSE=VW_VHOL_BUILD_TLV
AFTER SNAPSHOTS_START
AS
MERGE INTO YTD_SNAPSHOT curr_data USING 
(WITH ytd_performance AS (
  SELECT
    ticker,
    MIN(date) OVER (PARTITION BY ticker) AS start_of_year_date,
    FIRST_VALUE(value) OVER (PARTITION BY ticker ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS start_of_year_price,
    MAX(date) OVER (PARTITION BY ticker) AS latest_date,
    LAST_VALUE(value) OVER (PARTITION BY ticker ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_price
  FROM VHOL_BUILD_TLV.RAW_LAKE.STOCK_PRICES_HIST
  WHERE
    ticker IN ('AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', 'SNOW')
    AND date >= DATE_TRUNC('YEAR', CURRENT_DATE()) -- Truncates the current date to the start of the year
    AND variable_name = 'Post-Market Close'
)
SELECT
  ticker,
  start_of_year_date,
  start_of_year_price,
  latest_date,
  latest_price,
  (latest_price - start_of_year_price) / start_of_year_price * 100 AS percentage_change_ytd
FROM
  ytd_performance
GROUP BY
  ticker, start_of_year_date, start_of_year_price, latest_date, latest_price)
AS new_data
ON curr_data.latest_date = new_data.latest_date and curr_data.ticker = new_data.ticker
WHEN MATCHED THEN 
        UPDATE SET 
        curr_data.start_of_year_date = new_data.start_of_year_date,
        curr_data.start_of_year_price = new_data.start_of_year_price,
        curr_data.latest_price = new_data.latest_price,
        curr_data.percentage_change_ytd = new_data.percentage_change_ytd
WHEN NOT MATCHED THEN INSERT (ticker,
  start_of_year_date,
  start_of_year_price,
  latest_date,
  latest_price,percentage_change_ytd) VALUES (new_data.ticker,
  new_data.start_of_year_date,
  new_data.start_of_year_price,
  new_data.latest_date,
  new_data.latest_price, new_data.percentage_change_ytd);

CREATE OR REPLACE TASK QTD_SNAPSHOT_TASK
WAREHOUSE=VW_VHOL_BUILD_TLV
AFTER SNAPSHOTS_START
AS
MERGE INTO QTD_SNAPSHOT curr_data USING 
(WITH qtd_performance AS (
  SELECT
    ticker,
    MIN(date) OVER (PARTITION BY ticker) AS start_of_quarter_date,
    FIRST_VALUE(value) OVER (PARTITION BY ticker ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS start_of_quarter_price,
    MAX(date) OVER (PARTITION BY ticker) AS latest_date,
    LAST_VALUE(value) OVER (PARTITION BY ticker ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_price
  FROM VHOL_BUILD_TLV.RAW_LAKE.STOCK_PRICES_HIST
  WHERE
    ticker IN ('AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', 'SNOW')
    AND date >= DATE_TRUNC('QUARTER', CURRENT_DATE()) -- Truncates the current date to the start of the quarter
    AND variable_name = 'Post-Market Close'
)
SELECT
  ticker,
  start_of_quarter_date,
  start_of_quarter_price,
  latest_date,
  latest_price,
  (latest_price - start_of_quarter_price) / start_of_quarter_price * 100 AS percentage_change_qtd
FROM
  QTD_performance
GROUP BY
  ticker, start_of_quarter_date, start_of_quarter_price, latest_date, latest_price)
AS new_data
ON curr_data.latest_date = new_data.latest_date and curr_data.ticker = new_data.ticker
WHEN MATCHED THEN 
        UPDATE SET 
        curr_data.start_of_quarter_date = new_data.start_of_quarter_date,
        curr_data.start_of_quarter_price = new_data.start_of_quarter_price,
        curr_data.latest_price = new_data.latest_price,
        curr_data.percentage_change_qtd = new_data.percentage_change_qtd
WHEN NOT MATCHED THEN INSERT (ticker,
  start_of_quarter_date,
  start_of_quarter_price,
  latest_date,
  latest_price,percentage_change_QTD) VALUES (new_data.ticker,
  new_data.start_of_quarter_date,
  new_data.start_of_quarter_price,
  new_data.latest_date,
  new_data.latest_price, new_data.percentage_change_qtd);

CREATE OR REPLACE TASK MTD_SNAPSHOT_TASK
WAREHOUSE=VW_VHOL_BUILD_TLV
AFTER SNAPSHOTS_START
AS
MERGE INTO MTD_SNAPSHOT curr_data USING 
(WITH MTD_performance AS (
  SELECT
    ticker,
    MIN(date) OVER (PARTITION BY ticker) AS start_of_month_date,
    FIRST_VALUE(value) OVER (PARTITION BY ticker ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS start_of_month_price,
    MAX(date) OVER (PARTITION BY ticker) AS latest_date,
    LAST_VALUE(value) OVER (PARTITION BY ticker ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_price
  FROM VHOL_BUILD_TLV.RAW_LAKE.STOCK_PRICES_HIST
  WHERE
    ticker IN ('AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', 'SNOW')
    AND date >= DATE_TRUNC('month', CURRENT_DATE()) -- Truncates the current date to the start of the month
    AND variable_name = 'Post-Market Close'
)
SELECT
  ticker,
  start_of_month_date,
  start_of_month_price,
  latest_date,
  latest_price,
  (latest_price - start_of_month_price) / start_of_month_price * 100 AS percentage_change_mtd
FROM
  MTD_performance
GROUP BY
  ticker, start_of_month_date, start_of_month_price, latest_date, latest_price)
AS new_data
ON curr_data.latest_date = new_data.latest_date and curr_data.ticker = new_data.ticker
WHEN MATCHED THEN 
        UPDATE SET 
        curr_data.start_of_month_date = new_data.start_of_month_date,
        curr_data.start_of_month_price = new_data.start_of_month_price,
        curr_data.latest_price = new_data.latest_price,
        curr_data.percentage_change_mtd = new_data.percentage_change_mtd
WHEN NOT MATCHED THEN INSERT (ticker,
  start_of_month_date,
  start_of_month_price,
  latest_date,
  latest_price,percentage_change_mtd) VALUES (new_data.ticker,
  new_data.start_of_month_date,
  new_data.start_of_month_price,
  new_data.latest_date,
  new_data.latest_price, new_data.percentage_change_mtd);

ALTER TASK YTD_SNAPSHOT_TASK RESUME;
ALTER TASK QTD_SNAPSHOT_TASK RESUME;
ALTER TASK MTD_SNAPSHOT_TASK RESUME;
  
EXECUTE TASK SNAPSHOTS_START;

SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('hour',-1,current_timestamp()),
    RESULT_LIMIT => 10));

CREATE OR REPLACE DYNAMIC TABLE STOCKS_TO_DATE
LAG = '1 minute'
WAREHOUSE = 'VW_VHOL_BUILD_TLV'
AS
SELECT TICKER, LATEST_DATE, PERCENTAGE_CHANGE_YTD, PERCENTAGE_CHANGE_QTD, PERCENTAGE_CHANGE_MTD
FROM YTD_SNAPSHOT JOIN MTD_SNAPSHOT USING (TICKER, LATEST_DATE)
JOIN QTD_SNAPSHOT USING (TICKER, LATEST_DATE);

SELECT * FROM STOCKS_TO_DATE;
