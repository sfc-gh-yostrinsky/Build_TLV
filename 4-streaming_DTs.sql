USE ROLE VHOL_BUILD_TLV_DE;
USE WAREHOUSE VW_VHOL_BUILD_TLV;
USE DATABASE VHOL_BUILD_TLV;
USE SCHEMA TRANSFORMED;
CREATE OR REPLACE DYNAMIC TABLE LIMIT_ORDERS_CURRENT_DT
LAG = 'DOWNSTREAM'
WAREHOUSE = 'VW_VHOL_BUILD_TLV'
AS
SELECT * EXCLUDE (score,action) from (
  SELECT
    RECORD_CONTENT:transaction:primaryKey_tokenized::varchar as orderid_tokenized,
    RECORD_CONTENT:transaction:record_after:orderid_encrypted::varchar as orderid_encrypted,
    TO_TIMESTAMP_NTZ(RECORD_CONTENT:transaction:committed_at::number/1000) as lastUpdated,
    RECORD_CONTENT:transaction:action::varchar as action,
    RECORD_CONTENT:transaction:record_after:client::varchar as client,
    RECORD_CONTENT:transaction:record_after:ticker::varchar as ticker,
    RECORD_CONTENT:transaction:record_after:LongOrShort::varchar as position,
    RECORD_CONTENT:transaction:record_after:Price::number(38,3) as price,
    RECORD_CONTENT:transaction:record_after:Quantity::number(38,3) as quantity,
    RANK() OVER (
        partition by orderid_tokenized order by RECORD_CONTENT:transaction:committed_at::number desc) as score
  FROM RAW_STREAM.CDC_STREAMING_TABLE
    WHERE
        RECORD_CONTENT:transaction:schema::varchar='PROD' AND RECORD_CONTENT:transaction:table::varchar='LIMIT_ORDERS'
)
WHERE score = 1 and action != 'DELETE';

CREATE OR REPLACE DYNAMIC TABLE LIMIT_ORDERS_SCD_DT
LAG = 'DOWNSTREAM'
WAREHOUSE = 'VW_VHOL_BUILD_TLV'
AS
SELECT * EXCLUDE score from ( SELECT *,
  CASE when score=1 then true else false end as Is_Latest,
  LAST_VALUE(score) OVER (
            partition by orderid_tokenized order by valid_from desc)+1-score as version
  FROM (
      SELECT
        RECORD_CONTENT:transaction:primaryKey_tokenized::varchar as orderid_tokenized,
        RECORD_CONTENT:transaction:action::varchar as action,
        IFNULL(RECORD_CONTENT:transaction:record_after:client::varchar,RECORD_CONTENT:transaction:record_before:client::varchar) as client,
        IFNULL(RECORD_CONTENT:transaction:record_after:ticker::varchar,RECORD_CONTENT:transaction:record_before:ticker::varchar) as ticker,
        IFNULL(RECORD_CONTENT:transaction:record_after:LongOrShort::varchar,RECORD_CONTENT:transaction:record_before:LongOrShort::varchar) as position,
        RECORD_CONTENT:transaction:record_after:Price::number(38,3) as price,
        RECORD_CONTENT:transaction:record_after:Quantity::number(38,3) as quantity,
        RANK() OVER (
            partition by orderid_tokenized order by RECORD_CONTENT:transaction:committed_at::number desc) as score,
        TO_TIMESTAMP_NTZ(RECORD_CONTENT:transaction:committed_at::number/1000) as valid_from,
        TO_TIMESTAMP_NTZ(LAG(RECORD_CONTENT:transaction:committed_at::number/1000,1,null) over
                         (partition by orderid_tokenized order by RECORD_CONTENT:transaction:committed_at::number desc)) as valid_to
      FROM RAW_STREAM.CDC_STREAMING_TABLE
      WHERE
            RECORD_CONTENT:transaction:schema::varchar='PROD' AND RECORD_CONTENT:transaction:table::varchar='LIMIT_ORDERS'
    ));

CREATE OR REPLACE DYNAMIC TABLE LIMIT_ORDERS_SUMMARY_DT
LAG = 'DOWNSTREAM'
WAREHOUSE = 'VW_VHOL_BUILD_TLV'
AS
SELECT ticker,position,min(price) as MIN_PRICE,max(price) as MAX_PRICE, TO_DECIMAL(avg(price),38,2) as AVERAGE_PRICE,
    SUM(quantity) as TOTAL_SHARES,TO_DECIMAL(TOTAL_SHARES*AVERAGE_PRICE,38,2) as TOTAL_VALUE_USD
from (
  SELECT
    RECORD_CONTENT:transaction:action::varchar as action,
    RECORD_CONTENT:transaction:record_after:ticker::varchar as ticker,
    RECORD_CONTENT:transaction:record_after:LongOrShort::varchar as position,
    RECORD_CONTENT:transaction:record_after:Price::number(38,3) as price,
    RECORD_CONTENT:transaction:record_after:Quantity::number(38,3) as quantity
  FROM RAW_STREAM.CDC_STREAMING_TABLE
  WHERE
        RECORD_CONTENT:transaction:schema::varchar='PROD' AND RECORD_CONTENT:transaction:table::varchar='LIMIT_ORDERS'
  QUALIFY RANK() OVER (
        partition by RECORD_CONTENT:transaction:primaryKey_tokenized::varchar order by RECORD_CONTENT:transaction:committed_at::number desc) = 1
)
WHERE action != 'DELETE' group by ticker,position order by position,TOTAL_VALUE_USD DESC;
