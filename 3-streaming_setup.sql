-- Laptop prpes!
-- 1. Download from URL https://sfquickstarts.s3.us-west-1.amazonaws.com/data_engineering_CDC_snowpipestreaming_dynamictables/CDCSimulatorApp.zip
-- and extract zip file.
-- 2. Enter the extracted directory and run the command to generate SSL keys
-- openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
-- openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
-- You'll need the content of rsa_key.pub to assign in Snowflake
-- 3. Edit the snowflake.properties file and set
-- user=VHOL_BUILD_TLV_STREAMING_USER
-- role=VHOL_BUILD_TLV_CDC_AGENT
-- account=<Copy from Snowsight, change dot to dash>
-- host=<Copy from Snowsight, change dot to dash>.snowflakecomputing.com
-- database=VHOL_BUILD_TLV
-- schema=RAW_STREAM

-- Run this code now
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE USER VHOL_BUILD_TLV_STREAMING_USER COMMENT="User for the streaming SDK/Kafka";
ALTER USER VHOL_BUILD_TLV_STREAMING_USER SET rsa_public_key='<Paste Your Public Key Here>';

CREATE ROLE IF NOT EXISTS VHOL_BUILD_TLV_CDC_AGENT;
GRANT USAGE ON DATABASE VHOL_BUILD_TLV TO ROLE VHOL_BUILD_TLV_CDC_AGENT;
GRANT USAGE ON SCHEMA VHOL_BUILD_TLV.RAW_STREAM TO ROLE VHOL_BUILD_TLV_CDC_AGENT;
GRANT ROLE VHOL_BUILD_TLV_CDC_AGENT TO USER VHOL_BUILD_TLV_STREAMING_USER;

-- Streaming prep
USE ROLE VHOL_BUILD_TLV_DE;
USE WAREHOUSE VW_VHOL_BUILD_TLV;
USE DATABASE VHOL_BUILD_TLV;
USE SCHEMA RAW_STREAM;
CREATE OR REPLACE TABLE CDC_STREAMING_TABLE (RECORD_CONTENT variant);
GRANT INSERT ON TABLE CDC_STREAMING_TABLE to role VHOL_BUILD_TLV_CDC_AGENT;
SELECT * FROM CDC_STREAMING_TABLE;

-- Now you should be able to pass the ./Test.sh script
-- Now run the ./Run_MAX.sh
SELECT * FROM CDC_STREAMING_TABLE;
