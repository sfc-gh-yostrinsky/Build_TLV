USE ROLE VHOL_BUILD_TLV_DE;
DROP DATABASE VHOL_BUILD_TLV;
DROP WAREHOUSE VW_VHOL_BUILD_TLV;

USE ROLE ACCOUNTADMIN;

DROP USER VHOL_BUILD_TLV_STREAMING_USER;
DROP ROLE VHOL_BUILD_TLV_CDC_AGENT;
DROP ROLE VHOL_BUILD_TLV_DE;
DROP EXTERNAL VOLUME EXT_VOL_BUILD_TLV;
DROP CATALOG INTEGRATION CAT_INTEGRATION_OBJ_STORE;