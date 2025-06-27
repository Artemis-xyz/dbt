{{ config(materialized="table", snowflake_warehouse="LABELING") }}

select
   parquet_raw:ARTEMIS_APPLICATION_ID::string as artemis_application_id
   , parquet_raw:ARTEMIS_CATEGORY_ID::string as artemis_category_id
   , parquet_raw:ARTEMIS_SUB_CATEGORY_ID::string as artemis_sub_category_id
   , parquet_raw:ARTEMIS_ID::string as artemis_id
   , parquet_raw:COINGECKO_ID::string as coingecko_id
   , parquet_raw:ECOSYSTEM_ID::string as ecosystem_id
   , parquet_raw:DEFILLAMA_PROTOCOL_ID::string as defillama_protocol_id
   , parquet_raw:VISIBILITY::string as visibility
   , parquet_raw:SYMBOL::string as symbol
   , parquet_raw:ICON::string as icon
   , parquet_raw:APP_NAME::string as app_name
   , parquet_raw:DESCRIPTION::string as description
   , parquet_raw:WEBSITE_URL::string as website_url
   , parquet_raw:GITHUB_URL::string as github_url
   , parquet_raw:X_HANDLE::string as x_handle
   , parquet_raw:DISCORD_HANDLE::string as discord_handle
   , parquet_raw:DEVELOPER_NAME::string as developer_name
   , parquet_raw:DEVELOPER_EMAIL::string as developer_email
   , parquet_raw:DEVELOPER_X_HANDLE::string as developer_x_handle
   , parquet_raw:LAST_UPDATED_BY::string as last_updated_by
   , parquet_raw:LAST_UPDATED_TIMESTAMP::timestamp as last_updated_timestamp
from {{ source("PROD_LANDING", "raw_manually_labeled_applications_parquet") }}
where parquet_raw:LAST_UPDATED_TIMESTAMP is not null and parquet_raw:LAST_UPDATED_TIMESTAMP::timestamp > '2025-03-23'
    -- Exclude this artemis application id, which is a test application
    and parquet_raw:ARTEMIS_APPLICATION_ID::string <> 'test123'
order by artemis_application_id, last_updated_timestamp desc