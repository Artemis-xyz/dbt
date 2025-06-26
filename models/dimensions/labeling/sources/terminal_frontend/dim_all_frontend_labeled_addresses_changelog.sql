{{ config(materialized="table", snowflake_warehouse="LABELING") }}

select
    parquet_raw:ACTION::string as action,
    parquet_raw:ADDRESS::string as address,
    parquet_raw:ARTEMIS_APPLICATION_ID::string as artemis_application_id,
    parquet_raw:CHAIN::string as chain,
    parquet_raw:LAST_UPDATED_BY::string as last_updated_by,
    parquet_raw:LAST_UPDATED_TIMESTAMP::timestamp as last_updated,
    parquet_raw:NAME::string as name
from {{ source("PROD_LANDING", "raw_manually_labeled_addresses_parquet") }}
where parquet_raw:LAST_UPDATED_TIMESTAMP::timestamp > '2025-04-01'
order by chain, address, last_updated desc