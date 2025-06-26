{{
    config(
        materialized="table",
        snowflake_warehouse="RESERVE",
    )
}}

with max_extraction as (
    select max(extraction_date) as max_date
    from {{ source("PROD_LANDING", "raw_reserve_rtoken_market_cap") }}
)
, latest_data as (
    select parse_json(source_json) as data
    from {{ source("PROD_LANDING", "raw_reserve_rtoken_market_cap") }}
    where extraction_date = (select max_date from max_extraction)
)
select
    left(f.value:last_update_time::string, 10)::date as date
    , f.value:rtokens_tvl_arbitrum::number as rtokens_tvl_arbitrum
    , f.value:rtokens_tvl_base::number as rtokens_tvl_base
    , f.value:rtokens_tvl_ethereum::number as rtokens_tvl_ethereum
    , rtokens_tvl_arbitrum + rtokens_tvl_base + rtokens_tvl_ethereum as rtokens_mc
from latest_data, lateral flatten(input => data) f
