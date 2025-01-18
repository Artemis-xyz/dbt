{{ 
    config(
        materialized="table",
        unique_key="unique_id",
        snowflake_warehouse="SUI",
    ) 
}}

with
    raw_balances as (
        select
            parquet_raw:date::date as date
            , parquet_raw:timestamp_ms::timestamp as block_timestamp
            , parquet_raw:epoch::bigint as epoch
            , parquet_raw:checkpoint::bigint as checkpoint
            , parquet_raw:coin_type::string as contract_address
            , parquet_raw:owner_address::string as address
            , parquet_raw:owner_type::string as owner_type
            , parquet_raw:balance::bigint as balance_token
            , md5(block_timestamp || coalesce(address, '') || contract_address) as unique_id
        from {{ source("PROD_LANDING", "raw_sui_balances_parquet") }}
        {% if is_incremental() %} 
            where block_timestamp >= (
                select dateadd('day', -3, max(parquet_raw:timestamp_ms::timestamp))
                from {{ this }}
            )
        {% endif %}
    )
select address, contract_address, block_timestamp, balance_token
from raw_balances
where address != 'TOTAL_SUPPLY'
qualify row_number() over (partition by unique_id order by block_timestamp desc) = 1