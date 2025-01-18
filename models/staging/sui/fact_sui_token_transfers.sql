{{ config(
    materialized="table",
    unique_key="unique_id",
    snowflake_warehouse="SUI",
) }}

with
    raw_transfers as (
        select
            parquet_raw:transaction_hash::string as tx_hash  
            , parquet_raw:timestamp_ms::timestamp as block_timestamp
            , parquet_raw:date::date as date
            , parquet_raw:checkpoint::bigint as checkpoint
            , parquet_raw:epoch::bigint as epoch
            , parquet_raw:coin_type::string as coin_type
            , parquet_raw:event_type::string as event_type
            , parquet_raw:from_address::string as from_address
            , parquet_raw:to_address::string as to_address
            , parquet_raw:amount::bigint as amount   
            , md5(tx_hash || coalesce(from_address, '') || coalesce(to_address, '') || amount) as unique_id
        from {{ source("PROD_LANDING", "raw_sui_transfers_parquet") }}
    )
select *
from raw_transfers
qualify row_number() over (partition by unique_id order by block_timestamp desc) = 1