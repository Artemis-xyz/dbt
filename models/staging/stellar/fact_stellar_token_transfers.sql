{{ config(
    materialized="incremental",
    unique_key="unique_id",
    snowflake_warehouse="STELLAR",
) }}

with 
    prices as ({{get_multiple_coingecko_price_with_latest("stellar")}})
    , contract_addresses as (
        select 
            distinct 
            contract_address,
            symbol,
            decimals
        from prices
    ), token_transfers as (
        select
            TO_TIMESTAMP_NTZ(parquet_raw:closed_at::bigint / 1000000) AS block_timestamp
            , parquet_raw:ledger_sequence::bigint as block_number
            , parquet_raw:transaction_hash::string as transaction_hash
            , parquet_raw:transaction_id::string as transaction_index
            , parquet_raw:operation_id::string as event_index
            , concat(parquet_raw:asset_code::string, '-', parquet_raw:asset_issuer::string) as contract_address
            , parquet_raw:event_topic::string as event_type
            , parquet_raw:from_address::string as from_address
            , parquet_raw:to_address::string as to_address
            , parquet_raw:amount_raw::bigint as amount_raw
            , parquet_raw:unique_key::string as unique_id
        from {{ source("PROD_LANDING", "raw_stellar_fact_stellar_token_transfers_bigquery_parquet") }}
        {% if is_incremental() %}
            where block_timestamp >= (
                select dateadd('day', -3, max(block_timestamp))
                from {{ this }}
            )
        {% endif %}
    )
select
    block_timestamp
    , block_number
    , transaction_hash
    , transaction_index
    , event_index
    , event_type
    , token_transfers.contract_address
    , from_address
    , to_address
    , amount_raw
    , amount_raw / pow(10, contract_addresses.decimals) as amount_native
    , amount_native * prices.price as amount
    , prices.price as price
    , unique_id
from token_transfers
left join prices
    on token_transfers.block_timestamp::date = prices.date
    and lower(token_transfers.contract_address) = lower(prices.contract_address)
left join contract_addresses 
    on lower(token_transfers.contract_address) = lower(contract_addresses.contract_address)
where amount_raw > 0
qualify row_number() over (partition by unique_id order by block_timestamp desc) = 1