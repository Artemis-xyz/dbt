-- Do I need to set up the snowflake_warehouse? Set db/schema/alias
-- Is alias the reference for importing in downstream ez tables?

{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE_MD",
        alias="fact_stargate_v1_linea_transactions"
    )
}}

-- TODO add input token
with
    pools as (
        select LOWER(address) AS address
        from (
            values
            ('0xAad094F6A75A14417d39f04E690fC216f080A41a')
        ) AS addresses(address)
    ),

    event_signatures as (
        select event_signature
        from (
            values
            ('Mint(address,uint256,uint256,uint256)'),
            ('Burn(address,uint256,uint256)'),
            ('Swap(uint16,uint256,address,uint256,uint256,uint256,uint256,uint256)'),
            ('SwapRemote(address,uint256,uint256,uint256)')
        ) AS signatures(event_signature)
    )

-- Join txns to get block time
select
    parquet_raw:address::string contract_address,
    parquet_raw:block_number block_number,
    NULL as block_timestamp, -- add by joining to txns/block table later
    parquet_raw:transaction_hash::string tx_hash,
    parquet_raw:log_index event_index,
    parquet_raw:event_signature::string event_signature,
    parquet_raw:event_params event_params
from LANDING_DATABASE.PROD_LANDING.RAW_LINEA_LOGS_PARQUET
where 1=1
    and parquet_raw:address::string in (select address from pools)
    and parquet_raw:event_signature::string in (select event_signature from event_signatures)