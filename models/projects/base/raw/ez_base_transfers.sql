{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="base",
        database="base",
        schema="raw",
        alias="ez_transfers",
    )
}}


with
token_transfers as (
    select 
        block_timestamp
        , block_number
        , tx_hash
        , lower(origin_from_address) as origin_from_address
        , lower(origin_to_address) as origin_to_address
        , lower(from_address) as from_address
        , lower(to_address) as to_address
        , 'native-token:8453' as contract_address
        , 'ETH' as symbol
        , 18 as decimals
        , amount_precise_raw as amount
        , amount_precise_raw / pow(10, 18) as amount_adjusted
        , amount_usd
        , tx_position as tx_index
        , trace_index as trace_index
        , null as event_index
        , CONCAT(
            LPAD(block_number::TEXT, 16, '0'), 
            '-', LPAD(tx_position::TEXT, 4, '0'), 
            '-', LPAD(trace_index::TEXT, 4, '0'),
            '-', LPAD(0::TEXT, 4, '0')
        ) AS index
    from base_flipside.core.ez_native_transfers 
    {% if is_incremental() %}
        where block_timestamp >= (
            select dateadd('day', -7, max(block_timestamp)) 
            from {{ this }}
            where contract_address = 'native-token:8453'
        )
    {% endif %}
    union all
    select 
        block_timestamp
        , block_number
        , tx_hash
        , lower(origin_from_address) as origin_from_address
        , lower(origin_to_address) as origin_to_address
        , lower(from_address) as from_address
        , lower(to_address) as to_address
        , t.contract_address
        , t.symbol
        , c.num_decimals as decimals
        , raw_amount_precise as amount
        , raw_amount_precise / pow(10, c.num_decimals) as amount_adjusted
        , amount_usd
        , position as tx_index
        , null as trace_index
        , event_index
        , CONCAT(
            LPAD(block_number::TEXT, 16, '0'), 
            '-', LPAD(tx_index::TEXT, 4, '0'), 
            '-', LPAD(0::TEXT, 4, '0'),
            '-', LPAD(event_index::TEXT, 4, '0')
        ) AS index
    from base_flipside.core.ez_token_transfers t
    inner join {{ref('fact_base_stablecoin_contracts')}} c using(contract_address)
    left join base_flipside.core.fact_transactions txn using(tx_hash)
    {% if is_incremental() %}
        where block_timestamp >= (
            select dateadd('day', -7, max(block_timestamp)) 
            from {{ this }}
            where contract_address <> 'native-token:8453'
        )
    {% endif %}
)

select 
    block_timestamp
    , block_number
    , tx_hash
    , origin_from_address
    , origin_to_address
    , from_address
    , to_address
    , contract_address
    , symbol
    , decimals
    , amount
    , amount_adjusted
    , amount_usd
    , tx_index
    , trace_index
    , event_index
    , index
from token_transfers
order by from_address, to_address, index