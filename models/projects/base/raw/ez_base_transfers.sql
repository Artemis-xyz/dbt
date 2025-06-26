{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="ETHEREUM_LG",
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
        , 'eip:8453:native' as contract_address
        , 'ETH' as symbol
        , 18 as decimals
        , amount_precise_raw as amount
        , amount_precise_raw / pow(10, 18) as amount_adjusted
        , amount_usd
        , tx_position as tx_index
        , trace_index as trace_index
        , -1 as event_index
        , CONCAT(
            LPAD(block_number::TEXT, 16, '0'), 
            '-', LPAD(tx_position::TEXT, 8, '0'), 
            '-', CASE WHEN trace_index = -1 THEN 'FFFFFFFF' ELSE LPAD(trace_index::TEXT, 8, '0') END,
            '-', CASE WHEN event_index = -1 THEN 'FFFFFFFF' ELSE LPAD(event_index::TEXT, 8, '0') END
        ) AS index
    from base_flipside.core.ez_native_transfers 
    {% if is_incremental() %}
        where block_timestamp >= (
            select DATEADD('day', -3, max(block_timestamp)) 
            from {{ this }}
            where contract_address = 'eip:8453:native'
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
        , tx_position as tx_index
        , -1 as trace_index
        , event_index
        , CONCAT(
            LPAD(block_number::TEXT, 16, '0'), 
            '-', LPAD(tx_position::TEXT, 8, '0'), 
            '-', CASE WHEN trace_index = -1 THEN 'FFFFFFFF' ELSE LPAD(trace_index::TEXT, 8, '0') END,
            '-', CASE WHEN event_index = -1 THEN 'FFFFFFFF' ELSE LPAD(event_index::TEXT, 8, '0') END
        ) AS index
    from base_flipside.core.ez_token_transfers t
    inner join {{ref('fact_base_stablecoin_contracts')}} c on lower(t.contract_address) = lower(c.contract_address)
    left join base_flipside.core.fact_transactions txn using(tx_hash)
    {% if is_incremental() %}
        where block_timestamp >= (
            select DATEADD('day', -3, max(block_timestamp)) 
            from {{ this }}
            where contract_address <> 'eip:8453:native'
        )
    {% endif %}
)
, tags as (
    select distinct 
        address
        , artemis_application_id
        , artemis_category_id
    from {{ref('dim_all_addresses_labeled_gold')}}
    where chain = 'base'
)
, with_tags as (
    select 
        token_transfers.block_timestamp
        , block_number
        , tx_hash
        , origin_from_address
        , origin_to_address

        , from_address
        , from_labels.artemis_application_id as from_normalized_application_id
        , from_labels.artemis_category_id as from_normalized_category_id
        , case when from_address_metadata.contract_address is null then 1 else 0 end as from_is_wallet 

        , to_address
        , to_labels.artemis_application_id as to_normalized_application_id
        , to_labels.artemis_category_id as to_normalized_category_id
        , case when to_address_metadata.contract_address is null then 1 else 0 end as to_is_wallet

        , token_transfers.contract_address
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
    left join tags to_labels on lower(to_address) = lower(to_labels.address)
    left join tags from_labels on lower(from_address) = lower(from_labels.address)
    left join {{ ref('dim_base_contract_addresses')}} to_address_metadata 
        on lower(to_address) = lower(to_address_metadata.contract_address)
    left join {{ ref('dim_base_contract_addresses')}} from_address_metadata 
        on lower(from_address) = lower(from_address_metadata.contract_address)
)

select distinct
    block_timestamp
    , block_number
    , tx_hash
    , origin_from_address
    , origin_to_address

    , from_address
    , from_normalized_application_id
    , from_normalized_category_id
    , from_is_wallet

    , to_address
    , to_normalized_application_id
    , to_normalized_category_id
    , to_is_wallet

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
from with_tags
order by from_address, to_address, index