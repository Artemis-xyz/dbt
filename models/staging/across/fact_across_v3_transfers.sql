{{
    config(
        materialized="table",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}


with
    transfers_v3 as (
        select
            contract_address,
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"outputAmount"::double as amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"repaymentChainId"::integer as destination_chain_id,
            decoded_log:"originChainId"::integer as origin_chain_id,
            decoded_log:"outputToken"::string as destination_token,
            decoded_log:"inputAmount"::double as input_amount,  -- new column
            decoded_log:"inputToken"::string as input_token  -- new column
        from ethereum_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledV3Relay'
            and lower(contract_address)
            = lower('0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5')
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
        union

        select
            contract_address,
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"outputAmount"::double as amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"repaymentChainId"::integer as destination_chain_id,
            decoded_log:"originChainId"::integer as origin_chain_id,
            decoded_log:"outputToken"::string as destination_token,
            decoded_log:"inputAmount"::double as input_amount,  -- new column
            decoded_log:"inputToken"::string as input_token  -- new column
        from arbitrum_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledV3Relay'
            and lower(contract_address)
            = lower('0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A')
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
        union

        select
            contract_address,
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"outputAmount"::double as amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"repaymentChainId"::integer as destination_chain_id,
            decoded_log:"originChainId"::integer as origin_chain_id,
            decoded_log:"outputToken"::string as destination_token,
            decoded_log:"inputAmount"::double as input_amount,  -- new column
            decoded_log:"inputToken"::string as input_token  -- new column
        from optimism_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledV3Relay'
            and lower(contract_address)
            = lower('0x6f26Bf09B1C792e3228e5467807a900A503c0281')
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}

        union

        select
            contract_address,
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"outputAmount"::double as amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"repaymentChainId"::integer as destination_chain_id,
            decoded_log:"originChainId"::integer as origin_chain_id,
            decoded_log:"outputToken"::string as destination_token,
            decoded_log:"inputAmount"::double as input_amount,  -- new column
            decoded_log:"inputToken"::string as input_token  -- new column
        from base_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledV3Relay'
            and lower(contract_address)
            = lower('0x09aea4b2242abc8bb4bb78d537a67a245a7bec64')
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}

        union

        select
            contract_address,
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"outputAmount"::double as amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"repaymentChainId"::integer as destination_chain_id,
            decoded_log:"originChainId"::integer as origin_chain_id,
            decoded_log:"outputToken"::string as destination_token,
            decoded_log:"inputAmount"::double as input_amount,  -- new column
            decoded_log:"inputToken"::string as input_token  -- new column
        from polygon_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledV3Relay'
            and lower(contract_address)
            = lower('0x9295ee1d8C5b022Be115A2AD3c30C72E34e7F096')
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}

    ),
    zksync_extraction as (
        select
            date_trunc('day', flat_json.value:"block_timestamp"::timestamp) as date,
            max(extraction_date) as extraction_date
        from
            {{ source("PROD_LANDING", "raw_across_v3_zksync_events") }},
            lateral flatten(input => parse_json(source_json)) as flat_json
        group by date
        order by date
    ),

    flattened_json as (
        select
            extraction_date,
            date_trunc('day', flat_json.value:"block_timestamp"::timestamp) as date,
            to_timestamp(flat_json.value:"block_timestamp"::varchar) as block_timestamp,
            flat_json.value:"contract_address"::string as contract_address,
            flat_json.value:"tx_hash"::string as tx_hash,
            flat_json.value:"event_index"::integer as event_index,
            flat_json.value:"amount"::double as amount,
            flat_json.value:"depositor"::string as depositor,
            flat_json.value:"recipient"::string as recipient,
            flat_json.value:"destination_chain_id"::integer as destination_chain_id,
            flat_json.value:"destination_token"::string as destination_token,
            flat_json.value:"origin_chain_id"::integer as origin_chain_id,
            flat_json.value:"input_amount"::float as input_amount,
            flat_json.value:"input_token"::string as input_token,
            flat_json.value:"destination_token_symbol"::string
            as destination_token_symbol
        from
            {{ source("PROD_LANDING", "raw_across_v3_zksync_events") }},
            lateral flatten(input => parse_json(source_json)) as flat_json
    ),

    zksync_transfers as (
        select
            t1.contract_address,
            t1.block_timestamp,
            t1.tx_hash,
            t1.event_index,
            t1.amount,
            t1.depositor,
            t1.recipient,
            t1.destination_chain_id,
            t1.destination_token,
            t1.origin_chain_id,
            t1.input_amount,
            t1.input_token,
            t1.destination_token_symbol,
        from flattened_json t1
        left join
            zksync_extraction t2
            on t1.date = t2.date
            and t1.extraction_date = t2.extraction_date
    ),

    linea_extraction as (
        select
            date_trunc('day', flat_json.value:"block_timestamp"::timestamp) as date,
            max(extraction_date) as extraction_date
        from
            {{ source("PROD_LANDING", "raw_across_v3_linea_events") }},
            lateral flatten(input => parse_json(source_json)) as flat_json
        group by date
        order by date
    ),

    linea_flattened_json as (
        select
            extraction_date,
            date_trunc('day', flat_json.value:"block_timestamp"::timestamp) as date,
            to_timestamp(flat_json.value:"block_timestamp"::varchar) as block_timestamp,
            flat_json.value:"contract_address"::string as contract_address,
            flat_json.value:"tx_hash"::string as tx_hash,
            flat_json.value:"event_index"::integer as event_index,
            flat_json.value:"amount"::double as amount,
            flat_json.value:"depositor"::string as depositor,
            flat_json.value:"recipient"::string as recipient,
            flat_json.value:"destination_chain_id"::integer as destination_chain_id,
            flat_json.value:"destination_token"::string as destination_token,
            flat_json.value:"origin_chain_id"::integer as origin_chain_id,
            flat_json.value:"input_amount"::float as input_amount,
            flat_json.value:"input_token"::string as input_token,
            flat_json.value:"destination_token_symbol"::string
            as destination_token_symbol
        from
            {{ source("PROD_LANDING", "raw_across_v3_linea_events") }},
            lateral flatten(input => parse_json(source_json)) as flat_json
    ),

    linea_transfers as (
        select
            t1.contract_address,
            t1.block_timestamp,
            t1.tx_hash,
            t1.event_index,
            t1.amount,
            t1.depositor,
            t1.recipient,
            t1.destination_chain_id,
            t1.destination_token,
            t1.origin_chain_id,
            t1.input_amount,
            t1.input_token,
            t1.destination_token_symbol,
        from linea_flattened_json t1
        left join
            linea_extraction t2
            on t1.date = t2.date
            and t1.extraction_date = t2.extraction_date
    )






select
    contract_address,
    block_timestamp,
    tx_hash,
    event_index,
    amount,
    depositor,
    recipient,
    destination_chain_id,
    destination_token,
    origin_chain_id,
    input_amount,
    input_token,
    null as destination_token_symbol
from transfers_v3

union

select
    contract_address,
    block_timestamp,
    tx_hash,
    event_index,
    amount,
    depositor,
    recipient,
    destination_chain_id,
    destination_token,
    origin_chain_id,
    input_amount,
    input_token,
    destination_token_symbol
from zksync_transfers

union 

select
    contract_address,
    block_timestamp,
    tx_hash,
    event_index,
    amount,
    depositor,
    recipient,
    destination_chain_id,
    destination_token,
    origin_chain_id,
    input_amount,
    input_token,
    destination_token_symbol
from linea_transfers



