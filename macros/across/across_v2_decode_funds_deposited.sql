{% macro across_v2_decode_funds_deposited(chain, spot_fee_contract) %}
    select
        decoded_log:"depositId"::integer as deposit_id,
        contract_address as messaging_contract_address,
        block_timestamp,
        tx_hash,
        event_index,
        decoded_log:"amount"::double as amount,
        decoded_log:"depositor"::string as depositor,
        decoded_log:"recipient"::string as recipient,
        decoded_log:"destinationChainId"::integer as destination_chain_id,
        decoded_log:"originChainId"::integer as origin_chain_id,
        decoded_log:"originToken"::string as origin_token,
        decoded_log:"relayerFeePct"::integer / 1e18 as relayer_fee_pct,
        '{{ chain }}' as chain
    from {{ chain }}_flipside.core.ez_decoded_event_logs
    where block_timestamp < '2025-02-07' and event_name = 'FundsDeposited'
        and
        contract_address = '{{ spot_fee_contract }}'
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
{% endmacro %}

{% macro across_v2_rpc_decode_funds_deposited(chain) %}       
    WITH extraction_dates as (
        select
            date_trunc('day', flat_json.value:"block_timestamp"::timestamp) as date,
            max(extraction_date) as extraction_date
        from
            {{ source("PROD_LANDING", "raw_across_v2_" ~ chain ~ "_funds_deposited_events") }},
            lateral flatten(input => parse_json(source_json)) as flat_json
        {% if is_incremental() %}
            where
                date_trunc('day', flat_json.value:"block_timestamp"::timestamp) >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
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
            flat_json.value:"deposit_id"::integer as deposit_id,
            flat_json.value:"depositor"::string as depositor,
            flat_json.value:"recipient"::string as recipient,
            flat_json.value:"destination_chain_id"::integer as destination_chain_id,
            flat_json.value:"origin_token"::string as origin_token,
            flat_json.value:"origin_chain_id"::integer as origin_chain_id,
            flat_json.value:"relayer_fee_pct"::float as relayer_fee_pct,
            to_timestamp(flat_json.value:"quote_timestamp"::varchar) as  quote_timestamp
        from
            {{ source("PROD_LANDING", "raw_across_v2_" ~ chain ~ "_funds_deposited_events") }},
            lateral flatten(input => parse_json(source_json)) as flat_json
        WHERE flat_json.value:"block_timestamp"::timestamp < '2025-02-07' 
        {% if is_incremental() %}
            and
                date_trunc('day', flat_json.value:"block_timestamp"::timestamp) >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    )
    select
        t1.deposit_id
        , t1.contract_address as messaging_contract_address
        , t1.block_timestamp
        , t1.tx_hash
        , t1.event_index
        , t1.amount
        , t1.depositor
        , t1.recipient
        , t1.destination_chain_id
        , t1.origin_chain_id
        , t1.origin_token
        , t1.relayer_fee_pct / 1e18 as relayer_fee_pct
        , '{{ chain }}' as chain
    from flattened_json t1
    left join
        extraction_dates t2
        on t1.date = t2.date
        and t1.extraction_date = t2.extraction_date
{% endmacro %}
