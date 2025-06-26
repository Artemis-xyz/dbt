{% macro across_v3_decode_funds_deposited(chain, spot_fee_contract) %}
    select
        contract_address as messaging_contract_address,
        block_timestamp,
        tx_hash,
        event_index,
        TRY_TO_NUMBER(decoded_log:"depositId"::string) as deposit_id,
        IFF(event_name = 'FundsDeposited', '0x' || substr(decoded_log:"inputToken"::string, 27, 40), decoded_log:"inputToken") as origin_token,
        TRY_TO_DOUBLE(decoded_log:"inputAmount"::string) as src_amount,
        TRY_TO_DOUBLE(decoded_log:"outputAmount"::string) as dst_amount,
        IFF(event_name = 'FundsDeposited', '0x' || substr(decoded_log:"depositor"::string, 27, 40), decoded_log:"depositor") as depositor,
        IFF(event_name = 'FundsDeposited', '0x' || substr(decoded_log:"recipient"::string, 27, 40), decoded_log:"recipient") as recipient,
        TRY_TO_NUMBER(decoded_log:"destinationChainId"::string) as destination_chain_id,
        IFF(event_name = 'FundsDeposited', '0x' || substr(decoded_log:"outputToken"::string, 27, 40), decoded_log:"outputToken")  destination_token,
        TRY_TO_NUMBER(decoded_log:"originChainId"::string) as origin_chain_id,
        null as realized_lp_fee_pct,
        null as relayer_fee_pct,
        coalesce(src_amount, 0) - coalesce(dst_amount,0) as protocol_fee,
        decoded_log:"message"::string as message,
        '{{ chain }}' as chain,
        decoded_log
    from {{ chain }}_flipside.core.ez_decoded_event_logs
    where
        ((event_name = 'V3FundsDeposited') or (event_name = 'FundsDeposited' and block_timestamp >= '2025-02-07'))
        and
        lower(contract_address) = lower('{{ spot_fee_contract }}')
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}


{% macro across_v3_goldsky_decode_funds_deposited(chain, spot_fee_contract) %}
    select
        contract_address as messaging_contract_address,
        block_timestamp,
        transaction_hash as tx_hash,
        event_index,
        decoded_log:"depositId"::integer as deposit_id,
        IFF(event_name = 'FundsDeposited', '0x' || substr(decoded_log:"inputToken"::string, 25, 40), decoded_log:"inputToken") as origin_token,
        decoded_log:"inputAmount"::double as src_amount,
        decoded_log:"outputAmount"::double as dst_amount,
        IFF(event_name = 'FundsDeposited', '0x' || substr(decoded_log:"depositor"::string, 25, 40), decoded_log:"depositor") as depositor,
        IFF(event_name = 'FundsDeposited', '0x' || substr(decoded_log:"recipient"::string, 25, 40), decoded_log:"recipient") as recipient,
        decoded_log:"destinationChainId"::integer as destination_chain_id,
        IFF(event_name = 'FundsDeposited', '0x' || substr(decoded_log:"outputToken"::string, 25, 40), decoded_log:"outputToken")  destination_token,
        decoded_log:"originChainId"::integer as origin_chain_id,
        null as realized_lp_fee_pct,
        null as relayer_fee_pct,
        src_amount - dst_amount as protocol_fee,
        decoded_log:"message"::string as message,
        '{{ chain }}' as chain,
        decoded_log
    from {{ ref("fact_" ~ chain  ~ "_decoded_events") }}
    where
        ((event_name = 'V3FundsDeposited') or (event_name = 'FundsDeposited' and block_timestamp >= '2025-02-07'))
        and
        lower(contract_address) = lower('{{ spot_fee_contract }}')
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}

{% macro across_v3_rpc_decode_funds_deposited(chain) %}       
    with extraction_dates as (
        select
            date_trunc('day', flat_json.value:"block_timestamp"::timestamp) as date,
            max(extraction_date) as extraction_date
        from
            {{ source("PROD_LANDING", "raw_across_v3_" ~ chain ~ "_funds_deposited_events") }},
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
            flat_json.value:"input_amount"::double as src_amount,
            flat_json.value:"output_amount"::double as dst_amount,
            flat_json.value:"deposit_id"::integer as deposit_id,
            flat_json.value:"depositor"::string as depositor,
            flat_json.value:"recipient"::string as recipient,
            flat_json.value:"destination_chain_id"::string as destination_chain_id,
            flat_json.value:"quote_timestamp"::string as quote_timestamp,
            flat_json.value:"fill_deadline"::string as fill_deadline,
            flat_json.value:"exclusivity_deadline"::string as exclusivity_deadline,
            flat_json.value:"exclusive_relayer"::string as exclusive_relayer,
            flat_json.value:"destination_token"::string as destination_token,
            flat_json.value:"origin_chain"::string as origin_chain,
            flat_json.value:"input_token"::string as origin_token,
            null as decoded_log
        from
            {{ source("PROD_LANDING", "raw_across_v3_" ~ chain ~ "_funds_deposited_events") }},
            lateral flatten(input => to_variant(source_json), outer => true) as flat_json
        {% if is_incremental() %}
            where
                date_trunc('day', flat_json.value:"block_timestamp"::timestamp) >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    )
    select
        t1.contract_address as messaging_contract_address
        , t1.block_timestamp
        , t1.tx_hash
        , t1.event_index
        , t1.deposit_id
        , t1.origin_token
        , t1.src_amount
        , t1.dst_amount
        , t1.depositor
        , t1.recipient
        , t1.destination_chain_id
        , t1.destination_token
        , null as origin_chain_id
        , null as realized_lp_fee_pct
        , null as relayer_fee_pct
        , src_amount - dst_amount as protocol_fee
        , null as message
        , '{{ chain}}' as chain
        , t1.decoded_log
    from flattened_json t1
    left join
        extraction_dates t2
        on t1.date = t2.date
        and t1.extraction_date = t2.extraction_date
{% endmacro %}

{% macro across_v3_rpc_decode_funds_deposited_array(chain) %}       
        with
        outer_flatten as (
            select
                extraction_date,
                index,
                key,
                value as json
            from
                {{ source("PROD_LANDING", "raw_across_v3_" ~ chain ~ "_funds_deposited_events") }},
                lateral flatten(input => to_variant(source_json), outer => true) as flat_json
            WHERE ARRAY_SIZE(source_json) > 1
        ),
        un_flatten as (
            select
                *
            from
                {{ source("PROD_LANDING", "raw_across_v3_" ~ chain ~ "_funds_deposited_events") }},
                lateral flatten(input => to_variant(source_json), outer => true) as flat_json
            WHERE ARRAY_SIZE(source_json) = 1
        ),
        flattened_json as (
            select
                    extraction_date,
                    date_trunc('day', flat_json.value:"block_timestamp"::timestamp) as date,
                    to_timestamp(flat_json.value:"block_timestamp"::varchar) as block_timestamp,
                    flat_json.value:"contract_address"::string as contract_address,
                    flat_json.value:"tx_hash"::string as tx_hash,
                    flat_json.value:"event_index"::integer as event_index,
                    flat_json.value:"input_amount"::double as src_amount,
                    flat_json.value:"output_amount"::double as dst_amount,
                    flat_json.value:"deposit_id"::integer as deposit_id,
                    flat_json.value:"depositor"::string as depositor,
                    flat_json.value:"recipient"::string as recipient,
                    flat_json.value:"destination_chain_id"::string as destination_chain_id,
                    flat_json.value:"quote_timestamp"::string as quote_timestamp,
                    flat_json.value:"fill_deadline"::string as fill_deadline,
                    flat_json.value:"exclusivity_deadline"::string as exclusivity_deadline,
                    flat_json.value:"exclusive_relayer"::string as exclusive_relayer,
                    flat_json.value:"destination_token"::string as destination_token,
                    flat_json.value:"origin_chain"::string as origin_chain,
                    flat_json.value:"input_token"::string as origin_token,
                    null as decoded_log
                from
                    outer_flatten,
                    lateral flatten(input => to_variant(json), outer => true) as flat_json
            union 
                SELECT 
                    extraction_date,
                    date_trunc('day', flat_json.value:"block_timestamp"::timestamp) as date,
                    to_timestamp(flat_json.value:"block_timestamp"::varchar) as block_timestamp,
                    flat_json.value:"contract_address"::string as contract_address,
                    flat_json.value:"tx_hash"::string as tx_hash,
                    flat_json.value:"event_index"::integer as event_index,
                    flat_json.value:"input_amount"::double as src_amount,
                    flat_json.value:"output_amount"::double as dst_amount,
                    flat_json.value:"deposit_id"::integer as deposit_id,
                    flat_json.value:"depositor"::string as depositor,
                    flat_json.value:"recipient"::string as recipient,
                    flat_json.value:"destination_chain_id"::string as destination_chain_id,
                    flat_json.value:"quote_timestamp"::string as quote_timestamp,
                    flat_json.value:"fill_deadline"::string as fill_deadline,
                    flat_json.value:"exclusivity_deadline"::string as exclusivity_deadline,
                    flat_json.value:"exclusive_relayer"::string as exclusive_relayer,
                    flat_json.value:"destination_token"::string as destination_token,
                    flat_json.value:"origin_chain"::string as origin_chain,
                    flat_json.value:"input_token"::string as origin_token,
                    null as decoded_log
                from
                    un_flatten,
                    lateral flatten(input => to_variant(source_json), outer => true) as flat_json
        ),
        extraction_dates as (
            select
                date,
                max(extraction_date) as extraction_date
            from
                flattened_json
            group by date
            order by date
        )
    select
        t1.contract_address as messaging_contract_address
        , t1.block_timestamp
        , t1.tx_hash
        , t1.event_index
        , t1.deposit_id
        , t1.origin_token
        , t1.src_amount
        , t1.dst_amount
        , t1.depositor
        , t1.recipient
        , t1.destination_chain_id
        , t1.destination_token
        , null as origin_chain_id
        , null as realized_lp_fee_pct
        , null as relayer_fee_pct
        , src_amount - dst_amount as protocol_fee
        , null as message
        , '{{ chain}}' as chain
        , t1.decoded_log
    from flattened_json t1
    left join
        extraction_dates t2
        on t1.date = t2.date
        and t1.extraction_date = t2.extraction_date
{% endmacro %}
