{{
    config(
        materialized="table",
        unique_key=["tx_hash", "event_index"],
    )
}}
with
    relays_v2 as (
        select
            contract_address,
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"fillAmount"::double as amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"destinationChainId"::integer as destination_chain_id,
            decoded_log:"destinationToken"::string as destination_token,
            decoded_log:"originChainId"::integer as origin_chain_id,
            decoded_log:"realizedLpFeePct"::integer / 1e18 as realized_lp_fee_pct,
            decoded_log:"relayerFeePct"::integer / 1e18 as relayer_fee_pct
        from ethereum_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledRelay'
            and contract_address = '0x4d9079bb4165aeb4084c526a32695dcfd2f77381'
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
            decoded_log:"fillAmount"::double as amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"destinationChainId"::integer as destination_chain_id,
            case
                when
                    decoded_log:"destinationToken"::string
                    = '0xd693ec944a85eeca4247ec1c3b130dca9b0c3b22'
                then '0x04fa0d235c4abf4bcf4787af4cf447de572ef828'
                else decoded_log:"destinationToken"::string
            end as destination_token,
            decoded_log:"originChainId"::integer as origin_chain_id,
            decoded_log:"realizedLpFeePct"::integer / 1e18 as realized_lp_fee_pct,
            decoded_log:"relayerFeePct"::integer / 1e18 as relayer_fee_pct
        from arbitrum_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledRelay'
            and contract_address = '0xb88690461ddbab6f04dfad7df66b7725942feb9c'
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
            decoded_log:"fillAmount"::double as amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"destinationChainId"::integer as destination_chain_id,
            case
                when
                    decoded_log:"destinationToken"::string
                    = '0xe7798f023fc62146e8aa1b36da45fb70855a77ea'
                then '0x04fa0d235c4abf4bcf4787af4cf447de572ef828'
                else decoded_log:"destinationToken"::string
            end as destination_token,
            decoded_log:"originChainId"::integer as origin_chain_id,
            decoded_log:"realizedLpFeePct"::integer / 1e18 as realized_lp_fee_pct,
            decoded_log:"relayerFeePct"::integer / 1e18 as relayer_fee_pct
        from optimism_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledRelay'
            and contract_address = '0x59485d57eecc4058f7831f46ee83a7078276b4ae'
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
            decoded_log:"fillAmount"::double as amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"destinationChainId"::integer as destination_chain_id,
            case
                when
                    decoded_log:"destinationToken"::string
                    = '0x3066818837c5e6ed6601bd5a91b0762877a6b731'
                then '0x04fa0d235c4abf4bcf4787af4cf447de572ef828'
                else decoded_log:"destinationToken"::string
            end as destination_token,
            decoded_log:"originChainId"::integer as origin_chain_id,
            decoded_log:"realizedLpFeePct"::integer / 1e18 as realized_lp_fee_pct,
            decoded_log:"relayerFeePct"::integer / 1e18 as relayer_fee_pct
        from polygon_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledRelay'
            and contract_address = '0x69b5c72837769ef1e7c164abc6515dcff217f920'
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
            decoded_log:"fillAmount"::double as amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"destinationChainId"::integer as destination_chain_id,
            decoded_log:"destinationToken"::string as destination_token,
            decoded_log:"originChainId"::integer as origin_chain_id,
            decoded_log:"realizedLpFeePct"::integer / 1e18 as realized_lp_fee_pct,
            decoded_log:"relayerFeePct"::integer / 1e18 as relayer_fee_pct
        from base_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledRelay'
            and contract_address = '0x09aea4b2242abc8bb4bb78d537a67a245a7bec64'
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
            {{ source("PROD_LANDING", "raw_across_v2_zksync_events") }},
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
            flat_json.value:"realized_lp_fee_pct"::float as realized_lp_fee_pct,
            flat_json.value:"relayer_fee_pct"::float as relayer_fee_pct,
            flat_json.value:"destination_token_symbol"::string
            as destination_token_symbol
        from
            {{ source("PROD_LANDING", "raw_across_v2_zksync_events") }},
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
            t1.realized_lp_fee_pct,
            t1.relayer_fee_pct,
            t1.destination_token_symbol
        from flattened_json t1
        left join
            zksync_extraction t2
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
    realized_lp_fee_pct,
    relayer_fee_pct,
    null as destination_token_symbol
from relays_v2

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
    realized_lp_fee_pct,
    relayer_fee_pct,
    destination_token_symbol
from zksync_transfers
