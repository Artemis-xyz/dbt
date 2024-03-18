{{
    config(
        materialized="table",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

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
    decoded_log:"updatableRelayData":"relayerFeePct"::integer / 1e18 as relayer_fee_pct
from ethereum_flipside.core.fact_decoded_event_logs
where
    event_name = 'FilledRelay'
    and contract_address = '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5'
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
    decoded_log:"updatableRelayData":"relayerFeePct" / 1e18 as relayer_fee_pct
from arbitrum_flipside.core.fact_decoded_event_logs
where
    event_name = 'FilledRelay'
    and contract_address = '0xe35e9842fceaca96570b734083f4a58e8f7c5f2a'
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
        when
            decoded_log:"destinationToken"::string
            = '0x4200000000000000000000000000000000000006'
        then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        else decoded_log:"destinationToken"::string
    end as destination_token,
    decoded_log:"originChainId"::integer as origin_chain_id,
    decoded_log:"realizedLpFeePct"::integer / 1e18 as realized_lp_fee_pct,
    decoded_log:"updatableRelayData":"relayerFeePct" / 1e18 as relayer_fee_pct
from optimism_flipside.core.fact_decoded_event_logs
where
    event_name = 'FilledRelay'
    and contract_address = '0x6f26bf09b1c792e3228e5467807a900a503c0281'
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
    decoded_log:"updatableRelayData":"relayerFeePct" / 1e18 as relayer_fee_pct
from polygon_flipside.core.fact_decoded_event_logs
where
    event_name = 'FilledRelay'
    and contract_address = '0x9295ee1d8c5b022be115a2ad3c30c72e34e7f096'
    {% if is_incremental() %}

        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

    {% endif %}
