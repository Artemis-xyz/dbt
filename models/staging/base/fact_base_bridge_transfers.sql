{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index", "src_messaging_contract_address", "dst_messaging_contract_address"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

with 
    eth_prices as ({{ get_coingecko_price_with_latest('ethereum') }})
    , super_chain_bridge_transfers as (
        {{ get_decoded_l1_superchain_bridge_events('base', '0x3154Cf16ccdb4C6d922629664174b904d80F2C35', 'flipside')}}
    )
    , portal_transfers as (
        select
            block_timestamp
            , '0x49048044d57e1c92a77f79988d21fa8faf74e97e' as src_messaging_contract_address
            , null as dst_messaging_contract_address
            , block_timestamp as src_block_timestamp
            , null as dst_block_timestamp
            , tx_hash as transaction_hash
            , tx_hash as src_transaction_hash
            , null as dst_transaction_hash
            , trace_index as event_index
            , trace_index as src_event_index
            , null as dst_event_index
            , origin_from_address::string as depositor
            , origin_from_address::string as recipient
            , amount_precise_raw::bigint as amount_native
            , amount_precise_raw::bigint / POW(10, 18) as amount_adjusted
            , amount_precise_raw::bigint / POW(10, 18) * coalesce(eth_prices.price, 0) as amount_usd
            , null as fee
            , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as origin_token
            , '0x4200000000000000000000000000000000000006' as destination_token
            , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address
            , 'ethereum' as source_chain
            , 'base' as destination_chain
            , coalesce(eth_prices.price, 0) as price_usd
            , 'eth' as source_token_symbol
            , 'eth' as destination_token_symbol
            , 18 as decimals
        from ethereum_flipside.core.ez_native_transfers
        left join eth_prices on date_trunc('day', block_timestamp) = eth_prices.date
        where
            to_address = lower('0x49048044d57e1c92a77f79988d21fa8faf74e97e')
            and origin_to_address
            = lower('0x49048044d57e1c92a77f79988d21fa8faf74e97e')
            {% if is_incremental() %}

                and block_timestamp >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )

            {% endif %}
        union all
        select
            block_timestamp
            , null as src_messaging_contract_address
            , '0x49048044d57e1c92a77f79988d21fa8faf74e97e' as dst_messaging_contract_address
            , null as src_block_timestamp
            , block_timestamp as dst_block_timestamp
            , tx_hash as transaction_hash
            , null as src_transaction_hash
            , tx_hash as dst_transaction_hash
            , trace_index as event_index
            , null as src_event_index
            , trace_index as dst_event_index
            , origin_to_address::string as depositor
            , origin_to_address::string as recipient
            , amount_precise_raw::bigint as amount_native
            , amount_precise_raw::bigint / POW(10, 18) as amount_adjusted
            , amount_precise_raw::bigint / POW(10, 18) * coalesce(eth_prices.price, 0) as amount_usd
            , null as fee
            , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as origin_token
            , '0x4200000000000000000000000000000000000006' as destination_token
            , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address
            , 'base' as source_chain
            , 'ethereum' as destination_chain
            , coalesce(eth_prices.price, 0) as price_usd
            , 'eth' as source_token_symbol
            , 'eth' as destination_token_symbol
            , 18 as decimals
        from ethereum_flipside.core.ez_native_transfers
        left join eth_prices on date_trunc('day', block_timestamp) = eth_prices.date
        where
            (origin_from_address
            = lower('0x49048044d57e1c92a77f79988d21fa8faf74e97e')
            or from_address = lower('0x49048044d57e1c92a77f79988d21fa8faf74e97e'))
            {% if is_incremental() %}

                and block_timestamp >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )

            {% endif %}
    )

select *
from super_chain_bridge_transfers
union
select *
from portal_transfers
