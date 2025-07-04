{{
    config(
        materialized="table",
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
            , 'weth' as source_token_symbol
            , 'weth' as destination_token_symbol
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
            , '0x4200000000000000000000000000000000000006' as origin_token
            , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as destination_token
            , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address
            , 'base' as source_chain
            , 'ethereum' as destination_chain
            , coalesce(eth_prices.price, 0) as price_usd
            , 'weth' as source_token_symbol
            , 'weth' as destination_token_symbol
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
    , base_bridge_transfers as (
        select *
        from super_chain_bridge_transfers
        union
        select *
        from portal_transfers
    )
    SELECT 
        MAX(block_timestamp) AS block_timestamp
        , MAX(src_messaging_contract_address) AS src_messaging_contract_address
        , MAX(dst_messaging_contract_address) AS dst_messaging_contract_address
        , MAX(src_block_timestamp) AS src_block_timestamp
        , MAX(dst_block_timestamp) AS dst_block_timestamp
        , MAX(transaction_hash) AS transaction_hash
        , src_transaction_hash
        , dst_transaction_hash
        , MAX(event_index) AS event_index
        , src_event_index
        , dst_event_index
        , MAX(depositor) AS depositor
        , MAX(recipient) AS recipient
        , MAX(amount_native) AS amount_native
        , MAX(amount_adjusted) AS amount_adjusted
        , MAX(amount_usd) AS amount_usd
        , MAX(fee) AS fee
        , MAX(origin_token) AS origin_token
        , MAX(destination_token) AS destination_token
        , MAX(token_address) AS token_address
        , MAX(source_chain) AS source_chain
        , MAX(destination_chain) AS destination_chain
        , MAX(price_usd) AS price_usd
        , MAX(source_token_symbol) AS source_token_symbol
        , MAX(destination_token_symbol) AS destination_token_symbol
        , MAX(decimals) AS decimals
    FROM base_bridge_transfers
    GROUP BY dst_transaction_hash, dst_event_index, src_transaction_hash, src_event_index
