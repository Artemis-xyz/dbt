{% macro get_decoded_l1_superchain_bridge_events(chain, bridge_contract_address, decoding_type)%}
with
    prices as (
        select date as date, contract_address, decimals, symbol, shifted_token_price_usd as price
        from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
        inner join {{ ref('dim_coingecko_token_map')}}
            on coingecko_id = coingecko_token_id
        where
            chain = 'ethereum'
            and date < dateadd(day, -1, to_date(sysdate()))
            {% if is_incremental() %}
                and date >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )
            {% endif %}
        union
        select dateadd('day', -1, to_date(sysdate())) as date, contract_address, decimals, symbol, token_current_price as price
        from {{ ref("fact_coingecko_token_realtime_data") }}
        inner join {{ ref('dim_coingecko_token_map')}}
            on token_id = coingecko_token_id
        where chain = 'ethereum'
            {% if is_incremental() %}
                and date >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )
            {% endif %}
        union
        select to_date(sysdate()) as date, contract_address, decimals, symbol, token_current_price as price
        from {{ ref("fact_coingecko_token_realtime_data") }}
        inner join {{ ref('dim_coingecko_token_map')}}
            on token_id = coingecko_token_id
        where chain = 'ethereum'
            {% if is_incremental() %}
                and date >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )
            {% endif %}
    )

    , eth_deposits as (
        select
            block_timestamp
            {% if decoding_type == 'artemis' %}
                , transaction_hash
            {% else %}
                , tx_hash as transaction_hash
            {% endif %}
            , event_index
            , decoded_log:"from"::string as depositor
            , decoded_log:"to"::string as recipient
            , decoded_log:"amount"::bigint as amount
            , null as fee
            -- wrapped eth address on ethereum
            , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as origin_token
            -- wrapped eth address on l2
            , '0x4200000000000000000000000000000000000006' as destination_token
            , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address
            , 'ethereum' as source_chain
            , '{{ chain }}' as destination_chain
        {% if decoding_type == 'artemis' %}
            from {{ ref('fact_ethereum_decoded_events') }} 
        {% else %}
            from ethereum_flipside.core.ez_decoded_event_logs
        {% endif %}
        where contract_address = lower('{{ bridge_contract_address }}')
        and event_name = 'ETHDepositInitiated'
        {% if is_incremental() %}

            and block_timestamp >= (
                select dateadd('day', -3, max(block_timestamp))
                from {{ this }}
            )

        {% endif %}
    )

    , eth_withdraws as (
        select
            block_timestamp
            {% if decoding_type == 'artemis' %}
                , transaction_hash
            {% else %}
                , tx_hash as transaction_hash
            {% endif %}
            , event_index
            , decoded_log:"from"::string as depositor
            , decoded_log:"to"::string as recipient
            , decoded_log:"amount"::bigint as amount
            , null as fee
            -- wrapped eth address on ethereum
            , '0x4200000000000000000000000000000000000006' as origin_token
            -- wrapped eth address on l2
            , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as destination_token
            , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address
            , '{{ chain }}' as source_chain
            , 'ethereum' as destination_chain
        {% if decoding_type == 'artemis' %}
            from {{ ref('fact_ethereum_decoded_events') }} 
        {% else %}
            from ethereum_flipside.core.ez_decoded_event_logs
        {% endif %}
        where
            contract_address
            = lower('{{ bridge_contract_address }}')
            and event_name = 'ETHWithdrawalFinalized'
            {% if is_incremental() %}

                and block_timestamp >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )

            {% endif %}
    )

    , erc20_deposits as (
        select
            block_timestamp
            {% if decoding_type == 'artemis' %}
                , transaction_hash
            {% else %}
                , tx_hash as transaction_hash
            {% endif %}
            , event_index
            , decoded_log:"from"::string as depositor
            , decoded_log:"to"::string as recipient
            , decoded_log:"amount"::bigint as amount
            , null as fee
            , decoded_log:"l1Token" as origin_token
            -- wrapped eth address on l2
            , decoded_log:"l2Token" as destination_token
            , decoded_log:"l1Token" as token_address
            , 'ethereum' as source_chain
            , '{{ chain }}' as destination_chain
        {% if decoding_type == 'artemis' %}
            from {{ ref('fact_ethereum_decoded_events') }} 
        {% else %}
            from ethereum_flipside.core.ez_decoded_event_logs
        {% endif %}
        where
            contract_address = lower('{{ bridge_contract_address }}')
            and event_name = 'ERC20DepositInitiated'
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    )

    , erc20_withdraws as (
        select
            block_timestamp
            {% if decoding_type == 'artemis' %}
                , transaction_hash
            {% else %}
                , tx_hash as transaction_hash
            {% endif %}
            , event_index
            , decoded_log:"from"::string as depositor
            , decoded_log:"to"::string as recipient
            , decoded_log:"amount"::bigint as amount
            , null as fee
            , decoded_log:"l2Token" as origin_token
            , decoded_log:"l1Token" as destination_token
            , decoded_log:"l1Token" as token_address
            , '{{ chain }}' as source_chain
            , 'ethereum' as destination_chain
        {% if decoding_type == 'artemis' %}
            from {{ ref('fact_ethereum_decoded_events') }} 
        {% else %}
            from ethereum_flipside.core.ez_decoded_event_logs
        {% endif %}
        where
            contract_address = lower('{{ bridge_contract_address }}')
            and event_name = 'ERC20WithdrawalFinalized'
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),
    bridge_transfers as (
        select *
        from eth_deposits
        union
        select *
        from eth_withdraws
        union
        select *
        from erc20_deposits
        union
        select *
        from erc20_withdraws
    )
    select
        block_timestamp
        , transaction_hash
        , event_index
        , depositor
        , recipient
        , amount as amount_native 
        , amount / POW(10, decimals) as amount_adjusted
        , amount / POW(10, decimals) * coalesce(prices.price, 0) as amount_usd
        , fee
        , origin_token
        , destination_token
        , token_address
        , source_chain
        , destination_chain
        , coalesce(prices.price, 0) as price_usd
        , symbol as source_token_symbol
        , symbol as destination_token_symbol
        , decimals
    from bridge_transfers 
    left join 
    prices on date_trunc('day', block_timestamp) = prices.date
    and token_address = prices.contract_address
{% endmacro %}