{% macro fact_dex_priced_tokens(app, version, chain) %}
    with
        stablecoin_dex_swaps as (
            select
                block_timestamp
                , tx_hash
                , event_index
                , token0
                , token0_symbol
                , case
                    when lower(token0) in (select lower(contract_address) from {{ref("fact_" ~chain~"_stablecoin_contracts")}}) then 'TRUE'
                    else 'FALSE'
                end as token0_is_stablecoin
                , token0_amount
                , token1
                , token1_symbol
                , case 
                    when lower(token1) in (select lower(contract_address) from {{ref("fact_" ~chain~"_stablecoin_contracts")}}) then 'TRUE'
                    else 'FALSE'
                end as token1_is_stablecoin
                , token1_amount
            from {{ ref("fact_"~app~"_"~version~"_"~chain~"_swap_events") }} dex_model
            where (
                lower(token0) in (select lower(contract_address) from {{ref("fact_" ~chain~"_stablecoin_contracts")}})
                or 
                lower(token1) in (select lower(contract_address) from {{ref("fact_" ~chain~"_stablecoin_contracts")}})
            )
            and token0_amount > 0 and token1_amount > 0
            {% if is_incremental() %}
                and block_timestamp >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
            {% endif %}
        )

    select
        block_timestamp
        , event_index
        , tx_hash
        , '{{chain}}' as chain
        , '{{app}}' as app
        , '{{version}}' as version
        , '{{chain}}' || '-' || '{{app}}' || '-' || '{{version}}' as source
        , case 
            when token0_is_stablecoin = 'TRUE' then token1_symbol || '/' || token0_symbol
            when token1_is_stablecoin = 'TRUE' then token0_symbol || '/' || token1_symbol
        end as pair
        , case 
            when token0_is_stablecoin = 'TRUE' then token1
            when token1_is_stablecoin = 'TRUE' then token0
        end as token_address
        , case 
            when token0_is_stablecoin = 'TRUE' then token1_symbol
            when token1_is_stablecoin = 'TRUE' then token0_symbol
        end as symbol
        , case
            when token0_is_stablecoin = 'TRUE' then token0_amount/token1_amount
            when token1_is_stablecoin = 'TRUE' then token1_amount/token0_amount
        end as price
    from stablecoin_dex_swaps

{% endmacro %}