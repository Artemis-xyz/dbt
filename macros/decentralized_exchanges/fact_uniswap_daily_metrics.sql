{% macro fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        chain, app, version, source_table
) %}

    {% set _version = version.lower() %}
    with 
        {% if chain == 'avalanche' %} 
            prices as ({{get_coingecko_price_with_latest("avalanche-2")}})
        {% elif chain == 'polygon' %}
            prices as ({{get_coingecko_price_with_latest("matic-network")}})
        {% else %}
            prices as ({{get_coingecko_price_with_latest("ethereum")}})
        {% endif %}

    select
        block_timestamp::date as date,
        '{{ chain }}' as chain,
        '{{ app }}' as app,
        '{{ _version }}' as version,
        'DeFi' as category,
        pool,
        token_0,
        token_0_symbol,
        token_1,
        token_1_symbol,
        sum(trading_volume) as trading_volume,
        sum(trading_fees) as trading_fees,
        sum(
            case 
                when sender not in (select pool from {{ ref(source_table) }}) then 1 
                else 0
            end
        ) as unique_traders,
        sum(coalesce(gas_cost_native, 0)) as gas_cost_native,
        sum(coalesce(gas_cost_native * prices.price, 0)) as gas_cost_usd
    from {{ ref(source_table) }} 
    left join prices on block_timestamp::date = prices.date
    where block_timestamp::date is not null
    group by block_timestamp::date, pool, token_0, token_0_symbol, token_1, token_1_symbol
{% endmacro %}


{% macro fact_daily_uniswap_fork_trading_vol_fees_traders(
        chain, app, version, source_table
) %}

    {% set _version = version.lower() %}
    with 
        {% if chain == 'avalanche' %} 
            prices as ({{get_coingecko_price_with_latest("avalanche-2")}})
        {% elif chain == 'polygon' %}
            prices as ({{get_coingecko_price_with_latest("matic-network")}})
        {% else %}
            prices as ({{get_coingecko_price_with_latest("ethereum")}})
        {% endif %}

    select
        block_timestamp::date as date,
        '{{ chain }}' as chain,
        '{{ app }}' as app,
        '{{ _version }}' as version,
        'DeFi' as category,
        sum(trading_volume) as trading_volume,
        sum(trading_fees) as trading_fees,
        sum(
            case 
                when sender not in (select pool from {{ ref(source_table) }}) then 1 
                else 0
            end
        ) as unique_traders,
        sum(coalesce(gas_cost_native, 0)) as gas_cost_native,
        sum(coalesce(gas_cost_native * prices.price, 0)) as gas_cost_usd
    from {{ ref(source_table) }}
    left join prices on block_timestamp::date = prices.date
    where block_timestamp::date is not null
    group by block_timestamp::date 
{% endmacro %}


{% macro fact_daily_uniswap_fork_tvl(
        chain, app, version, source_table
) %}

    {% set _version = version.lower() %}

    select
        date,
        '{{ chain }}' as chain,
        '{{ app }}' as app,
        '{{ _version }}' as version,
        'DeFi' as category,
        sum(tvl) as tvl
    from {{ ref(source_table) }}
    where date is not null
    group by date
{% endmacro %}