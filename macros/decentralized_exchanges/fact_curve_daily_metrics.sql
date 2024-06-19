{% macro fact_daily_curve_trading_vol_fees_traders_by_pool(chain, source_table, app="curve") %}
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
        'DeFi' as category,
        pool,
        sum(trading_volume) as trading_volume,
        sum(trading_fees) as trading_fees,
        sum(trading_revenue) as trading_revenue,
        count(distinct
            case 
                when sender not in (select pool from {{ ref(source_table) }}) then sender
                else null
            end
        ) as unique_traders,
        sum(coalesce(gas_cost_native, 0)) as gas_cost_native,
        sum(coalesce(gas_cost_native * prices.price, 0)) as gas_cost_usd
    from {{ ref(source_table) }} 
    left join prices on block_timestamp::date = prices.date
    where block_timestamp::date is not null
    group by block_timestamp::date, pool
{% endmacro %}
