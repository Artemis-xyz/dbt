{% macro calc_staked_eth(table_name) %}
with
    temp as (
        select
            f.date,
            f.total_supply as num_staked_eth,
            f.total_supply * p.shifted_token_price_usd as amount_staked_usd,
            p.shifted_token_price_usd
        from {{ ref(table_name) }} f
        join pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold as p
            on f.date = p.date
        where p.coingecko_id = 'ethereum'
        order by date desc
    )
select
    t.date,
    t.num_staked_eth,
    t.amount_staked_usd,
    case
        when t.date = (select min(date) from {{ ref(table_name) }})
        then 0
        else t.num_staked_eth - lag(t.num_staked_eth, 1) over (order by t.date)
    end as num_staked_eth_net_change,  
    case
        when t.date = (select min(date) from {{ ref(table_name) }})
        then 0
        else
            (t.num_staked_eth - lag(t.num_staked_eth, 1) over (order by t.date)) * (
                (
                    t.shifted_token_price_usd
                    + lag(t.shifted_token_price_usd, 1) over (order by t.date)
                )
                / 2
            )
    end as amount_staked_usd_net_change  
from temp t
order by date desc
{% endmacro %}
