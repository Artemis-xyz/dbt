{{ config(materialized="table") }}
with
    prices as ({{get_coingecko_price_with_latest("ethereum")}}),
    temp as (
        select
            f.date,
            f.value as num_staked_eth,
            f.value * price as amount_staked_usd,
            price
        from {{ ref("fact_meth_staked_eth_count") }} f
        left join prices on f.date = prices.date
        order by date desc
    )
select
    t.date,
    t.num_staked_eth,
    t.amount_staked_usd,
    case
        when t.date = '2023-11-01'
        then 0
        else t.num_staked_eth - lag(t.num_staked_eth, 1) over (order by t.date)
    end as num_staked_eth_net_change,  
    case
        when t.date = '2023-11-01'
        then 0
        else
            (t.num_staked_eth - lag(t.num_staked_eth, 1) over (order by t.date)) * (
                (
                    t.price
                    + lag(t.price, 1) over (order by t.date)
                )
                / 2
            )
    end as amount_staked_usd_net_change  
from temp t
order by date desc
