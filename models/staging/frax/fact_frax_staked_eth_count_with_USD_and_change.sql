{{ config(materialized="table") }}
with
    prices as ({{get_coingecko_price_with_latest("ethereum")}}),
    temp as (
        select
            f.date,
            f.value as num_staked_eth,
            f.value * price as amount_staked_usd,
            price
        from {{ ref("fact_frax_staked_eth_count") }} f
        left join prices on f.date = prices.date
        order by date desc
    )
select
    t.date,
    t.num_staked_eth,
    t.amount_staked_usd,
    case
        when t.date = '2022-10-07'
        then 0
        else t.num_staked_eth - lag(t.num_staked_eth, 1) over (order by t.date)
    end as num_staked_eth_net_change,  -- no previous date for 2022-10-07, so cannot calculate net change, hence set to 0
    case
        when t.date = '2022-10-07'
        then 0
        -- amount_staked_usd_net_change: (today's num eth staked -  prev day num eth
        -- staked ) * avg eth_price_USD
        else
            (t.num_staked_eth - lag(t.num_staked_eth, 1) over (order by t.date)) * (
                (
                    t.price
                    + lag(t.price, 1) over (order by t.date)
                )
                / 2
            )
    end as amount_staked_usd_net_change  -- no previous date for 2022-10-07, so cannot calculate net change, hence set to 0
from temp t
order by date desc
