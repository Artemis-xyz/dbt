{{ config(materialized="table") }}
with
    temp as (
        select
            f.date,
            f.seth2_value as seth2_num_staked_eth,
            f.reth2_value as reth2_num_staked_eth,
            f.seth2_value * p.shifted_token_price_usd as seth2_amount_staked_usd,
            f.reth2_value * p.shifted_token_price_usd as reth2_amount_staked_usd,
            p.shifted_token_price_usd
        from {{ ref("fact_stakewise_staked_eth_count") }} f
        join pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold p on f.date = p.date
        where p.coingecko_id = 'ethereum'
        order by date desc
    ),
    combined as (
        select
            t.date,
            t.seth2_num_staked_eth,
            t.reth2_num_staked_eth,
            t.seth2_amount_staked_usd,
            t.reth2_amount_staked_usd,
            t.seth2_num_staked_eth + t.reth2_num_staked_eth as num_staked_eth,
            t.seth2_amount_staked_usd + t.reth2_amount_staked_usd as amount_staked_usd,
            t.shifted_token_price_usd
        from temp t
        order by date desc
    )

select
    c.date,
    c.num_staked_eth,
    c.amount_staked_usd,
    case
        when c.date = '2021-01-26'
        then 0
        else c.num_staked_eth - lag(c.num_staked_eth, 1) over (order by c.date)
    end as num_staked_eth_net_change,  -- no previous date for 2021-01-26, so cannot calculate net change, hence set to 0
    case
        when c.date = '2021-01-26'
        then 0
        -- amount_staked_usd_net_change: (today's num eth staked -  prev day num eth
        -- staked ) * avg eth_price_USD
        else
            (c.num_staked_eth - lag(c.num_staked_eth, 1) over (order by c.date)) * (
                (
                    c.shifted_token_price_usd
                    + lag(c.shifted_token_price_usd, 1) over (order by c.date)
                )
                / 2
            )
    end as amount_staked_usd_net_change  -- no previous date for 2021-01-26, so cannot calculate net change, hence set to 0
from combined c
order by date desc
