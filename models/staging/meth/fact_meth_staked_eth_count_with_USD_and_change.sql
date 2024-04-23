{{ config(materialized="table") }}
with
    temp as (
        select
            f.date,
            f.value as num_staked_eth,
            f.value * p.shifted_token_price_usd as amount_staked_usd,
            p.shifted_token_price_usd
        from {{ ref("fact_meth_staked_eth_count") }} f
        join pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold p on f.date = p.date
        where p.coingecko_id = 'ethereum'
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
                    t.shifted_token_price_usd
                    + lag(t.shifted_token_price_usd, 1) over (order by t.date)
                )
                / 2
            )
    end as amount_staked_usd_net_change  
from temp t
order by date desc
