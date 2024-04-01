{{ config(snowflake_warehouse="ETHEREUM_XS", materialized="table") }}
with
    prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
    staking_data as (
        select t1.slot_number, t2.slot_timestamp, sum(balance) as balance
        from ethereum_flipside.beacon_chain.fact_validator_balances t1
        left join
            ethereum_flipside.beacon_chain.fact_blocks t2
            on t1.slot_number = t2.slot_number
        group by t1.slot_number, t2.slot_timestamp
    ),
    amount_staked as (
        select to_date(slot_timestamp) as date, avg(balance) as total_staked_native
        from staking_data
        group by date
        order by date
    )
select
    amount_staked.date,
    'ethereum' as chain,
    total_staked_native,
    total_staked_native * price as total_staked_usd
from amount_staked
left join prices on amount_staked.date = prices.date
where amount_staked.date < to_date(sysdate())
