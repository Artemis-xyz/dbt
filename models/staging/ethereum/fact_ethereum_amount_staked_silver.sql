{{ config(snowflake_warehouse="ETHEREUM_XS", materialized="table") }}
with
    min_date as (
        select min(to_date(slot_timestamp)) as start_date
        from ethereum_flipside.beacon_chain.fact_blocks
    ),
    date_spine as (
        select
            dateadd(
                day,
                row_number() over (order by seq4()) - 1,
                (select start_date from min_date)
            ) as date
        from table(generator(rowcount => 10000))
        qualify date <= dateadd(day, -1, current_date())
    ),
    prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
    staking_data as (
        select t1.slot_number, t2.slot_timestamp, sum(balance) as balance
        from ethereum_flipside.beacon_chain.fact_validator_balances t1
        left join ethereum_flipside.beacon_chain.fact_blocks t2
            on t1.slot_number = t2.slot_number
        group by t1.slot_number, t2.slot_timestamp
    ),
    raw_amount_staked as (
        select to_date(slot_timestamp) as date, avg(balance) as total_staked_native
        from staking_data
        group by date
    ),
    amount_staked as (
        select
            ds.date,
            LAST_VALUE(ras.total_staked_native ignore nulls) over (
                order by ds.date
                rows between unbounded preceding and current row
            ) as total_staked_native
        from date_spine ds
        left join raw_amount_staked ras
            on ds.date = ras.date
    )
select
    amount_staked.date,
    'ethereum' as chain,
    total_staked_native,
    total_staked_native * price as total_staked_usd
from amount_staked
left join prices on amount_staked.date = prices.date
where amount_staked.date < to_date(sysdate())
