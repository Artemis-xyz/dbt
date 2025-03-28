{{ config(materialized="table", snowflake_warehouse="berachain") }}

with berachain_emissions as (
    select
        transaction_hash,
        block_timestamp,
        contract_address,
        decoded_log:"from"::string AS from_address,
        decoded_log:"to"::string AS to_address,
        decoded_log:"value"::numeric / 1e18 AS value
    from pc_dbt_db.prod.fact_berachain_decoded_events
    where
        event_name = 'Transfer'
        and contract_address = '0x656b95e550c07a9ffe548bd4085c72418ceb1dba'
),
minted as (
    select
        date(block_timestamp) as date,
        sum(value) as minted_amount
    from berachain_emissions
    where from_address = '0x0000000000000000000000000000000000000000'
    group by 1
),
burned as (
    select
        date(block_timestamp) as date,
        sum(value) as burnt_amount
    from berachain_emissions
    where to_address = '0x0000000000000000000000000000000000000000'
    group by 1
),
daily_emissions as (
    select
        m.date,
        coalesce(m.minted_amount, 0) as emission_native,
        coalesce(b.burnt_amount, 0) as burns_native,
        coalesce(s.premine_unlocks_supply, 0) as premine_unlocks_native,
        coalesce(premine_unlocks_native, 0) + coalesce(m.minted_amount, 0) - coalesce(b.burnt_amount, 0) as net_supply_change_native,
        sum(net_supply_change_native)
            over (order by m.date asc rows between unbounded preceding and current row) as circulating_supply_native
    from minted m
    left join burned b using (date)
    left join {{ source('MANUAL_STATIC_TABLES', 'berachain_daily_supply_data') }} s
        on m.date = s.date
)
select 
    date,
    emission_native,
    premine_unlocks_native,
    burns_native,
    net_supply_change_native,
    circulating_supply_native
from daily_emissions
order by date desc