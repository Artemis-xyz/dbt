{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with ethereum_fee_transfers as (
    select 
        date_trunc('day', block_timestamp) as date, 
        sum(amount_usd) as ethereum_fees
    from ethereum_flipside.core.ez_token_transfers
    where to_address ILIKE '0xfeefeefeefeefeefeefeefeefeefeefeefeefeef'
    group by date
    order by date
),

optimism_fee_transfers as (
    select 
        date_trunc('day', block_timestamp) as date, 
        sum(amount_usd) as optimism_fees
    from optimism_flipside.core.ez_token_transfers
    where to_address ILIKE '0xfeefeefeefeefeefeefeefeefeefeefeefeefeef'
    group by date
    order by date
)

select 
    coalesce(oft.date, eft.date) as date,
    coalesce(oft.optimism_fees, 0) + coalesce(eft.ethereum_fees, 0) as daily_fees
from optimism_fee_transfers as oft
full outer join ethereum_fee_transfers as eft
    on oft.date = eft.date
where oft.date > '2024-01-01'
order by date desc