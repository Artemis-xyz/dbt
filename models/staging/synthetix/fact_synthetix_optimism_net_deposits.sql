{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with deposits_and_withdrawals as (
    select
        date_trunc('day', block_timestamp) as date, 
        sum(case 
                when to_address ILIKE '0xffffffaeff0b96ea8e4f94b2253f31abdd875847' 
                then coalesce(amount_usd, 0) 
                else 0 
            end) as daily_deposits,
        sum(case 
                when from_address ILIKE '0xffffffaeff0b96ea8e4f94b2253f31abdd875847' 
                then coalesce(amount_usd, 0) 
                else 0 
            end) as daily_withdrawals
    from optimism_flipside.core.ez_token_transfers
    group by date 
    order by date desc
)

select
    date,
    'optimism' as chain,
    daily_deposits - daily_withdrawals as net_deposits
from deposits_and_withdrawals