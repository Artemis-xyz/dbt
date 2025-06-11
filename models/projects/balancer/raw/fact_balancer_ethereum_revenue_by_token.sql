{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
        alias='fact_balancer_ethereum_revenue_by_token'
    )
}}

-- This is currently incomplete, but we can use it as a starting point. There are missing fees

with results as (
    select
        block_timestamp::date as date,
        contract_address,
        p.symbol as token,
        sum(raw_amount_precise::number / 1e18) as amount_native,
        sum(raw_amount_precise::number / 1e18 * p.price) as amount_usd
    from
        ethereum_flipside.core.ez_token_transfers t
        left join ethereum_flipside.price.ez_prices_hourly p on p.token_address = t.contract_address
        and p.hour = t.block_timestamp::date
    where
        to_address = lower('0xce88686553686DA562CE7Cea497CE749DA109f9F') -- Balancer v2 Protocol Fee Collector
    GROUP BY 
        1
        , 2
        , 3
)
select * from results having amount_usd < 1e6