{{
    config(
        materialized='table',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='raw',
        alias='fact_convex_token_incentives'
    )
}}


select
    block_timestamp::date as date,
    t.contract_address,
    t.symbol,
    sum(raw_amount_precise / pow(10, 18)) as token_incentives_native,
    sum(raw_amount_precise / pow(10, 18) * p.price) as token_incentives
from
    {{ source('ETHEREUM_FLIPSIDE', 'ez_token_transfers') }} t
    left join {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p on (
        p.hour = block_timestamp::date
        and p.token_address = t.contract_address
    )
where
    contract_address = lower('0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b')
    and from_address = lower('0x0000000000000000000000000000000000000000')
    and date(block_timestamp) > '2021-05-17'
group by
    1, 2, 3
order by
    1 desc