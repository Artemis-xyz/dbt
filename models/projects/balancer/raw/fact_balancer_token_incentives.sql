{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
        alias='fact_balancer_token_incentives'
    )
}}

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
where 1=1
    and from_address = '0x6d19b2bf3a36a61530909ae65445a906d98a2fa8'
    and contract_address = lower('0xba100000625a3754423978a60c9317c58a424e3D')
GROUP BY
    1,2,3