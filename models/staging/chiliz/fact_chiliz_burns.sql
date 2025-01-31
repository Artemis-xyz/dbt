{{
    config(
        materialized="table",
        snowflake_warehouse="CHILIZ"
    )
}}

select
    t.block_timestamp::date as date,
    sum(t.raw_amount_precise) / 1e18 as burns_native,
    sum(t.raw_amount_precise * p.price) / 1e18 as revenue
FROM 
    ethereum_flipside.core.ez_token_transfers t
LEFT JOIN  ethereum_flipside.price.ez_prices_hourly p
ON t.block_timestamp::date = p.hour
AND lower(p.token_address) = lower('0x3506424F91fD33084466F402d5D97f05F8e3b4AF')
WHERE 1=1
AND lower(t.contract_address) = lower('0x3506424F91fD33084466F402d5D97f05F8e3b4AF')
AND t.to_address = '0x000000000000000000000000000000000000dead'
GROUP BY 1
ORDER BY 1 DESC