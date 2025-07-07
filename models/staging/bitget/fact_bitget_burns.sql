{{
    config(
        materialized="table",
        snowflake_warehouse="BITGET"
    )
}}

select
    t.block_timestamp::date as date,
    sum(t.raw_amount_precise) / 1e18 as burns_native,
    sum(CASE 
        WHEN t.block_timestamp::date > '2025-04-15' THEN t.raw_amount_precise * p.price 
        ELSE 0 
    END) / 1e18 AS revenue
FROM 
    ethereum_flipside.core.ez_token_transfers t
LEFT JOIN  ethereum_flipside.price.ez_prices_hourly p
ON t.block_timestamp::date = p.hour
AND lower(p.token_address) = lower('0x54D2252757e1672EEaD234D27B1270728fF90581')
WHERE 1=1
AND lower(t.contract_address) = lower('0x54D2252757e1672EEaD234D27B1270728fF90581')
AND t.to_address = '0x000000000000000000000000000000000000dead'
GROUP BY 1
ORDER BY 1 DESC
