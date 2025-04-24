{{
    config(
        materialized="table",
        snowflake_warehouse="INJECTIVE",
    )
}}

with outflows_data as (
    SELECT 
        DATE_TRUNC('day', block_timestamp) as date,
        SUM(amount) as outflows
    FROM 
        ethereum_flipside.core.ez_token_transfers
    WHERE 
        LOWER(contract_address) = LOWER('0xe28b3B32B6c345A34Ff64674606124Dd5Aceca30') -- INJ ERC20 Contract Address
        AND LOWER(from_address) IN (
            LOWER('0x7103E07e404D2BC6CE354688E52Ac1f4629EdfCC'), -- Wallet which was initially seeded all 100m INJ
            LOWER('0x7E233EAfC76243474369bd080238fD6EB36A73CE')  -- Treasury/Escrow wallet 
        )
        AND LOWER(to_address) != LOWER('0x7E233EAfC76243474369bd080238fD6EB36A73CE') -- Treasury/Escrow wallet
    GROUP BY 
        1
)
SELECT 
    date,
    sum(outflows) as outflows
FROM 
    outflows_data
GROUP BY 
    date