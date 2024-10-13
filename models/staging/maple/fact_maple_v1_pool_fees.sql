{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

SELECT * FROM (
    VALUES 
    ('Orthogonal Credit USDC1', 0.1, 0.1),
    ('M11 Credit USDC1', 0.1, 0),
    ('M11 Credit USDC2', 0.1, 0.1),
    ('M11 Credit WETH', 0.1, 0.1),
    ('Alameda Research - USDC', 0, 0.05),
    ('Celsius WETH Pool', 0, 0),
    ('Blocktower Capital - USDC01', 0, 0.05)
) AS fees (pool_name, delegate_fee, staking_fee)