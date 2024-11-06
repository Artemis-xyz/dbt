{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
    )
}}

with eth_prices as (
    select * from {{ source('ETHEREUM_FLIPSIDE', 'ez_prices_hourly') }}
    where is_native = True
)
select
    block_timestamp,
    tx_hash,
    contract_address,
    pools.pool_name,
    pools.asset,
    CASE
        -- For the WETH Pools, convert to USD using the current price of ETH
        WHEN contract_address = '0x1a066b0109545455bc771e49e6edef6303cb0a93' OR contract_address = '0xa1fe1b5fc23c2dab0c28d4cc09021014f30be8f1' 
            THEN interest / POW(10, 18) * p.price
            ELSE interest / POW(10, 6)
    END AS interest_to_lps_usd,
    CASE
        WHEN contract_address = '0x1a066b0109545455bc771e49e6edef6303cb0a93' OR contract_address = '0xa1fe1b5fc23c2dab0c28d4cc09021014f30be8f1' 
            THEN interest / POW(10, 18)
            ELSE interest / POW(10, 6)
    END AS interest_to_lps_native
FROM
    {{ ref('fact_maple_Pool_evt_Claim') }} c
LEFT JOIN eth_prices p on p.hour = date_trunc('hour', c.block_timestamp)
LEFT JOIN {{ ref('dim_maple_pools') }} pools ON c.contract_address = pools.v1_pool_id