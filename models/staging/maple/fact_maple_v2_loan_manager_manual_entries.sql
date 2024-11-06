{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'MAPLE'
    )
}}

SELECT * FROM (
    VALUES 
        ('2022-12-12 00:00', '0xc2500c5bf2112cc39f80d314941d33b15e88a852b8e9a7ac4f9e7d6abdf89aad', 16164991, 'Orthogonal Credit USDC1', 4000000, 0, 'FTL Outstanding Update', 'USDC'),
        ('2022-12-12 00:00', '0xc2500c5bf2112cc39f80d314941d33b15e88a852b8e9a7ac4f9e7d6abdf89aad', 16164991, 'M11 Credit WETH', 22170, 21200, 'FTL Outstanding Update', 'WETH'),
        ('2022-12-12 00:00', '0xc2500c5bf2112cc39f80d314941d33b15e88a852b8e9a7ac4f9e7d6abdf89aad', 16164991, 'M11 Credit USDC2', 38500000, 14999998.753850002, 'FTL Outstanding Update', 'USDC'),
        ('2022-12-12 00:00', '0xc2500c5bf2112cc39f80d314941d33b15e88a852b8e9a7ac4f9e7d6abdf89aad', 16164991, 'M11 Credit USDC1', 7500000, 0, 'FTL Outstanding Update', 'USDC')
) AS t (DATE, TX_HASH, BLOCK, POOL_NAME, PRINCIPAL_OUT, DELTA, DESCRIPTION, ASSET)