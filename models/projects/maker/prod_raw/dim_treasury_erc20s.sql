{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="dim_treasury_erc20s"
    )
}}

SELECT * FROM (VALUES
    ('0xc18360217d8f7ab5e7c516566761ea12ce7f9d72', '0xc18360217d8f7ab5e7c516566761ea12ce7f9d72', 18, 'ENS'),
    ('0x4da27a545c0c5b758a6ba100e3a049001de870f5', '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 18, 'stkAAVE'),
    ('0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 18, 'AAVE'),
    ('0xc00e94cb662c3520282e6f5717214004a7f26888', '0xc00e94cb662c3520282e6f5717214004a7f26888', 18, 'COMP')
) AS t(contract_address, price_address, decimals, token)