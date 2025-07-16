{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
        database="pendle",
        schema="raw",
        alias="fact_pendle_treasury",
    )
}}

with treasury_distributor as (
    {{ forward_filled_balance_for_address('ethereum', '0x399be606db281a054e359eb709df9f21e922ec9a') }}
)
, treasury_vester as (
     {{ forward_filled_balance_for_address('ethereum', '0xc21A74c7150fed22C7CA0Bf9A15BBe0DdB4977CC') }}
)
SELECT
    date
    , 'PENDLE' as token
    , contract_address
    , address
    , balance_raw
    , balance_native
    , price
    , balance
    , chain
    , unique_id
FROM treasury_distributor
WHERE contract_address = '0x808507121b80c02388fad14726482e061b8da827'
UNION ALL
SELECT
    date
    , 'PENDLE' as token
    , contract_address
    , address
    , balance_raw
    , balance_native
    , price
    , balance
    , chain
    , unique_id
FROM treasury_vester
WHERE contract_address = '0x808507121b80c02388fad14726482e061b8da827'