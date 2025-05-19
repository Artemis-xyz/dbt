{{
    config(
        materialized="table",
        snowflake_warehouse="LIDO",
        database="lido",
        schema="raw",
        alias="fact_lido_token_incentives",
    )
}}

with ldo_prices as (
    SELECT
        hour
        , price
        , token_address
        , symbol
    FROM ethereum_flipside.price.ez_prices_hourly
    WHERE token_address in (
                        lower('0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32'),
                        lower('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
                        )
)
select
    date(p.hour) as date
    , symbol as token
    , sum(coalesce(t.raw_amount_precise::number / 1e18,0)) as amount_native
    , sum(coalesce(t.raw_amount_precise::number / 1e18 * p.price,0)) as amount_usd
from ldo_prices p
LEFT JOIN ethereum_flipside.core.ez_token_transfers t 
    ON p.hour = DATE_TRUNC('hour', t.block_timestamp)
    AND p.token_address = t.contract_address
    AND t.from_address IN (
        LOWER('0x87d93d9b2c672bf9c9642d853a8682546a5012b5'),
        LOWER('0x753D5167C31fBEB5b49624314d74A957Eb271709')
    )
    and contract_address in (lower('0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32'), lower('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'))
GROUP BY 1, 2