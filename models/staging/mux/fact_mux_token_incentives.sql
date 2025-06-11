{{ config(materialized="table") }}


with mcb_claims as (
    select
        date(block_timestamp) as date,
        sum(
            TRY_CAST(decoded_log:"amount"::STRING AS FLOAT) / 1e18 * eph.price
        ) as token_incentives
    from arbitrum_flipside.core.ez_decoded_event_logs as tt
    join arbitrum_flipside.price.ez_prices_hourly as eph
       on lower(eph.token_address) = lower('0x4e352cF164E64ADCBad318C3a1e222E9EBa4Ce42') and eph.hour = date_trunc('hour', tt.block_timestamp)
    where lower(contract_address) IN (
        lower('0xBCF8c124975DE6277D8397A3Cad26E2333620226')
    )
    and event_name = 'Claim'
    and MOD(TRY_CAST(decoded_log:"amount"::STRING AS FLOAT) / 1e18, 1) != 0
    group by date
)

select * from mcb_claims