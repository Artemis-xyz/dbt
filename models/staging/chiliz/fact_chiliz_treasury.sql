{{
    config(
        materialized="table",
        snowflake_warehouse="CHILIZ"
    )
}}

with chiliz_treasury as (
    {{ get_treasury_balance(
        chain='ethereum',
        addresses=[
            '0xcd38983905eB4A433Fc44B3C90321522D8340BF2',
            '0x3DD509eeA1CBE2FE00fbbce496DD453Bfde74e7F',
            '0x45a2EB4d96A84a1B408e98f04a3908776F2A41B4',
            '0xCc477b21D471fb9394a56aace72c8D59Ac80f6Af'
        ],
        earliest_date='2018-10-30'
    )
    }}
)

SELECT
    date,
    sum(native_balance) as native_balance,
    sum(native_balance) - lag(sum(native_balance)) over (order by date) as native_balance_change,
    sum(usd_balance) as usd_balance,
    sum(usd_balance) - lag(sum(usd_balance)) over (order by date) as usd_balance_change
FROM chiliz_treasury
WHERE lower(contract_address) = lower('0x3506424F91fD33084466F402d5D97f05F8e3b4AF')
GROUP BY 1
ORDER BY 1 DESC