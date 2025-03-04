{{ config(materialized="incremental", snowflake_warehouse="UNICHAIN", unique_key=["date"]) }}

with 
eth_l2_cost as (
    {{ get_eth_l2_cost(
        addresses=[
            '0xff00000000000000000000000000000000000480',
            '0x069c4c579671f8c120b1327a73217D01Ea2EC5ea'
        ],
        start_date = '2024-11-04'
        )
    }}
),
fundamental_data as (
    {{ get_goldsky_chain_fundamental_metrics("worldchain") }}
),
price as (
    {{ get_coingecko_price_with_latest('ethereum') }}
)

SELECT 
    fd.date,
    fd.daa,
    fd.txns,
    fd.fees_native,
    fees_native * price as fees,
    coalesce(c.gas, 0) as cost_native,
    coalesce(c.gas, 0) * price as cost,
    fees - coalesce(c.gas * price, 0) as revenue,
    fees_native - coalesce(c.gas, 0) as revenue_native
FROM fundamental_data fd
LEFT JOIN price p
ON fd.date = p.date
LEFT JOIN eth_l2_cost as c
on fd.date = c.date
