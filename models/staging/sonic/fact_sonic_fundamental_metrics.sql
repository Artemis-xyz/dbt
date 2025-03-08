{{
    config(
        materialized="table",
        snowflake_warehouse="SONIC",
    )
}}

SELECT
    block_timestamp::date as date,
    sum(gas_usd) as fees,
    count(distinct tx_hash) as txns,
    count(distinct from_address) as dau
FROM {{ ref("fact_sonic_transactions") }}
GROUP BY 1