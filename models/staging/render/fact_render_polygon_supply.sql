{{
    config(
        materialized="table",
        snowflake_warehouse="RENDER",
    )
}}

with daily_net_change as (
SELECT
    block_timestamp::date as date,
    sum(
        case when from_address = '0x0000000000000000000000000000000000000000'
            then amount
            when to_address in ('0x0000000000000000000000000000000000000000')
            then -1 * amount
        end
    ) as net_change
FROM {{ source("POLYGON_FLIPSIDE", "ez_token_transfers") }}
WHERE contract_address = lower('0x61299774020dA444Af134c82fa83E3810b309991')
GROUP BY 1
)
, date_spine as (
    SELECT date FROM dim_date_spine
    WHERE date between (SELECT MIN(date) FROM daily_net_change) and to_date(sysdate())
)
SELECT
    ds.date,
    'polygon' as chain,
    net_change,
    SUM(net_change) OVER (ORDER BY ds.date ASC) as supply_native
FROM date_spine ds
LEFT JOIN daily_net_change USING(date)
