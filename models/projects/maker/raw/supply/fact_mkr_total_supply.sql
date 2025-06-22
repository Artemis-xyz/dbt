{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
     )
}}

with a as (
    SELECT 
        block_timestamp::date as date,
        sum(CASE WHEN
            event_name = 'Burn'
        THEN coalesce(decoded_log:wad/1e18,0)END)  as mkr_burned,
        sum(CASE WHEN
            event_name = 'Mint'
        THEN coalesce(decoded_log:wad/1e18,0)END)  as mkr_minted
    FROM ethereum_flipside.core.ez_decoded_event_logs
    WHERE contract_address = lower('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2')
    AND event_name in ('Burn', 'Mint')
    GROUP BY 1
)
, date_spine as (
    SELECT
        date
    FROM pc_dbt_db.prod.dim_date_spine
    WHERE date between (SELECT MIN(date) FROM a) AND to_date(sysdate())
)
, sparse as (
    SELECT
        ds.date,
        coalesce(mkr_minted, 0) as mkr_minted,
        coalesce(mkr_burned, 0) as mkr_burned
    FROM date_spine ds
    LEFT JOIN a using(date)
)
SELECT
    date,
    mkr_minted,
    mkr_burned,
    SUM(mkr_minted - mkr_burned) OVER (ORDER BY date ASC) as total_supply_mkr
FROM
    sparse