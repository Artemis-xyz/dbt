{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="fact_arbitrum_burns_mints",
    )
}}

WITH date_spine AS (
    SELECT * FROM {{ ref("dim_date_spine") }}
    WHERE date < to_date(sysdate()) AND date >= (SELECT MIN(date) FROM {{ ref("fact_arbitrum_all_supply_events") }})
), 

mints_and_burns AS (
    SELECT
        DATE(block_timestamp) AS date, 
        tx_hash, 
        from_address, 
        to_address, 
        CASE
            WHEN LOWER(to_address) = LOWER('0x0000000000000000000000000000000000000000') THEN amount
            ELSE 0
        END AS burns_native, 
        CASE
            WHEN LOWER(to_address) = LOWER('0x0000000000000000000000000000000000000000') THEN amount_usd
            ELSE 0
        END AS burns,
        CASE
            WHEN LOWER(from_address) = LOWER('0x0000000000000000000000000000000000000000') THEN amount
            ELSE 0
        END AS mints_native, 
        CASE
            WHEN LOWER(from_address) = LOWER('0x0000000000000000000000000000000000000000') THEN amount_usd
            ELSE 0
        END AS mints, 
    FROM arbitrum_flipside.core.ez_token_transfers t
    WHERE LOWER(contract_address) = LOWER('0x912ce59144191c1204e64559fe8253a0e49e6548') 
        AND (LOWER(to_address) = LOWER('0x0000000000000000000000000000000000000000') 
            OR LOWER(from_address) = LOWER('0x0000000000000000000000000000000000000000')
        )
        AND LOWER(tx_hash) != LOWER('0x9cdbb4672b549c26d97cac29f9cd73c1951656e0622ba4b9ed0abff2ee58698d') -- Excluding TGE 
)

SELECT
    ds.date,
    m.tx_hash,
    m.from_address,
    m.to_address,
    m.burns_native,
    m.burns,
    m.mints_native,
    m.mints, 
    SUM(coalesce(m.burns_native, 0)) OVER (ORDER BY ds.date ROWS UNBOUNDED PRECEDING) AS cumulative_burns_native,
    SUM(coalesce(m.burns, 0)) OVER (ORDER BY ds.date ROWS UNBOUNDED PRECEDING) AS cumulative_burns,
    SUM(coalesce(m.mints_native, 0)) OVER (ORDER BY ds.date ROWS UNBOUNDED PRECEDING) AS cumulative_mints_native,
    SUM(coalesce(m.mints, 0)) OVER (ORDER BY ds.date ROWS UNBOUNDED PRECEDING) AS cumulative_mints
FROM date_spine ds
LEFT JOIN mints_and_burns m
    ON ds.date = m.date

