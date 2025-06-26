{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_outstanding_supply_polygon'
    )
}}

WITH c AS (
    SELECT
        block_timestamp::date as date,
        SUM(
            CASE
                from_address
                WHEN '0x0000000000000000000000000000000000000000' THEN raw_amount_precise::number / 1e18
                ELSE -(raw_amount_precise::number / 1e18)
            END
        ) AS lusdSupplyChange
    FROM
        polygon_flipside.core.ez_token_transfers
    WHERE 1 = 1 
        AND contract_address = lower('0x23001f892c0C82b79303EDC9B9033cD190BB21c7')
        AND(
            from_address = '0x0000000000000000000000000000000000000000'
            OR to_address = '0x0000000000000000000000000000000000000000'
            )
    GROUP BY
        1
)
, date_spine as (
    SELECT
        date
    FROM
        pc_dbt_db.prod.dim_date_spine
    WHERE date between (select min(date) from c) and to_date(sysdate())
)
, all_dates as (
    SELECT
        ds.date,
        lusdSupplyChange
    FROM
        date_spine ds
    LEFT JOIN c using(date)
    ORDER BY
        1
)
SELECT
    date,
    'polygon' as chain,
    'LUSD' as token,
    SUM(lusdSupplyChange) OVER (ORDER BY date asc) as outstanding_supply
FROM
    all_dates