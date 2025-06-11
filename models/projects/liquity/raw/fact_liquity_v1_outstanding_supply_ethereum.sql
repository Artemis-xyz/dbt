{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_outstanding_supply_ethereum'
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
        ethereum_flipside.core.ez_token_transfers
    WHERE 1 = 1 
        AND contract_address = lower('0x5f98805a4e8be255a32880fdec7f6728c6568ba0')
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
    'ethereum' as chain,
    'LUSD' as token,
    SUM(lusdSupplyChange) OVER (ORDER BY date asc) as outstanding_supply
FROM
    all_dates