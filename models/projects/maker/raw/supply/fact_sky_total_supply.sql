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
            to_address ='0x0000000000000000000000000000000000000000'
        THEN coalesce(amount,0)END)  as sky_burned,
        sum(CASE WHEN
            from_address = '0x0000000000000000000000000000000000000000'
        THEN coalesce(amount,0)END)  as sky_minted
    FROM {{ source('ETHEREUM_FLIPSIDE', 'ez_token_transfers')}}
    WHERE contract_address = lower('0x56072C95FAA701256059aa122697B133aDEd9279')
    AND (
        from_address = '0x0000000000000000000000000000000000000000'
        OR to_address ='0x0000000000000000000000000000000000000000'
        )
    GROUP BY 1
)
, date_spine as (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    WHERE date between (SELECT MIN(date) FROM a) AND to_date(sysdate())
)
, sparse as (
    SELECT
        ds.date,
        coalesce(sky_minted, 0) as sky_minted,
        coalesce(sky_burned, 0) as sky_burned
    FROM date_spine ds
    LEFT JOIN a using(date)
)
SELECT
    date,
    sky_minted,
    sky_burned,
    SUM(sky_minted - sky_burned) OVER (ORDER BY date ASC) as total_supply_sky
FROM
    sparse