{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_maker_tvl"
    )
}}

SELECT
    date,
    sum(total_amount_usd) as tvl_usd
FROM
    {{ref('fact_maker_tvl_by_asset')}}
GROUP BY
    1