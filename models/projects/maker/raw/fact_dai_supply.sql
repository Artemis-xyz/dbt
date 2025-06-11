{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_dai_supply"
    )
}}



SELECT date, SUM(dai_supply) as outstanding_supply FROM {{ ref('fact_dai_supply_by_chain') }}
GROUP BY 1