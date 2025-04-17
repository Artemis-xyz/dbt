{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_liquidation_expenses"
    )
}}

SELECT block_timestamp
     , tx_hash
     , SUM(tab) AS value
FROM {{ref('fact_vow_fess')}}
GROUP BY block_timestamp
    , tx_hash