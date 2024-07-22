{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="dim_psm_yield_tx"
    )
}}

SELECT DISTINCT
    tx_hash
    , CASE WHEN usr = '0xf2e7a5b83525c3017383deed19bb05fe34a62c27'
        THEN 'PSM-GUSD-A'
    END AS ilk
FROM {{ ref('fact_dai_burn') }}
    WHERE usr IN ('0xf2e7a5b83525c3017383deed19bb05fe34a62c27')