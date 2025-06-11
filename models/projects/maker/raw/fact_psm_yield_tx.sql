{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_psm_yield_tx"
    )
}}

SELECT DISTINCT
    tx_hash,
    CASE
        WHEN usr = '0xf2e7a5b83525c3017383deed19bb05fe34a62c27'
        THEN 'PSM-GUSD-A'
        WHEN usr = lower('0x8bF8b5C58bb57Ee9C97D0FEA773eeE042B10a787')
        THEN 'PSM-USDP-A'
    END AS ilk
FROM {{ ref('fact_dai_burn') }}
WHERE usr IN ('0xf2e7a5b83525c3017383deed19bb05fe34a62c27', lower('0x8bF8b5C58bb57Ee9C97D0FEA773eeE042B10a787')) -- GUSD interest payment contract