{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="dim_psms"
    )
}}

SELECT DISTINCT
    u_address as psm_address,
    ilk
FROM ethereum_flipside.maker.fact_vat_frob
WHERE ilk LIKE 'PSM-%'