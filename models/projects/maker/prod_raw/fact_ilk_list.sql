{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_ilk_list"
    )
}}

SELECT DISTINCT ilk
FROM (
    SELECT ilk
    FROM ethereum_flipside.maker.fact_vat_frob

    UNION

    SELECT ilk
    FROM {{ ref('fact_spot_file') }}

    UNION

    SELECT ilk
    FROM {{ ref('fact_jug_file') }}
)