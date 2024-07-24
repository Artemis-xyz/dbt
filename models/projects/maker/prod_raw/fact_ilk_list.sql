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
    -- Note: In the future, add a condition for call_success when available

    UNION

    SELECT ilk
    FROM {{ ref('fact_spot_file') }}
    -- Note: In the future, add a condition for call_success when available

    UNION

    SELECT ilk
    FROM {{ ref('fact_jug_file') }}
)