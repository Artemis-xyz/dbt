-- depends_on: {{ source("PROD_LANDING", "raw_eigenpie_restaked_eth_count") }}
{{ config(materialized="table") }}
SELECT
    a.value:date::date as date,
    a.value:mLRT_address::string as contract_address,
    a.value:total_supply::number as amount_native,
    extraction_date,
    ROW_NUMBER() OVER (PARTITION BY date, contract_address ORDER BY date DESC) as rn
FROM
    {{ source("PROD_LANDING", "raw_eigenpie_restaked_eth_count") }},
    LATERAL FLATTEN (input => parse_json(source_json)) a
QUALIFY rn = 1
ORDER BY
    extraction_date DESC, date desc, amount_native desc