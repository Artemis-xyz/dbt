
{{ config(materialized='table',
    snowflake_warehouse='LIDO',
    database='lido',
    schema='raw',
    alias='fact_lido_fee_split',
) }}

select
    a.value:date::date as date
    , a.value:value[0]::number/1e5 as treasury_fee_pct
    , a.value:value[1]::number/1e5 as insurance_fee_pct
    , a.value:value[2]::number/1e5 as operators_fee_pct
from
    {{ source("PROD_LANDING", "raw_lido_fee_split")}},
    lateral flatten (input => parse_json(source_json)) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.value:date ORDER BY extraction_date DESC) = 1