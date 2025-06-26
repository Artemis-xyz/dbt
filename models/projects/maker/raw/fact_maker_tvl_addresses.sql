{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_maker_tvl_addresses"
    )
}}


-- address, name, artemis_application_id, chain, is_token, is_fungible, type, last_updated
-- if there is a change here, please update the last_updated pinned timestamp

SELECT
    distinct
    join_address as address,
    'Maker' as name,
    'makerdao' as artemis_application_id,
    'ethereum' as chain,
    false as is_token,
    false as is_fungible,
    'lending_pool' as type,
    TO_TIMESTAMP_NTZ('2025-06-17 16:00:00') as last_updated
from {{ ref('dim_gem_join_addresses') }}

UNION ALL

SELECT
    lower('0x37305B1cD40574E4C5Ce33f8e8306Be057fD7341') as address,
    'Maker' as name,
    'makerdao' as artemis_application_id,
    'ethereum' as chain,
    false as is_token,
    false as is_fungible,
    'lending_pool' as type,
    TO_TIMESTAMP_NTZ('2025-06-17 16:00:00') as last_updated