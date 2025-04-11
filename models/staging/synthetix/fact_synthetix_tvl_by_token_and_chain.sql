{{
    config(
        materialized='table',
        snowflake_warehouse='SYNTHETIX',
        database='SYNTHETIX',
        schema='raw',
        alias='fact_synthetix_tvl_by_chain_and_token'
    )
}}

with all_tvl_by_chain_and_token as (
    SELECT * FROM {{ ref('fact_synthetix_ethereum_tvl') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_synthetix_optimism_tvl') }}
)

select
    date
    , contract_address
    , address
    , balance_raw
    , balance_native
    , price
    , balance
    , artemis_application_id
    , type
    , chain
    , unique_id
from all_tvl_by_chain_and_token