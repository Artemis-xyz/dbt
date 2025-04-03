{{
    config(
        materialized='table',
        snowflake_warehouse='SYNTHETIX',
        database='SYNTHETIX',
        schema='raw',
        alias='fact_synthetix_token_incentives_by_chain'
    )
}}

with all_token_incentives_by_chain as (
    SELECT * FROM {{ ref('fact_synthetix_ethereum_token_incentives') }}
)

select
    date,
    chain,
    token_incentives
from all_token_incentives_by_chain