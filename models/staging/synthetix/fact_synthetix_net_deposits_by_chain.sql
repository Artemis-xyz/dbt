{{
    config(
        materialized='table',
        snowflake_warehouse='SYNTHETIX',
        database='SYNTHETIX',
        schema='raw',
        alias='fact_synthetix_net_deposits_by_chain'
    )
}}

with all_net_deposits_by_chain as (
    SELECT * FROM {{ ref('fact_synthetix_ethereum_net_deposits') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_synthetix_optimism_net_deposits') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_synthetix_base_net_deposits') }}
)

select
    date,
    chain,
    net_deposits
from all_net_deposits_by_chain