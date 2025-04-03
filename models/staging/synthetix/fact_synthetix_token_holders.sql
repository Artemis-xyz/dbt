{{
    config(
        materialized='table',
        snowflake_warehouse='SYNTHETIX',
        database='synthetix',
        schema='raw',
        alias='fact_synthetix_token_holders'
    )
}}

select * from {{ ref('fact_synthetix_ethereum_token_holders') }}
union all
select * from {{ ref('fact_synthetix_optimism_token_holders') }}
union all
select * from {{ ref('fact_synthetix_base_token_holders') }}

