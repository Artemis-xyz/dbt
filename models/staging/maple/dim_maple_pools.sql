{{ 
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
    )
}}


-- Creates a table consisting of each Maple Pool with its ID, name, activation block, Loan Managers, etc.
-- For Pools that were migrated from V1->V2, also includes the V1 contract address so we can merge all historical data for those Pools
-- Each Pool is denominated in a single asset, and we store that asset's on-chain decimal precision here (e.g. WETH is 18, USDC is 6)

-- Source: 

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_maple_dim_pools") }}
    )
select
    value:pool_id::string as pool_id,
    value:pool_name::string as pool_name,
    value:loan_manager::string as loan_manager,
    value:open_term_loan_manager::string as open_term_loan_manager,
    value:v1_pool_id::string as v1_pool_id,
    value:asset::string as asset,
    value:precision::int as precision,
    value:block_activated::int as block_activated
from
    {{ source("PROD_LANDING", "raw_maple_dim_pools") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)