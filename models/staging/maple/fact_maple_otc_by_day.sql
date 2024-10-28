{{ 
    config(
        materialized="table",
        snowflake_warehouse="MAPLE",
    )
}}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_maple_otc_by_day") }}
    )
select
    value:date::date as date,
    value:timestamp::int as timestamp,
    value:alt_collat_btc::float as alt_collat_btc,
    value:alt_deposits_btc::float as alt_deposits_btc,
    value:alt_deposits_sol::float as alt_deposits_sol,
    value:alt_loans_btc::float as alt_loans_btc,
    value:alt_loans_inj::float as alt_loans_inj,
    value:alt_loans_sol::float as alt_loans_sol,
    value:bc_collat_btc::float as bc_collat_btc,
    value:bc_collat_steth::float as bc_collat_steth,
    value:hy_collat_btc::float as hy_collat_btc,
    value:hy_collat_eth::float as hy_collat_eth,
    value:hy_collat_sol::float as hy_collat_sol,
    value:syrup_usdc_collat_btc::float as syrup_usdc_collat_btc,
    value:syrup_usdc_collat_eth::float as syrup_usdc_collat_eth,
    value:syrup_usdc_collat_orca::float as syrup_usdc_collat_orca,
    value:syrup_usdc_collat_pt::float as syrup_usdc_collat_pt,
    value:syrup_usdc_collat_sol::float as syrup_usdc_collat_sol,
    value:syrup_usdt_collat_btc::float as syrup_usdt_collat_btc,
    value:syrup_usdt_collat_eth::float as syrup_usdt_collat_eth,
    value:syrup_usdt_collat_pt::float as syrup_usdt_collat_pt,
    value:syrup_usdt_collat_sol::float as syrup_usdt_collat_sol
from
    {{ source("PROD_LANDING", "raw_maple_otc_by_day") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
