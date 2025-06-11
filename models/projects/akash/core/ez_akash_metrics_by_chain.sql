{{
    config(
        materialized="table",
        snowflake_warehouse="AKASH",
        database="akash",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

SELECT
    date
    , 'akash' AS chain
    , active_leases
    , active_providers
    , new_leases
    , compute_fees
    , gas_fees
    , ecosystem_revenue
    , validator_fee_allocation
    , treasury_fee_allocation
    , service_fee_allocation
    , revenue
    , burns_native
    , gross_emissions_native
    , price
    , market_cap
    , fdmc
    , token_turnover_fdv
    , token_volume
FROM
    {{ ref("ez_akash_metrics") }}
