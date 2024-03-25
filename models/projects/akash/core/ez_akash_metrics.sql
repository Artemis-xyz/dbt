{{
    config(
        materialized="table",
        snowflake_warehouse="akash",
        database="akash",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    active_providers as (select * from {{ ref("fact_akash_active_providers_silver") }}),
    active_leases as (select * from {{ ref("fact_akash_active_leases_silver") }}),
    new_leases as (select * from {{ ref("fact_akash_new_leases_silver") }}),
    compute_fees_native as (
        select * from {{ ref("fact_akash_compute_fees_native_silver") }}
    ),
    compute_fees_usdc as (
        select * from {{ ref("fact_akash_compute_fees_usdc_silver") }}
    ),
    compute_fees_total_usd as (
        select * from {{ ref("fact_akash_compute_fees_total_usd_silver") }}
    ),
    validator_fees_native as (
        select * from {{ ref("fact_akash_validator_fees_native_silver") }}
    ),
    validator_fees as (select * from {{ ref("fact_akash_validator_fees_silver") }}),
    total_fees as (select * from {{ ref("fact_akash_total_fees_silver") }}),
    revenue_native as (select * from {{ ref("fact_akash_burns_native_silver") }})

select
    active_leases.date,
    'akash' as chain,
    coalesce(active_leases.active_leases, 0) as active_leases,
    coalesce(active_providers.active_providers, 0) as active_providers,
    coalesce(new_leases.new_leases, 0) as new_leases,
    (coalesce(compute_fees_native.compute_fees_native, 0)) / 1e6 as compute_fees_native,
    (coalesce(compute_fees_usdc.compute_fees_usdc, 0)) / 1e6 as compute_fees_usdc,
    (coalesce(compute_fees_total_usd.compute_fees_total_usd, 0))
    / 1e6 as compute_fees_total_usd,
    coalesce(validator_fees_native.validator_fees_native, 0) as validator_fees_native,
    coalesce(validator_fees.validator_fees, 0) as validator_fees,
    coalesce(total_fees.total_fees, 0) as total_fees,
    coalesce(revenue_native.revenue_native, 0) as revenue_native
from active_leases
full join active_providers on active_leases.date = active_providers.date
full join new_leases on active_leases.date = new_leases.date
full join compute_fees_native on active_leases.date = compute_fees_native.date
full join compute_fees_usdc on active_leases.date = compute_fees_usdc.date
full join compute_fees_total_usd on active_leases.date = compute_fees_total_usd.date
full join validator_fees_native on active_leases.date = validator_fees_native.date
full join validator_fees on active_leases.date = validator_fees.date
full join total_fees on active_leases.date = total_fees.date
full join revenue_native on active_leases.date = revenue_native.date
where active_leases.date < to_date(sysdate())
order by date desc
