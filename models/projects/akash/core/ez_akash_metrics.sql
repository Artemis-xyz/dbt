{{
    config(
        materialized="table",
        snowflake_warehouse="AKASH",
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
    revenue as (select * from {{ ref("fact_akash_revenue_silver") }}),
    mints as (select * from {{ref("fact_akash_mints_silver")}}),
    burns as (select * from {{ref("fact_akash_burns_native_silver")}})


select
    mints.date,
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
    coalesce(revenue.revenue, 0) as revenue,
    coalesce(burns.total_burned_native, 0) as total_burned_native,
    coalesce(mints.mints, 0) as mints
from mints
left join active_providers on active_providers.date = mints.date
left join new_leases on  new_leases.date = mints.date
left join compute_fees_native on  compute_fees_native.date = mints.date
left join compute_fees_usdc on  compute_fees_usdc.date = mints.date
left join compute_fees_total_usd on  compute_fees_total_usd.date = mints.date
left join validator_fees_native on  validator_fees_native.date = mints.date
left join validator_fees on  validator_fees.date = mints.date
left join total_fees on  total_fees.date = mints.date
left join revenue on  revenue.date = mints.date
left join burns on  burns.date = mints.date
left join active_leases on active_leases.date =  mints.date
where mints.date < to_date(sysdate())
order by date desc 
