-- depends_on {{ ref("ez_blast_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="blast",
        database="blast",
        schema="core",
        alias="ez_metrics"
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("blast") }}),
    defillama_data as ({{ get_defillama_metrics("blast") }}),
    contract_data as ({{ get_contract_metrics("blast") }}),
    -- NOTE, this says l1 data cost, but that's inaccurate
    -- its both data and execution cost, but I'm following convention for now and we don't publish 
    -- this field anywhere, we only use it to derive revenue
    expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost
        from {{ ref("fact_blast_l1_data_cost") }}
    ),  -- supply side revenue and fees
    rolling_metrics as ({{ get_rolling_active_address_metrics("blast") }}),
    blast_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_blast_daily_dex_volumes") }}
    )
select
    coalesce(
        fundamental_data.date,
        defillama_data.date,
        contract_data.date,
        expenses_data.date
    ) as date,
    'blast' as chain,
    txns,
    dau,
    wau,
    mau,
    fees_native, 
    fees,
    fees / txns as avg_txn_fee,
    median_txn_fee,
    l1_data_cost_native,  
    l1_data_cost,
    coalesce(fees_native, 0) - l1_data_cost_native as revenue_native,  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    coalesce(fees, 0) - l1_data_cost as revenue,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    dau_over_100,
    tvl,
    dune_dex_volumes_blast.dex_volumes as dex_volumes,
    weekly_contracts_deployed,
    weekly_contract_deployers
from fundamental_data
left join defillama_data on fundamental_data.date = defillama_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join blast_dex_volumes as dune_dex_volumes_blast on fundamental_data.date = dune_dex_volumes_blast.date
where fundamental_data.date < to_date(sysdate())
