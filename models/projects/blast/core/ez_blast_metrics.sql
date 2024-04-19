-- depends_on {{ ref("ez_blast_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="blast",
        database="blast",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("blast") }}),
    defillama_data as ({{ get_defillama_metrics("blast") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("blast") }}),
    contract_data as ({{ get_contract_metrics("blast") }}),
    -- NOTE, this says l1 data cost, but that's inaccurate
    -- its both data and execution cost, but I'm following convention for now and we don't publish 
    -- this field anywhere, we only use it to derive revenue
    expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost, revenue_native, revenue
        from {{ ref("agg_daily_blast_revenue") }}
    )
select
    coalesce(
        fundamental_data.date,
        defillama_data.date,
        stablecoin_data.date,
        contract_data.date,
        expenses_data.date,
    ) as date,
    'blast' as chain,
    txns,
    dau,
    fees_native, 
    fees,
    l1_data_cost_native,  
    l1_data_cost,
    revenue_native,  
    revenue,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    dau_over_100,
    tvl,
    dex_volumes,
    weekly_contracts_deployed,
    weekly_contract_deployers,
    stablecoin_total_supply,
    stablecoin_txns,
    stablecoin_dau,
    stablecoin_transfer_volume
from fundamental_data
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
where fundamental_data.date < to_date(sysdate())
