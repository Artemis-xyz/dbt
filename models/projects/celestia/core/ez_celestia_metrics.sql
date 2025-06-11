{{
    config(
        materialized="table",
        snowflake_warehouse="CELESTIA",
        database="celestia",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date,
            sum(mints) as mints,
            sum(mints_usd) as mints_usd,
            sum(fees_tia) as fees_native,
            sum(fees) as fees,
            sum(da_txns) as da_txns,
            sum(transaction_count) as txns,
            sum(unique_namespaces_count) as unique_namespaces,
            sum(total_blob_size_mb) as blob_size_mib,
            sum(fees_for_blobs_tia) as fees_for_blobs_native,
            'celestia' as chain
        from
            (
                {{
                    dbt_utils.union_relations(
                        relations=[
                            ref("fact_celestia_mints_with_usd"),
                            ref("fact_celestia_fees_for_blobs_silver"),
                            ref("fact_celestia_namespaces_blob_sizes_silver"),
                            ref("fact_celestia_txn_count_and_fees_silver"),
                            ref("fact_celestia_fees_usd")
                        ]
                    )
                }}
            )
        group by 1
    ),
    supply_data as (
        select *
        from {{ ref("fact_celestia_supply_data") }}
    ),
    price_data as ({{ get_coingecko_metrics("celestia") }})

select
    fundamental_data.date
    , fundamental_data.chain
    , coalesce(txns, 0) as txns
    , coalesce(fees_native, 0) as fees_native
    , coalesce(fees, 0) as fees
    , coalesce(unique_namespaces, 0) as submitters
    , coalesce(mints, 0) as gross_emissions_native
    , coalesce(mints_usd, 0) as gross_emissions
    -- Standardized Metrics

    -- Token Metrics
    , coalesce(price, 0) as price
    , coalesce(market_cap, 0) as market_cap
    , coalesce(fdmc, 0) as fdmc
    , coalesce(token_volume, 0) as token_volume

    -- Chain Metrics
    , coalesce(txns, 0) as chain_txns
    , coalesce(unique_namespaces, 0) as da_dau
    , coalesce(txns, 0) as da_txns
    , coalesce(fees, 0) / coalesce(txns, 1) as chain_avg_txn_fee
    , coalesce(blob_size_mib, 0) as blob_size_mib
    , coalesce(blob_size_mib / 86400, 0) as avg_mib_per_second
    , coalesce(fees_for_blobs_native / blob_size_mib, 0) as avg_cost_per_mib_native
    , coalesce(fees_for_blobs_native * price / blob_size_mib, 0) as avg_cost_per_mib

    -- Cash Flow Metrics
    , coalesce(fees_for_blobs_native, 0) as blob_fees_native
    , coalesce(fees_for_blobs_native, 0) * coalesce(price, 0) as blob_fees
    , coalesce(fees, 0) as chain_fees
    , coalesce(fees, 0) + coalesce(blob_fees, 0) as ecosystem_revenue
    , coalesce(fees_native, 0) + coalesce(blob_fees_native, 0) as ecosystem_revenue_native
    , coalesce(ecosystem_revenue, 0) as validator_fee_allocation
    , coalesce(ecosystem_revenue_native, 0) as validator_fee_allocation_native

    -- Supply Metrics
    , coalesce(premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(circulating_supply_native, 0) as circulating_supply_native
    , coalesce(net_supply_change_native, 0) as net_supply_change_native

    -- Turnover Metrics
    , coalesce(token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(token_turnover_fdv, 0) as token_turnover_fdv
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join supply_data on fundamental_data.date = supply_data.date