{{
    config(
        materialized="incremental",
        snowflake_warehouse="CELESTIA",
        database="celestia",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

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
    ,'celestia' as artemis_id

    --Market Data
    , coalesce(price, 0) as price
    , coalesce(market_cap, 0) as mc
    , coalesce(fdmc, 0) as fdmc
    , coalesce(token_volume, 0) as token_volume

    --Usage Data
    , coalesce(unique_namespaces, 0) as da_dau
    , coalesce(unique_namespaces, 0) as dau
    , coalesce(txns, 0) as da_txns
    , coalesce(txns, 0) as txns
    , coalesce(fees, 0) / coalesce(txns, 1) as da_avg_txn_fee
    , coalesce(fees, 0) / coalesce(txns, 1) as avg_txn_fee

    --Fee Data
    , coalesce(fees_native, 0) as chain_fees_native
    , coalesce(fees_for_blobs_native, 0) as blob_fees_native
    , coalesce(blob_fees_native, 0) + coalesce(chain_fees_native, 0) as fees_native
    , coalesce(fees_for_blobs_native, 0) * coalesce(price, 0) as blob_fees
    , coalesce(fees, 0) as chain_fees
    , coalesce(fees, 0) + coalesce(blob_fees, 0) as fees

    --Fee Allocation
    , coalesce(blob_fees_native, 0) + coalesce(chain_fees_native, 0) as validator_fee_allocation_native
    , coalesce(fees, 0) + coalesce(blob_fees, 0) as validator_fee_allocation

    -- Supply Data
    , coalesce(premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(mints, 0) as gross_emissions_native
    , coalesce(circulating_supply_native, 0) as circulating_supply_native

    --Token Turnover/Other Data
    , coalesce(token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(token_turnover_fdv, 0) as token_turnover_fdv

     --Bespoke Data
    , coalesce(blob_size_mib, 0) as blob_size_mib
    , coalesce(blob_size_mib / 86400, 0) as avg_mib_per_second
    , coalesce(fees_for_blobs_native / blob_size_mib, 0) as avg_cost_per_mib_native
    , coalesce(fees_for_blobs_native * price / blob_size_mib, 0) as avg_cost_per_mib
    , coalesce(unique_namespaces, 0) as submitters

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join supply_data on fundamental_data.date = supply_data.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())