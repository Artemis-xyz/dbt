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
    , price_data.price
    , price_data.market_cap as mc
    , price_data.fdmc
    , price_data.token_volume

    --Usage Data
    , fundamental_data.unique_namespaces as da_dau
    , fundamental_data.unique_namespaces as dau
    , fundamental_data.txns as da_txns
    , fundamental_data.txns as txns
    , fundamental_data.fees / fundamental_data.txns as da_avg_txn_fee
    , fundamental_data.fees / fundamental_data.txns as avg_txn_fee

    --Fee Data
    , fundamental_data.fees_native as chain_fees_native
    , fundamental_data.fees_for_blobs_native as blob_fees_native
    , fundamental_data.fees_for_blobs_native + fundamental_data.fees_native as fees_native
    , fundamental_data.fees_for_blobs_native * price_data.price as blob_fees
    , fundamental_data.fees as chain_fees
    , fundamental_data.fees + blob_fees as fees

    --Fee Allocation
    , fundamental_data.fees_for_blobs_native + fundamental_data.fees_native as validator_fee_allocation_native
    , fundamental_data.fees + blob_fees as validator_fee_allocation

    -- Supply Data
    , coalesce(supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(supply_data.mints_usd, 0) as gross_emissions_native
    , supply_data.circulating_supply_native as circulating_supply_native

    --Token Turnover/Other Data
    , price_data.token_turnover_circulating as token_turnover_circulating
    , price_data.token_turnover_fdv as token_turnover_fdv

     --Bespoke Data
    , fundamental_data.blob_size_mib as blob_size_mib
    , fundamental_data.blob_size_mib / 86400 as avg_mib_per_second
    , fundamental_data.fees_for_blobs_native / fundamental_data.blob_size_mib as avg_cost_per_mib_native
    , fundamental_data.fees_for_blobs_native * price_data.price / fundamental_data.blob_size_mib as avg_cost_per_mib
    , fundamental_data.unique_namespaces as submitters

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join supply_data on fundamental_data.date = supply_data.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())