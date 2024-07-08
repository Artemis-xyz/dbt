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
            sum(fees_tia) as fees_native,
            sum(fees) as fees,
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
                            ref("fact_celestia_mints_silver"),
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
    price_data as ({{ get_coingecko_metrics("celestia") }})

select
    fundamental_data.date,
    fundamental_data.chain,
    coalesce(txns, 0) as txns,
    coalesce(fees_native, 0) as fees_native,
    coalesce(fees, 0) as fees,
    coalesce(mints, 0) as mints,
    coalesce(unique_namespaces, 0) as submitters,
    blob_size_mib,
    coalesce(fees_for_blobs_native, 0) * price as blob_fees,
    coalesce(fees_for_blobs_native, 0) as blob_fees_native,
    blob_size_mib / 86400 as avg_mib_per_second,
    coalesce(blob_fees_native / blob_size_mib, 0) as avg_cost_per_mib_native,
    coalesce(blob_fees / blob_size_mib, 0) as avg_cost_per_mib,
    price,
    market_cap,
    fdmc
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
