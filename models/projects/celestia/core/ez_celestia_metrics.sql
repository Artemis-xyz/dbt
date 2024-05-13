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
            sum(total_blob_size_mb) as blob_size_mb,
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
    coalesce(unique_namespaces, 0) as unique_namespaces,
    coalesce(blob_size_mb, 0) as blob_size_mb,
    coalesce(fees_for_blobs_native, 0) as fees_for_blobs_native,
    coalesce(fees_for_blobs_native, 0) * price as fees_for_blobs,
    price,
    market_cap,
    fdmc
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
