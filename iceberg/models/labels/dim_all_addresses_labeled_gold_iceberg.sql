{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="LABELS",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="dim_all_addresses_labeled_gold",
        partition_by=["chain"],
        post_hook = merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'chain, artemis_application_id, address'
        })
    )
}}

SELECT
    * EXCLUDE (last_updated),
    last_updated::TIMESTAMP_NTZ(6) AS last_updated
FROM PC_DBT_DB.PROD.DIM_ALL_ADDRESSES_LABELED_GOLD
ORDER BY chain, artemis_application_id