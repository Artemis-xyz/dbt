{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="STABLECOINS",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="agg_weekly_stablecoin_breakdown_datahub",
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': '_dbt_source_relation',
        }) }}"
    )
}}

SELECT
    *
FROM PC_DBT_DB.PROD.agg_weekly_stablecoin_breakdown_datahub
ORDER BY _dbt_source_relation