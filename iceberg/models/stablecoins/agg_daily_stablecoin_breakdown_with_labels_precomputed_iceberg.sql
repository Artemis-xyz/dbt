{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="STABLECOINS",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="agg_daily_stablecoin_breakdown_with_labels_precomputed",
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'parent, value',
        }) }}"
    )
}}

select
    * EXCLUDE(path),
    to_json(path) AS path
from PC_DBT_DB.PROD.agg_daily_stablecoin_breakdown_with_labels_precomputed
order by parent, value


