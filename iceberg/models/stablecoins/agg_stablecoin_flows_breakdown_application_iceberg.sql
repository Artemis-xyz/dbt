{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="STABLECOINS",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="agg_stablecoin_flows_breakdown_application",
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'application',
        }) }}"
    )
}}

select
    *
from PC_DBT_DB.PROD.agg_stablecoin_flows_breakdown_application
order by application
    