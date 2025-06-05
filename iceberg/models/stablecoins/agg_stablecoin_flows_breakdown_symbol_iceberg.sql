{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="STABLECOINS",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="agg_stablecoin_flows_breakdown_symbol",
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'symbol',
        }) }}"
    )
}}

select
    *
from PC_DBT_DB.PROD.agg_stablecoin_flows_breakdown_symbol
order by symbol
    