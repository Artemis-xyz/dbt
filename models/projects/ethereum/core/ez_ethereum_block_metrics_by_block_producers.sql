-- depends_on {{ ref("fact_ethereum_block_producers_silver") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM_XS",
        database="ethereum",
        schema="core",
        alias="ez_block_metrics_by_block_producers",
    )
}}

select date, chain, builder, builder_name, censors, blocks_produced
from {{ ref("fact_ethereum_block_producers_silver") }}
