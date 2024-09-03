{{
    config(
        materialized="table",
        snowflake_warehouse="defillama",
        database="defillama",
        schema="core",
        alias="ez_defillama_metrics_by_protocols",
    )
}}

select
    date,
    name,
    dex_volumes,
    tvl
from {{ref("agg_defillama_protocol_fees_rev_tvl_dex_vol")}}
where date is not null
