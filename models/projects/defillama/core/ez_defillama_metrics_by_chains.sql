{{
    config(
        materialized="table",
        snowflake_warehouse="defillama",
        database="defillama",
        schema="core",
        alias="ez_defillama_metrics_by_chains",
    )
}}

select
    date,
    defillama_chain_name as chain,
    dex_volumes,
    tvl
from {{ref("agg_defillama_chain_fees_rev_tvl_dex_vol")}}
where date is not null
