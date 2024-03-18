{{ config(snowflake_warehouse="ETHEREUM_XS", materialized="table") }}
with
    taggeded_censoring_builders as (
        select
            trunc(block_timestamp, 'day') as date,
            chain,
            builder,
            builder_name,
            censors,
            count(hash) as blocks_produced
        from {{ ref("fact_ethereum_blocks") }}
        group by date, chain, builder, builder_name, censors
    )
select date, 'ethereum' as chain, builder, builder_name, censors, blocks_produced
from taggeded_censoring_builders
where date < to_date(sysdate())
order by blocks_produced desc
