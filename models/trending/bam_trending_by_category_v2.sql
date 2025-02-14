{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE") }}

with
    trending_category as (
        select
            sum(txns) txns,
            sum(gas) gas,
            sum(gas_usd) gas_usd,
            sum(dau) dau,
            category,
            chain,
            granularity,
            sum(prev_txns) prev_txns,
            sum(prev_dau) prev_dau,
            sum(prev_gas) prev_gas,
            sum(prev_gas_usd) prev_gas_usd
        from {{ ref("bam_trending_data_v2") }}
        where category is not null
        group by category, chain, granularity
    )
select
    category,
    chain,
    granularity,
    txns,
    gas,
    gas_usd,
    dau,
    prev_dau,
    prev_txns,
    prev_gas,
    prev_gas_usd,
    case
        when prev_txns is not null and prev_txns <> 0
        then ((txns - prev_txns) * 100) / prev_txns
        else null
    end as txns_change,
    case
        when prev_dau is not null and prev_dau <> 0
        then ((dau - prev_dau) * 100) / prev_dau
        else null
    end as dau_change,
    case
        when prev_gas is not null and prev_gas <> 0
        then ((gas - prev_gas) * 100) / prev_gas
        else null
    end as gas_change,
    case
        when prev_gas_usd is not null and prev_gas_usd <> 0
        then ((gas_usd - prev_gas_usd) * 100) / prev_gas_usd
        else null
    end as gas_usd_change
from trending_category
