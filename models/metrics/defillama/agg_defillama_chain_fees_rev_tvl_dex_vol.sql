{{ config(materialized="view") }}
-- Create a combined table of all Defillama chain metrics in preparation for
-- joining with datahub.
with
    dex_vol_table as (
        select date, defillama_chain_name, dex_volumes
        from {{ ref("fact_defillama_chain_dex_volumes") }}
    ),
    tvl_table as (
        select date, defillama_chain_name, tvl
        from {{ ref("fact_defillama_chain_tvls") }}
    ),
    combined_data as (
        select
            coalesce(dex_vol_table.date, tvl_table.date) as date,
            coalesce(
                dex_vol_table.defillama_chain_name, tvl_table.defillama_chain_name
            ) as defillama_chain_name,
            dex_vol_table.dex_volumes,
            tvl_table.tvl
        from dex_vol_table
        full outer join
            tvl_table
            on dex_vol_table.date = tvl_table.date
            and dex_vol_table.defillama_chain_name = tvl_table.defillama_chain_name
    )

select
    combined_data.date,
    combined_data.defillama_chain_name,
    combined_data.dex_volumes,
    combined_data.tvl
from combined_data
where date is not null
