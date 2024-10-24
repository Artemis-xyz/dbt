{{ config(materialized="table") }}
-- Create a combined table of all Defillama protocol metrics in preparation for
-- joining with datahub.
with
    fees_table as (
        select date, defillama_protocol_id, fees
        from {{ ref("fact_defillama_protocol_fees") }}
    ),
    revenue_table as (
        select date, defillama_protocol_id, revenue
        from {{ ref("fact_defillama_protocol_revenue") }}
    ),
    dex_vol_table as (
        select date, defillama_protocol_id, dex_volumes
        from {{ ref("fact_defillama_protocol_dex_volumes") }}
    ),
    tvl_table as (
        select date, defillama_protocol_id, tvl
        from {{ ref("fact_defillama_protocol_tvls") }}
    ),
    combined_data as (
        select
            coalesce(
                fees_table.date, revenue_table.date, dex_vol_table.date, tvl_table.date
            ) as date,
            coalesce(
                fees_table.defillama_protocol_id,
                revenue_table.defillama_protocol_id,
                dex_vol_table.defillama_protocol_id,
                tvl_table.defillama_protocol_id
            ) as defillama_protocol_id,
            fees_table.fees,
            revenue_table.revenue,
            dex_vol_table.dex_volumes,
            tvl_table.tvl
        from fees_table
        full outer join
            revenue_table
            on fees_table.date = revenue_table.date
            and fees_table.defillama_protocol_id = revenue_table.defillama_protocol_id
        full outer join
            dex_vol_table
            on coalesce(fees_table.date, revenue_table.date) = dex_vol_table.date
            and coalesce(
                fees_table.defillama_protocol_id, revenue_table.defillama_protocol_id
            )
            = dex_vol_table.defillama_protocol_id
        full outer join
            tvl_table
            on coalesce(fees_table.date, revenue_table.date, dex_vol_table.date)
            = tvl_table.date
            and coalesce(
                fees_table.defillama_protocol_id,
                revenue_table.defillama_protocol_id,
                dex_vol_table.defillama_protocol_id
            )
            = tvl_table.defillama_protocol_id
    )

-- Add name to make table more readable
select
    combined_data.date,
    combined_data.defillama_protocol_id,
    protocol_names.name,
    max(combined_data.fees) as fees,
    max(combined_data.revenue) as revenue,
    max(combined_data.dex_volumes) as dex_volumes,
    max(combined_data.tvl) as tvl
from combined_data
where defillama_protocol_id is not null
left join
    {{ ref("fact_defillama_protocols") }} as protocol_names
    on combined_data.defillama_protocol_id = protocol_names.id
group by combined_data.date, combined_data.defillama_protocol_id, protocol_names.name
