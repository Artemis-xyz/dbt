{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'REF_FINANCE',
        unique_key = ['date']
    )
}}

with defillama_tvl as (
    {{ get_defillama_protocol_tvl('ref finance') }}
)
select
    t.date,
    p.name,
    p.symbol,
    f.fees,
    r.revenue,
    t.tvl
from
    defillama_tvl t
join {{ source('DEFILLAMA', 'fact_defillama_protocols') }} p on p.name = 'Ref Finance'
left join {{ source('DEFILLAMA', 'fact_defillama_protocol_revenue') }} r on t.date = r.date and p.id = r.defillama_protocol_id
left join {{ source('DEFILLAMA', 'fact_defillama_protocol_fees') }} f on f.date = t.date and p.id = f.defillama_protocol_id

