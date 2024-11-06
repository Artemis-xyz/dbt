{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'REF_FINANCE',
        unique_key = ['date']
    )
}}

select
    t.date,
    p.name,
    p.symbol,
    f.fees,
    r.revenue,
    t.tvl
from
    {{ source('DEFILLAMA', 'fact_defillama_protocol_tvls') }} t
left join {{ source('DEFILLAMA', 'fact_defillama_protocol_revenue') }} r on t.date = r.date and t.defillama_protocol_id = r.defillama_protocol_id
left join {{ source('DEFILLAMA', 'fact_defillama_protocol_fees') }} f on f.date = t.date and f.defillama_protocol_id = t.defillama_protocol_id
join {{ source('DEFILLAMA', 'fact_defillama_protocols') }} p on t.defillama_protocol_id = p.id
    and name = 'Ref Finance'

