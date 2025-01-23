{% macro get_defillama_protocol_tvl(defillama_name) %}

select 
    t.date, 
    tvl,
    p.name
from pc_dbt_db.prod.fact_defillama_protocol_tvls t
join pc_dbt_db.prod.fact_defillama_protocols p 
    on p.id = t.defillama_protocol_id 
    and p.name ilike '%{{ defillama_name }}%'
order by t.date desc

{% endmacro %}