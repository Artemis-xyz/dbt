{% macro get_defillama_metrics(defillama_chain_name) %}
    select
        coalesce(
            dex.defillama_chain_name, tvl.defillama_chain_name
        ) as defillama_chain_name,
        coalesce(dex.date, tvl.date) as date,
        dex_volumes,
        tvl
    from {{ ref("fact_defillama_chain_dex_volumes") }} as dex
    full join
        {{ ref("fact_defillama_chain_tvls") }} as tvl
        on dex.defillama_chain_name = tvl.defillama_chain_name
        and dex.date = tvl.date
    where
        lower(dex.defillama_chain_name) = lower('{{ defillama_chain_name }}')
        or lower(tvl.defillama_chain_name) = lower('{{ defillama_chain_name }}')
{% endmacro %}


{% macro get_defillama_protocol_metrics(defillama_protocol_name) %}
-- First get the Drift Trade protocol ID once
WITH defillama_protocol AS (
    SELECT id
    FROM {{ref("fact_defillama_protocols")}}
    WHERE lower(name) = lower('{{ defillama_protocol_name }}')
),

defillama_fees AS (
    SELECT 
        date,
        fees
    FROM {{ref("fact_defillama_protocol_fees")}} f
    WHERE f.defillama_protocol_id = (SELECT id FROM defillama_protocol)
),

defillama_revenue AS (
    SELECT 
        date,
        revenue
    FROM {{ref("fact_defillama_protocol_revenue")}} r
    WHERE r.defillama_protocol_id = (SELECT id FROM defillama_protocol)
),
    defillama_tvls as (
        SELECT 
            date,
            tvl
        FROM {{ref("fact_defillama_protocol_tvls")}}
        WHERE defillama_protocol_id = (SELECT id FROM defillama_protocol)
    )

SELECT 
    COALESCE(f.date, r.date) AS date,
    f.fees,
    r.revenue,
    t.tvl
FROM defillama_fees f
FULL OUTER JOIN defillama_revenue r
    ON f.date = r.date
FULL OUTER JOIN defillama_tvls t
    ON f.date = t.date
ORDER BY date DESC
{% endmacro %}
