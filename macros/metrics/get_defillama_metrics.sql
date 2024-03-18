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
