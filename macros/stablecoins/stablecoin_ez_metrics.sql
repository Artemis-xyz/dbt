{% macro get_stablecoin_ez_metrics(symbol) %}

select 
    '{{ symbol }}' as symbol
    , date
    , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
    , sum(artemis_stablecoin_transfer_volume) as deduped_stablecoin_transfer_volume
    , count(distinct from_address) as stablecoin_dau
    , sum(stablecoin_daily_txns) as stablecoin_txns
    , sum(stablecoin_supply) as stablecoin_total_supply
from {{ ref("agg_daily_stablecoin_breakdown_silver") }}
where lower(symbol) = lower('{{ symbol }}')
{% if is_incremental() %}
    and date >= (select dateadd('day', -3, max(date)) from {{ this }}) 
{% endif %}
group by date

{% endmacro %}
