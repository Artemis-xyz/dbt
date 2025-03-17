{% macro extract_dune_dex_volumes(chain) %}
    select 
        block_date::date as date,
        sum(amount_usd) as daily_volume
    from {{ source("DUNE_DEX_VOLUMES", "trades")}}
    where blockchain = '{{chain}}'
    group by date
    order by date asc
{% endmacro %}
