{% macro extract_dune_dex_volumes(chain) %}
    select 
        date_trunc('day',block_date) as date, 
        sum(amount_usd) as daily_volume
    from zksync_dune.dex.trades
    where blockchain = '{{chain}}'
    group by date
    order by date asc
{% endmacro %}
