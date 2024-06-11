{% macro get_stablecoin_metrics(chain) %}
    select
        chain,
        date,
        sum(total_supply) as stablecoin_total_supply,
        sum(txns) as stablecoin_txns,
        sum(dau) as stablecoin_dau,
        sum(transfer_volume) as stablecoin_transfer_volume,
        sum(deduped_transfer_volume) as deduped_stablecoin_transfer_volume
    from {{ ref("agg_" ~ chain ~ "_stablecoin_metrics") }}
    group by chain, date
{% endmacro %}
