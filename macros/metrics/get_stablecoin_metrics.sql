{% macro get_stablecoin_metrics(identifier, breakdown="chain") %}
with mau as (
    select
        date_granularity as date
        , '{{ identifier }}' as {{breakdown}}
        , stablecoin_dau as stablecoin_mau
        , artemis_stablecoin_dau as artemis_stablecoin_mau
        , p2p_stablecoin_dau as p2p_stablecoin_mau
    from {{ ref("agg_monthly_stablecoin_breakdown_" ~ breakdown) }}
    {% if breakdown == "chain" %}
        where lower(chain) = lower('{{ identifier }}')
    {% elif breakdown == "symbol" %}
        where lower(symbol) = lower('{{ identifier }}')
    {% endif %}
)
, daily_metrics as (
    select 
        date_granularity as date
        , '{{ identifier }}' as {{ breakdown }}
        
        , stablecoin_transfer_volume as stablecoin_transfer_volume
        , stablecoin_daily_txns as stablecoin_txns
        , stablecoin_dau 

        , artemis_stablecoin_transfer_volume
        , artemis_stablecoin_daily_txns as artemis_stablecoin_txns
        , artemis_stablecoin_dau

        , p2p_stablecoin_transfer_volume
        , p2p_stablecoin_daily_txns as p2p_stablecoin_txns
        , p2p_stablecoin_dau

        , p2p_stablecoin_supply as p2p_stablecoin_total_supply
        , stablecoin_supply as stablecoin_total_supply
    from {{ ref("agg_daily_stablecoin_breakdown_" ~ breakdown) }}
    {% if breakdown == "chain" %}
        where lower(chain) = lower('{{ identifier }}')
    {% elif breakdown == "symbol" %}
        where lower(symbol) = lower('{{ identifier }}')
    {% endif %}
)
select
    daily_metrics.date
    , daily_metrics.{{breakdown}} as {{ breakdown }}
    , stablecoin_transfer_volume
    , stablecoin_txns
    , stablecoin_dau
    , stablecoin_mau
    , artemis_stablecoin_transfer_volume
    , artemis_stablecoin_txns
    , artemis_stablecoin_dau
    , artemis_stablecoin_mau
    , p2p_stablecoin_transfer_volume
    , p2p_stablecoin_txns
    , p2p_stablecoin_dau
    , p2p_stablecoin_mau
    , p2p_stablecoin_total_supply
    , stablecoin_total_supply
from daily_metrics
left join mau on daily_metrics.date = mau.date
{% endmacro %}