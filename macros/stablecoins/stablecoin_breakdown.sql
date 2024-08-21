{% macro stablecoin_breakdown(breakdowns=[], granularity='day') %}
select
    date_trunc('{{granularity}}', date) as date_granularity
    , date_trunc('{{granularity}}', date) as date
    {% for breakdown in breakdowns %}
        , {{ breakdown }}
    {% endfor %}
    , count(distinct case when stablecoin_daily_txns > 0 then from_address end) as stablecoin_dau
    , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
    , sum(stablecoin_daily_txns) as stablecoin_daily_txns
    , case 
        when sum(stablecoin_daily_txns) > 0 then sum(stablecoin_transfer_volume) / sum(stablecoin_daily_txns) 
        else 0
    end as stablecoin_avg_txn_value
    , count(distinct case when artemis_stablecoin_daily_txns > 0 then from_address end) as artemis_stablecoin_dau
    , sum(artemis_stablecoin_transfer_volume) as artemis_stablecoin_transfer_volume
    , sum(artemis_stablecoin_daily_txns) as artemis_stablecoin_daily_txns
    , case
        when sum(artemis_stablecoin_daily_txns) > 0 then sum(artemis_stablecoin_transfer_volume) / sum(artemis_stablecoin_daily_txns) 
        else 0
    end as artemis_stablecoin_avg_txn_value
    , count(distinct case when p2p_stablecoin_daily_txns > 0 then from_address end) as p2p_stablecoin_dau
    , sum(p2p_stablecoin_transfer_volume) as p2p_stablecoin_transfer_volume
    , sum(p2p_stablecoin_daily_txns) as p2p_stablecoin_daily_txns
    , case
        when sum(p2p_stablecoin_daily_txns) > 0 then sum(p2p_stablecoin_transfer_volume) / sum(p2p_stablecoin_daily_txns) 
        else 0
    end as p2p_stablecoin_avg_txn_value

    {% if granularity == 'day' %}
        , sum(stablecoin_supply) as stablecoin_supply
        , sum(case when is_wallet::number = 1 then stablecoin_supply else 0 end) as p2p_stablecoin_supply
    {% else %}
        , sum(case when date = date_trunc('{{granularity}}', date) then stablecoin_supply else 0 end) as stablecoin_supply
        , sum(case when is_wallet::number = 1 and date = date_trunc('{{granularity}}', date) then stablecoin_supply else 0 end) as p2p_stablecoin_supply
    {% endif %}
from {{ ref("agg_daily_stablecoin_breakdown_silver") }}
group by date_granularity, date {% for breakdown in breakdowns %}, {{ breakdown }} {% endfor %}
order by date_granularity
{% endmacro %}