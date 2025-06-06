{% macro stablecoin_breakdown(breakdowns=[], granularity='day') %}

with max_date as (
    select max(date) as max_date
    from {{ ref("agg_daily_stablecoin_breakdown_with_labels_silver") }}
)

select
    date_trunc('{{granularity}}', date) as date_granularity
    {% for breakdown in breakdowns %}
        {% if breakdown == 'application' %}
            , coalesce(application, 'Unlabeled') as {{ breakdown }}
        {% elif breakdown == 'category' %}
            , coalesce(artemis_category_id, 'Unlabeled') as {{ breakdown }}
        {% else %}
            , {{ breakdown }}
        {% endif %}
    {% endfor %}
    , count(distinct case when stablecoin_daily_txns > 0 then address end) as stablecoin_dau
    , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
    , sum(stablecoin_daily_txns) as stablecoin_daily_txns
    , case 
        when sum(stablecoin_daily_txns) > 0 then sum(stablecoin_transfer_volume) / sum(stablecoin_daily_txns) 
        else 0
    end as stablecoin_avg_txn_value
    , count(distinct case when artemis_stablecoin_daily_txns > 0 then address end) as artemis_stablecoin_dau
    , sum(artemis_stablecoin_transfer_volume) as artemis_stablecoin_transfer_volume
    , sum(artemis_stablecoin_daily_txns) as artemis_stablecoin_daily_txns
    , case
        when sum(artemis_stablecoin_daily_txns) > 0 then sum(artemis_stablecoin_transfer_volume) / sum(artemis_stablecoin_daily_txns) 
        else 0
    end as artemis_stablecoin_avg_txn_value
    , count(distinct case when p2p_stablecoin_daily_txns > 0 then address end) as p2p_stablecoin_dau
    , sum(p2p_stablecoin_transfer_volume) as p2p_stablecoin_transfer_volume
    , sum(p2p_stablecoin_daily_txns) as p2p_stablecoin_daily_txns
    , case
        when sum(p2p_stablecoin_daily_txns) > 0 then sum(p2p_stablecoin_transfer_volume) / sum(p2p_stablecoin_daily_txns) 
        else 0
    end as p2p_stablecoin_avg_txn_value
    {% if granularity == 'day' and breakdowns | length == 1 and (breakdowns[0] == 'symbol' or breakdowns[0] == 'chain') %}
        , count(distinct case when stablecoin_supply > 0 then address end) as stablecoin_tokenholder_count
        , count(distinct case when is_wallet::number = 1 and stablecoin_supply > 0 then address end) as p2p_stablecoin_tokenholder_count
    {% endif %}
    {% if granularity == 'day' %}
        , sum(stablecoin_supply) as stablecoin_supply
        , sum(case when is_wallet::number = 1 then stablecoin_supply else 0 end) as p2p_stablecoin_supply
    {% else %}
        , sum(case when date = 
            case
                when date_trunc('{{granularity}}', date) = date_trunc('{{granularity}}', (select max_date from max_date))
                    then (select max_date from max_date)
                else last_day(date, '{{granularity}}')
            end 
        then stablecoin_supply else 0 end) as stablecoin_supply
        , sum(case when is_wallet::number = 1 and date = 
            case
                when date_trunc('{{granularity}}', date) = date_trunc('{{granularity}}', (select max_date from max_date))
                    then (select max_date from max_date)
                else last_day(date, '{{granularity}}')
            end
        then stablecoin_supply else 0 end) as p2p_stablecoin_supply
    {% endif %}
from {{ ref("agg_daily_stablecoin_breakdown_with_labels_silver") }}
{% if is_incremental() %}
    where date >= (select dateadd('{{granularity}}', -3, max(date_granularity)) from {{ this }})
{% endif %}
{% if 'application' in breakdowns %}
    {% if not is_incremental() %}
        where application is not null
    {% else %}
        and application is not null
    {% endif %}
{% endif %}
group by date_granularity {% for breakdown in breakdowns %}, {{ breakdown }} {% endfor %}
order by date_granularity
{% endmacro %}