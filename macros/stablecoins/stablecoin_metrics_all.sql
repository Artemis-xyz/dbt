{% macro stablecoin_metrics_all(chain, new_stablecoin_address) %}

{% set backfill_days = 3 %}

with
    stablecoin_transfers as (
        select 
            block_timestamp
            , tx_hash
            , from_address
            , contract_address
            , symbol
            , transfer_volume
            , to_address
        from {{ ref("fact_" ~ chain ~ "_stablecoin_transfers")}}
        where transfer_volume > 0
        {% if is_incremental() and new_stablecoin_address == '' %} 
            and block_timestamp >= (
                select dateadd('day', -{{ backfill_days }}, max(date))
                from {{ this }}
            )
        {% endif %}
        {% if new_stablecoin_address != '' %}
            and lower(contract_address) = lower('{{ new_stablecoin_address }}')
        {% endif %}
    )
    , stablecoin_metrics as (
        select
            block_timestamp::date as date
            , stablecoin_transfers.from_address::string as from_address
            , stablecoin_transfers.contract_address as contract_address
            , stablecoin_transfers.symbol as symbol
            , sum(transfer_volume) as stablecoin_transfer_volume
            , sum(
                case
                    when stablecoin_transfers.from_address is not null
                    then 1
                    else 0
                end
            ) as stablecoin_daily_txns
        from stablecoin_transfers
        group by 1, 2, 3, 4
    )
    , results_dollar_denom as (
        select
            stablecoin_metrics.date
            , stablecoin_metrics.contract_address
            , stablecoin_metrics.symbol
            , from_address
            , stablecoin_transfer_volume * {{waterfall_stablecoin_prices('c', 'd')}} as stablecoin_transfer_volume
            , stablecoin_daily_txns
        from stablecoin_metrics
        left join {{ ref( "fact_" ~ chain ~ "_stablecoin_contracts") }} c
            on lower(stablecoin_metrics.contract_address) = lower(c.contract_address)
        left join {{ ref( "fact_coingecko_token_date_adjusted_gold") }} d
            on lower(c.coingecko_id) = lower(d.coingecko_id)
            and stablecoin_metrics.date = d.date::date
    )
    select
        date
        , from_address
        , contract_address
        , symbol
        , stablecoin_transfer_volume
        , stablecoin_daily_txns
        , '{{ chain }}' as chain
        , date || '-' || from_address || '-' || contract_address as unique_id
    from results_dollar_denom
    where date < to_date(sysdate())
    {% if is_incremental() and new_stablecoin_address == '' %} 
        and date >= (
            select dateadd('day', -{{ backfill_days }}, max(date))
            from {{ this }}
        )
    {% endif %} 
    {% if new_stablecoin_address != '' %}
        and lower(contract_address) = lower('{{ new_stablecoin_address }}')
    {% endif %}

{% endmacro %}