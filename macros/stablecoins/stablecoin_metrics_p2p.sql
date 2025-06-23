{% macro stablecoin_metrics_p2p(chain, new_stablecoin_address) %}

{% set backfill_days = 3 %}

with
    stablecoin_transfers as (
        select 
            block_timestamp
            , tx_hash
            , from_address
            , token_address as contract_address
            , symbol
            , amount_usd as transfer_volume
            , to_address
        from {{ ref("fact_" ~ chain ~ "_p2p_stablecoin_transfers")}} t
        left join {{ ref( "fact_" ~ chain ~ "_stablecoin_contracts") }} c
            on lower(t.token_address) = lower(c.contract_address)
        {% if is_incremental() and new_stablecoin_address == '' %} 
            where block_timestamp >= (
                select dateadd('day', -{{ backfill_days }}, max(date))
                from {{ this }}
            )
        {% endif %}
        {% if new_stablecoin_address != '' %}
            where lower(contract_address) = lower('{{ new_stablecoin_address }}')
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
    
    select
        date
        , from_address
        , contract_address
        , symbol
        , stablecoin_transfer_volume
        , stablecoin_daily_txns
        , '{{ chain }}' as chain
        , date || '-' || from_address || '-' || contract_address as unique_id
    from stablecoin_metrics
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