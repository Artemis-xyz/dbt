{% macro agg_daily_stablecoin_metrics_breakdown(chain) %}
with 
    transfer_transactions as (
        select 
            block_timestamp,
            from_address,
            contract_address,
            symbol,
            transfer_volume,
            to_address
        -- @anthony
        -- Can move into stablecoin transfers table if needed
        -- Logic is slightly different for solana tron and near
        -- Right now I am leaving it here so that we dont have to change the logic in the stablecoin transfers table

        --Average transfer volume is currently done in the API stablecoins.py with `_fetch_avg_transaction_size`
        , case 
            {% if chain not in ('solana', 'tron', 'near') %}
                when 
                    to_address not in (select contract_address from {{ ref("dim_" ~ chain ~ "_contract_addresses") }})
                    and from_address not in (select contract_address from {{ ref("dim_" ~ chain ~ "_contract_addresses")}}) 
                    then 1
                else 0 
            {% else %}
                when 
                    to_address in (select address from {{ ref("dim_" ~ chain ~ "_eoa_addresses") }})
                    and from_address in (select address from {{ ref("dim_" ~ chain ~ "_eoa_addresses") }})
                    then 1
                else 0 
            {% endif %}
        end as is_p2p 
        from {{ ref("fact_" ~ chain ~ "_stablecoin_transfers")}}
        {% if is_incremental() %} 
            where block_timestamp >= (
                select dateadd('day', -3, max(date))
                from {{ this }}
            )
        {% endif %}
    ),
    filtered_contracts as (
        select * from pc_dbt_db.prod.dim_contracts_gold where chain = '{{ chain }}'
    ),
    transfer_transactions_agg as (
        select
            block_timestamp::date as date
            , transfer_transactions.from_address::string as from_address
            , transfer_transactions.contract_address as contract_address
            , transfer_transactions.symbol as symbol
            , sum(transfer_volume) as stablecoin_transfer_volume
            , sum(
                case
                    when transfer_transactions.from_address is not null
                    then 1
                    else 0
                end
            ) as stablecoin_daily_txns
            , count(distinct(to_address)) as stablecoin_dau
            , sum(transfer_volume * is_p2p) as p2p_stablecoin_transfer_volume
            , sum(
                case
                    when transfer_transactions.from_address is not null and is_p2p = 1
                    then 1
                    else 0
                end
            ) as p2p_stablecoin_daily_txns
            , count(
                distinct case 
                    when is_p2p = 1 then to_address 
                    else null 
                end
            ) as p2p_stablecoin_dau
        from transfer_transactions
        group by 1, 2, 3, 4
    ),
    results as (
        select
            transfer_transactions_agg.date
            --stablecoin idenifiers
            , transfer_transactions_agg.contract_address
            , transfer_transactions_agg.symbol as symbol
            --sender idenifiers
            , transfer_transactions_agg.from_address
            , filtered_contracts.name as contract_name
            , coalesce(filtered_contracts.name, transfer_transactions_agg.from_address) as contract
            , filtered_contracts.friendly_name as application
            , dim_apps_gold.icon as icon
            , filtered_contracts.app as app
            , filtered_contracts.category as category
            --metrics
            , coalesce(stablecoin_transfer_volume, 0) as stablecoin_transfer_volume
            , coalesce(stablecoin_daily_txns, 0) as stablecoin_daily_txns
            , coalesce(stablecoin_dau, 0) stablecoin_dau
            --p2p metrics
            , coalesce(p2p_stablecoin_transfer_volume, 0) as p2p_stablecoin_transfer_volume
            , coalesce(p2p_stablecoin_daily_txns, 0) as p2p_stablecoin_daily_txns
            , coalesce(p2p_stablecoin_dau, 0) as p2p_stablecoin_dau
            , '{{ chain }}' as chain
        from transfer_transactions_agg
        left join filtered_contracts
            on lower(transfer_transactions_agg.from_address) = lower(filtered_contracts.address)
        left join pc_dbt_db.prod.dim_apps_gold dim_apps_gold 
            on filtered_contracts.app = dim_apps_gold.namespace
    ),
    results_dollar_denom as (
        select
            results.date
            , results.contract_address
            , results.symbol

            , from_address
            , contract_name
            , contract
            , application
            , icon
            , app
            , category
            , stablecoin_transfer_volume * coalesce(
                d.token_current_price, 1
            ) as stablecoin_transfer_volume
            , stablecoin_daily_txns
            , stablecoin_dau
            , p2p_stablecoin_transfer_volume * coalesce(
                d.token_current_price, 1
            ) as p2p_stablecoin_transfer_volume
            , p2p_stablecoin_daily_txns
            , p2p_stablecoin_dau
            , chain
        from results
        left join {{ ref( "fact_" ~ chain ~ "_stablecoin_contracts") }} c
            on lower(results.contract_address) = lower(c.contract_address)
        left join {{ ref( "fact_coingecko_token_realtime_data") }} d
            on lower(c.coingecko_id) = lower(d.token_id)
    )
select *
from results_dollar_denom
where date < to_date(sysdate())
{% endmacro %}