{% macro stablecoin_metrics(chain) %}
    {% set backfill_date = '' %}
    with
        stablecoin_supply as (
            select
                date
                , contract_address
                , symbol
                , address
                {% if chain in ('ethereum') %}
                    , case 
                        when lower(address) in (select lower(premint_address) from {{ref("fact_"~chain~"_stablecoin_bridge_addresses")}}) then 0
                        else stablecoin_supply
                    end as stablecoin_supply
                {% else %}
                    , stablecoin_supply
                {% endif %}
                , unique_id
            from {{ ref("fact_" ~ chain ~ "_stablecoin_balances")}}
            {% if is_incremental() %}
                where date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
            {% if backfill_date != '' %}
                {% if is_incremental() %}
                    and date < '{{ backfill_date }}'
                {% else %}
                    where date < '{{ backfill_date }}'
                {% endif %}
            {% endif %}
        )
        , all_metrics as (
            select 
                date
                , contract_address
                , symbol
                , from_address
                , stablecoin_transfer_volume
                , stablecoin_daily_txns
                , unique_id
            from {{ ref("fact_" ~ chain ~ "_stablecoin_metrics_all")}}
            {% if is_incremental() %}
                where date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
            {% if backfill_date != '' %}
                {% if is_incremental() %}
                    and date < '{{ backfill_date }}'
                {% else %}
                    where date < '{{ backfill_date }}'
                {% endif %}
            {% endif %}
        )
        , artemis_metrics as (
            select 
                date
                , contract_address
                , symbol
                , from_address
                , stablecoin_transfer_volume as artemis_stablecoin_transfer_volume
                , stablecoin_daily_txns as artemis_stablecoin_daily_txns
                , unique_id
            from {{ ref("fact_" ~ chain ~ "_stablecoin_metrics_artemis")}}
            {% if is_incremental() %}
                where date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
            {% if backfill_date != '' %}
                {% if is_incremental() %}
                    and date < '{{ backfill_date }}'
                {% else %}
                    where date < '{{ backfill_date }}'
                {% endif %}
            {% endif %}
        )
        , p2p_metrics as (
            select 
                date
                , contract_address
                , symbol
                , from_address
                , stablecoin_transfer_volume as p2p_stablecoin_transfer_volume
                , stablecoin_daily_txns as p2p_stablecoin_daily_txns
                , unique_id
            from {{ ref("fact_" ~ chain ~ "_stablecoin_metrics_p2p")}}
            {% if is_incremental() %}
                where date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
            {% if backfill_date != '' %}
                {% if is_incremental() %}
                    and date < '{{ backfill_date }}'
                {% else %}
                    where date < '{{ backfill_date }}'
                {% endif %}
            {% endif %}
        )
        , chain_stablecoin_metrics as (
            select
                date
                , contract_address
                , symbol
                , from_address
                , stablecoin_transfer_volume
                , stablecoin_daily_txns
                , artemis_stablecoin_transfer_volume
                , artemis_stablecoin_daily_txns
                , p2p_stablecoin_transfer_volume
                , p2p_stablecoin_daily_txns
                , stablecoin_supply
                , unique_id
            from stablecoin_supply
            left join all_metrics using (unique_id)
            left join artemis_metrics using (unique_id)
            left join p2p_metrics using (unique_id)
        )
        , filtered_contracts as (
            select * from {{ ref("dim_contracts_gold")}} where chain = '{{ chain }}'
        )
        , tagged_chain_stablecoin_metrics as (
            select
                date
                --From Address Identifers
                , from_address
                , filtered_contracts.name as contract_name
                , coalesce(filtered_contracts.name, chain_stablecoin_metrics.from_address) as contract
                , filtered_contracts.friendly_name as application
                , dim_apps_gold.icon as icon
                , filtered_contracts.app as app
                , case 
                    when filtered_contracts.sub_category = 'Market Maker' then filtered_contracts.sub_category
                    when filtered_contracts.sub_category = 'CEX' then filtered_contracts.sub_category
                    else filtered_contracts.category 
                end as category
                , case 
                    {% if chain not in ('solana', 'tron', 'near') %}
                        when 
                            from_address not in (select contract_address from {{ ref("dim_" ~ chain ~ "_contract_addresses")}}) 
                            then 1
                        else 0 
                    {% else %}
                        when 
                            from_address in (select address from {{ ref("dim_" ~ chain ~ "_eoa_addresses") }})
                            then 1
                        else 0 
                    {% endif %}
                end as is_wallet
                --Stablecoin Identifiers
                , chain_stablecoin_metrics.contract_address
                , chain_stablecoin_metrics.symbol
                --Metrics
                , stablecoin_transfer_volume
                , stablecoin_daily_txns
                , artemis_stablecoin_transfer_volume
                , artemis_stablecoin_daily_txns
                , p2p_stablecoin_transfer_volume
                , p2p_stablecoin_daily_txns
                , stablecoin_supply
                , unique_id
            from chain_stablecoin_metrics
            left join filtered_contracts
                on lower(chain_stablecoin_metrics.from_address) = lower(filtered_contracts.address)
            left join {{ref("dim_apps_gold")}} dim_apps_gold 
                on filtered_contracts.app = dim_apps_gold.namespace
        )
    select
        date
        , from_address
        , contract_name
        , contract
        , application
        , icon
        , app
        , category
        , is_wallet

        , contract_address
        , symbol

        , stablecoin_transfer_volume
        , stablecoin_daily_txns
        , artemis_stablecoin_transfer_volume
        , artemis_stablecoin_daily_txns
        , p2p_stablecoin_transfer_volume
        , p2p_stablecoin_daily_txns
        --issue with float precision
        , round(stablecoin_supply, 3) as stablecoin_supply
        , '{{ chain }}' as chain
        , unique_id || '-' || chain as unique_id
    from tagged_chain_stablecoin_metrics

{% endmacro %}