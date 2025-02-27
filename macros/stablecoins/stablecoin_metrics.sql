{% macro stablecoin_metrics(chain, new_stablecoin_address) %}
    {% set backfill_date = '' %}

    with
        stablecoin_supply as (
            select
                date
                , contract_address
                , symbol
                , address as from_address
                {% if chain in ('tron') %}
                    , stablecoin_supply
                {% elif chain in ('ethereum') %}
                    , case
                        when (
                            lower(address) in (
                                select 
                                    lower(premint_address) 
                                from {{ref("fact_"~chain~"_stablecoin_bridge_addresses")}}
                                union all
                                select
                                    lower(premint_address)
                                from {{ref("fact_"~chain~"_stablecoin_premint_addresses")}}
                            )
                            and not (
                                lower(address) = lower('0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503')
                                and lower(contract_address) = lower('0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409')
                            ) 
                        ) then 0
                        else stablecoin_supply
                    end as stablecoin_supply
                {% elif chain in ('solana', 'celo', 'ton', 'sui', 'polygon', 'avalanche') %}
                    , case
                        when 
                            lower(address) in (select lower(premint_address) from {{ref("fact_"~chain~"_stablecoin_premint_addresses")}}) then 0
                        else stablecoin_supply
                    end as stablecoin_supply
                {% else %}
                    , stablecoin_supply
                {% endif %}
                , unique_id
            from {{ ref("fact_" ~ chain ~ "_stablecoin_balances")}}
            {% if is_incremental() and new_stablecoin_address == '' %}
                where date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
            {% if backfill_date != '' %}
                {% if is_incremental() and new_stablecoin_address == '' %}
                    and date < '{{ backfill_date }}'
                {% else %}
                    where date < '{{ backfill_date }}'
                {% endif %}
            {% endif %}
            {% if new_stablecoin_address != '' %}
                {% if backfill_date != '' %}
                    and lower(contract_address) = lower('{{ new_stablecoin_address }}')
                {% else %}
                    where lower(contract_address) = lower('{{ new_stablecoin_address }}')
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
            {% if is_incremental() and new_stablecoin_address == '' %}
                where date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
            {% if backfill_date != '' %}
                {% if is_incremental() and new_stablecoin_address == '' %}
                    and date < '{{ backfill_date }}'
                {% else %}
                    where date < '{{ backfill_date }}'
                {% endif %}
            {% endif %}
            {% if new_stablecoin_address != '' %}
                {% if backfill_date != '' %}
                    and lower(contract_address) = lower('{{ new_stablecoin_address }}')
                {% else %}
                    where lower(contract_address) = lower('{{ new_stablecoin_address }}')
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
            {% if is_incremental() and new_stablecoin_address == '' %}
                where date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
            {% if backfill_date != '' %}
                {% if is_incremental() and new_stablecoin_address == '' %}
                    and date < '{{ backfill_date }}'
                {% else %}
                    where date < '{{ backfill_date }}'
                {% endif %}
            {% endif %}
            {% if new_stablecoin_address != '' %}
                {% if backfill_date != '' %}
                    and lower(contract_address) = lower('{{ new_stablecoin_address }}')
                {% else %}
                    where lower(contract_address) = lower('{{ new_stablecoin_address }}')
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
            {% if is_incremental() and new_stablecoin_address == '' %}
                where date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
            {% if backfill_date != '' %}
                {% if is_incremental() and new_stablecoin_address == '' %}
                    and date < '{{ backfill_date }}'
                {% else %}
                    where date < '{{ backfill_date }}'
                {% endif %}
            {% endif %}
            {% if new_stablecoin_address != '' %}
                {% if backfill_date != '' %}
                    and lower(contract_address) = lower('{{ new_stablecoin_address }}')
                {% else %}
                    where lower(contract_address) = lower('{{ new_stablecoin_address }}')
                {% endif %}
            {% endif %}
        )
        , chain_stablecoin_metrics as (
            select
                coalesce(stablecoin_supply.date, all_metrics.date) as date
                , coalesce(stablecoin_supply.contract_address, all_metrics.contract_address) as contract_address
                , coalesce(stablecoin_supply.symbol, all_metrics.symbol) as symbol
                , coalesce(stablecoin_supply.from_address, all_metrics.from_address) as from_address
                , coalesce(stablecoin_transfer_volume, 0) as stablecoin_transfer_volume
                , coalesce(stablecoin_daily_txns, 0) as stablecoin_daily_txns
                , coalesce(artemis_stablecoin_transfer_volume, 0) as artemis_stablecoin_transfer_volume
                , coalesce(artemis_stablecoin_daily_txns, 0) as artemis_stablecoin_daily_txns
                , coalesce(p2p_stablecoin_transfer_volume, 0) as p2p_stablecoin_transfer_volume
                , coalesce(p2p_stablecoin_daily_txns, 0) as p2p_stablecoin_daily_txns
                , coalesce(stablecoin_supply, 0) as stablecoin_supply
                , coalesce(stablecoin_supply.unique_id, all_metrics.unique_id) as unique_id
            from stablecoin_supply
            full outer join all_metrics on stablecoin_supply.unique_id = all_metrics.unique_id
            left join artemis_metrics on coalesce(stablecoin_supply.unique_id, all_metrics.unique_id) = artemis_metrics.unique_id
            left join p2p_metrics on coalesce(stablecoin_supply.unique_id, all_metrics.unique_id) = p2p_metrics.unique_id
            --Filter out rows that don't contribute to metrics
            where stablecoin_supply.stablecoin_supply != 0 or all_metrics.from_address is not null
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
                    {% if chain not in ('solana', 'tron', 'near', 'ton', 'sui') %}
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
        , round(stablecoin_supply, 2) as stablecoin_supply
        , '{{ chain }}' as chain
        , unique_id || '-' || chain as unique_id
    from tagged_chain_stablecoin_metrics

{% endmacro %}