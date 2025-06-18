{% macro stablecoin_metrics_automatic_labels(chain, new_stablecoin_address) %}
    {% set backfill_date = '' %}
    {% set backfill_days = 3 %}
    {% set heavy_compute_chains = ['ethereum', 'solana', 'tron', 'polygon', 'bsc'] %}
    with
        stablecoin_supply as (
            select
                date
                , contract_address
                , symbol
                , address as from_address
                {% if chain in ('ethereum', 'tron') %}
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
                {% elif chain in ('solana', 'celo', 'ton', 'sui', 'polygon', 'avalanche', 'aptos', 'kaia') %}
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
                where date > (select dateadd('day', -{{ backfill_days }}, max(date)) from {{ this }})
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
                where date > (select dateadd('day', -{{ backfill_days }}, max(date)) from {{ this }})
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
                where date > (select dateadd('day', -{{ backfill_days }}, max(date)) from {{ this }})
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
                where date > (select dateadd('day', -{{ backfill_days }}, max(date)) from {{ this }})
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
            select 
                address
                , name
                , friendly_name
                , labels.artemis_application_id
                , labels.artemis_category_id
                , labels.artemis_sub_category_id
                , dim_apps_gold.icon
            from {{ ref("dim_all_addresses_labeled_gold")}} labels
            left join {{ref("dim_all_apps_gold")}} dim_apps_gold 
                on labels.artemis_application_id = dim_apps_gold.artemis_application_id
             where chain = '{{ chain }}'
        )
        , tagged_chain_stablecoin_metrics as (
            select
                date
                --From Address Identifers
                , from_address as address
                , filtered_contracts.name as name
                , filtered_contracts.friendly_name as friendly_name
                , filtered_contracts.icon as icon
                , filtered_contracts.artemis_application_id as artemis_application_id
                , case 
                    when filtered_contracts.artemis_sub_category_id = 'market_maker' then filtered_contracts.artemis_sub_category_id
                    when filtered_contracts.artemis_sub_category_id = 'cex' then filtered_contracts.artemis_sub_category_id
                    else filtered_contracts.artemis_category_id 
                end as artemis_category_id
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
        )
    {% if is_incremental() and backfill_date == '' %}
        , updated_labels as (
            select 
                t1.address
                , max(filtered_contracts.name) as name
                , max(filtered_contracts.friendly_name) as friendly_name
                , max(filtered_contracts.artemis_application_id) as artemis_application_id
                , max(case 
                    when filtered_contracts.artemis_sub_category_id = 'market_maker' then filtered_contracts.artemis_sub_category_id
                    when filtered_contracts.artemis_sub_category_id = 'cex' then filtered_contracts.artemis_sub_category_id
                    else filtered_contracts.artemis_category_id 
                end) as artemis_category_id
                , max(filtered_contracts.icon) as icon
            from {{this}} t1
            inner join filtered_contracts
                on lower(t1.address) = lower(filtered_contracts.address)
            where 
            {% if chain in heavy_compute_chains %}
                date = (select dateadd('day', -{{ backfill_days }}, max(date)) from {{this}}) and 
            {% endif %}
                (
                    coalesce(t1.name, '') <> filtered_contracts.name 
                    or coalesce(t1.friendly_name, '') <> filtered_contracts.friendly_name
                    or coalesce(t1.artemis_application_id, '') <> filtered_contracts.artemis_application_id
                    or (
                        filtered_contracts.artemis_category_id in ('market_maker', 'cex')
                        -- take into account market maker and cex sub categories
                        and filtered_contracts.artemis_sub_category_id <> coalesce(t1.artemis_category_id, '')
                    )
                    or (
                        filtered_contracts.artemis_category_id not in ('market_maker', 'cex')
                        and filtered_contracts.artemis_category_id <> coalesce(t1.artemis_category_id, '')
                    )
                    or coalesce(t1.icon, '') != filtered_contracts.icon
                )
            group by 1
        )
        , updated_rows as (
            select 
                date
                , t1.address
                , updated_labels.name
                , updated_labels.friendly_name
                , updated_labels.icon
                , updated_labels.artemis_application_id
                , updated_labels.artemis_category_id
                
                , is_wallet
                --Stablecoin Identifiers
                , contract_address
                , symbol
                --Metrics
                , stablecoin_transfer_volume
                , stablecoin_daily_txns
                , artemis_stablecoin_transfer_volume
                , artemis_stablecoin_daily_txns
                , p2p_stablecoin_transfer_volume
                , p2p_stablecoin_daily_txns
                , stablecoin_supply
                , unique_id
            from {{this}} t1
            inner join updated_labels
                on lower(t1.address) = lower(updated_labels.address)
            where date <= (select dateadd('day', -{{ backfill_days }}, max(date)) from {{this}})
            {% if chain in heavy_compute_chains %}
                and date > (select dateadd('day', -30, max(date)) from {{this}})
            {% endif %}
        )
    {% endif %}
    select
        date
        , address
        , name
        , friendly_name
        , icon
        , artemis_application_id
        , artemis_category_id
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
    {% if is_incremental() and backfill_date == '' %}
        union all
        select
            date
            , address
            , name
            , friendly_name
            , icon
            , artemis_application_id
            , artemis_category_id
            
            , is_wallet
            --Stablecoin Identifiers
            , contract_address
            , symbol
            --Metrics
            , stablecoin_transfer_volume
            , stablecoin_daily_txns
            , artemis_stablecoin_transfer_volume
            , artemis_stablecoin_daily_txns
            , p2p_stablecoin_transfer_volume
            , p2p_stablecoin_daily_txns
            , stablecoin_supply
            , '{{ chain }}' as chain
            , unique_id
        from updated_rows
    {% endif %}
{% endmacro %}