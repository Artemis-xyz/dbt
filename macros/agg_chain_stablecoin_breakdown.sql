{% macro agg_chain_stablecoin_breakdown(chain, granularity) %}
    with
        transfer_transactions as (
            select * from fact_{{ chain }}_stablecoin_transfers
            where block_timestamp >= (
                select dateadd({{ granularity }}, -1, max(block_timestamp))
                {% if chain in ("tron") %} from tron_allium.assets.trc20_token_transfers
                {% elif chain in ("solana") %} from solana_flipside.core.fact_transfers
                {% elif chain in ("celo") %} from {{ref("fact_" ~ chain ~ "_decoded_events")}}
                {% else %} from {{ chain }}_flipside.core.ez_decoded_event_logs
                {% endif %}
            )
            and (not is_mint or is_mint is null)
            and (not is_burn or is_burn is null)
        ),
        latest_balances as (
            select
                address,
                fact_{{ chain }}_address_balances_by_token.contract_address contract_address,
                {% if chain in ("solana") %}
                    greatest(max_by(amount, block_timestamp), 0) stablecoin_supply,
                {% else %}
                    greatest(
                        max_by(balance_token, block_timestamp) / pow(
                            10,
                            coalesce(
                                max(fact_{{ chain }}_stablecoin_contracts.num_decimals),
                                0
                            )
                        ),
                        0
                    ) stablecoin_supply,
                {% endif %}
                max(fact_{{ chain }}_stablecoin_contracts.symbol) symbol
            from fact_{{ chain }}_address_balances_by_token
            join
                fact_{{ chain }}_stablecoin_contracts
                on lower(fact_{{ chain }}_address_balances_by_token.contract_address)
                = lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
            -- TODO if there are other chains with pre-mints, add them here. Probably
            -- generalize
            where
                fact_{{ chain }}_address_balances_by_token.address not in (
                    select distinct (premint_address)
                    from fact_solana_stablecoin_premint_addresses
                )
            group by
                address, fact_{{ chain }}_address_balances_by_token.contract_address
        ),
        filtered_contracts as (
            select * from dim_contracts_gold where chain = '{{chain}}'
        ),
        transfer_transactions_agg as (
            select
                transfer_transactions.from_address::string from_address,
                transfer_transactions.contract_address contract_address,
                max(transfer_transactions.symbol) symbol,
                sum(transfer_volume) stablecoin_transfer_volume,
                sum(
                    case
                        when transfer_transactions.from_address is not null
                        then 1
                        else 0
                    end
                ) as stablecoin_daily_txns,
                count(distinct(to_address)) stablecoin_dau
            from transfer_transactions
            group by 1, 2
            order by stablecoin_transfer_volume desc
        ),
        results as (
            select
                coalesce(
                    transfer_transactions_agg.from_address, latest_balances.address
                ) from_address,
                coalesce(
                    transfer_transactions_agg.contract_address,
                    latest_balances.contract_address
                ) contract_address,
                max(filtered_contracts.name) contract_name,
                coalesce(
                    max(filtered_contracts.name),
                    coalesce(
                        transfer_transactions_agg.from_address, latest_balances.address
                    )
                ) as contract,
                max(filtered_contracts.friendly_name) application,
                max(dim_apps_gold.icon) icon,
                max(filtered_contracts.app) app,
                max(filtered_contracts.category) category,
                coalesce(
                    max(transfer_transactions_agg.symbol), max(latest_balances.symbol)
                ) symbol,
                coalesce(sum(stablecoin_transfer_volume), 0) stablecoin_transfer_volume,
                coalesce(sum(stablecoin_daily_txns), 0) as stablecoin_daily_txns,
                coalesce(sum(stablecoin_dau), 0) stablecoin_dau,
                coalesce(max(latest_balances.stablecoin_supply), 0) stablecoin_supply,
                '{{chain}}' as chain
            from transfer_transactions_agg
            full outer join
                latest_balances
                on lower(transfer_transactions_agg.contract_address)
                = lower(latest_balances.contract_address)
                and lower(transfer_transactions_agg.from_address)
                = lower(latest_balances.address)
            left join
                filtered_contracts
                on lower(
                    coalesce(
                        transfer_transactions_agg.from_address, latest_balances.address
                    )
                )
                = lower(filtered_contracts.address)
            left join dim_apps_gold on filtered_contracts.app = dim_apps_gold.namespace
            group by 1, 2
            order by stablecoin_transfer_volume desc
        ),
        results_dollar_denom as (
            select
                from_address,
                results.contract_address,
                contract_name,
                contract,
                application,
                icon,
                app,
                category,
                results.symbol,
                stablecoin_transfer_volume * coalesce(
                    fact_coingecko_token_realtime_data.token_current_price, 1
                ) as stablecoin_transfer_volume,
                stablecoin_daily_txns,
                stablecoin_dau,
                stablecoin_supply * coalesce(
                    fact_coingecko_token_realtime_data.token_current_price, 1
                ) as stablecoin_supply,
                chain
            from results
            join
                fact_{{ chain }}_stablecoin_contracts
                on lower(results.contract_address)
                = lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
            left join
                fact_coingecko_token_realtime_data
                on lower(fact_{{ chain }}_stablecoin_contracts.coingecko_id)
                = lower(fact_coingecko_token_realtime_data.token_id)
        )
    select *
    from results_dollar_denom
{% endmacro %}
