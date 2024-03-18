-- Assumes the following:
-- 1. The table `fact_{{ chain }}_stablecoin_contracts` exists
-- 2. The table `{{ chain }}_flipside.core.ez_decoded_event_logs` exists and the
-- contracts in `fact_{{ chain }}_stablecoin_contracts` are decoded
-- 3. The table `{{ chain }}_flipside.core.fact_transactions` exists
{% macro agg_chain_stablecoin_metrics(chain) %}
    with
        transfer_transactions as ({{ agg_chain_stablecoin_transfers(chain) }}),
        daily_flows as (
            select
                date,
                sum(inflow) inflow,
                sum(transfer_volume) transfer_volume,
                count(distinct from_address) as dau,
                count(*) as txns,
                lower(contract_address) as contract_address
            from transfer_transactions
            group by date, contract_address
            union all
            select
                dateadd(
                    day, -1, (select min(trunc(date, 'day')) from transfer_transactions)
                ) as date,
                sum(initial_supply) as inflow,
                0 as transfer_volume,
                0 as dau,
                0 as txns,
                lower(contract_address) as contract_address
            from fact_{{ chain }}_stablecoin_contracts
            group by contract_address
        ),
        daily_cum_flows as (
            select
                date as date,
                sum(inflow) over (
                    partition by contract_address order by date asc
                ) as total_supply,
                txns,
                dau,
                transfer_volume,
                contract_address
            from daily_flows
        ),
        daily_cum_flows_dollar_denom as (
            select
                daily_cum_flows.date,
                -- TODO: Refactor to support weird currencies. Currently assumes
                -- everything is $1 if not found
                coalesce(
                    fact_coingecko_token_date_adjusted_gold.shifted_token_price_usd, 1
                ) as price,
                total_supply * price as total_supply,
                txns,
                dau,
                transfer_volume * price as transfer_volume,
                daily_cum_flows.contract_address
            from daily_cum_flows
            join
                fact_{{ chain }}_stablecoin_contracts
                on lower(daily_cum_flows.contract_address)
                = lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
            left join
                fact_coingecko_token_date_adjusted_gold
                on lower(fact_{{ chain }}_stablecoin_contracts.coingecko_id)
                = lower(fact_coingecko_token_date_adjusted_gold.coingecko_id)
                and daily_cum_flows.date = fact_coingecko_token_date_adjusted_gold.date
        )
    select
        daily_cum_flows_dollar_denom.date as date,
        daily_cum_flows_dollar_denom.price as price,
        coalesce(daily_cum_flows_dollar_denom.total_supply, 0) as total_supply,
        coalesce(daily_cum_flows_dollar_denom.txns, 0) as txns,
        coalesce(daily_cum_flows_dollar_denom.dau, 0) as dau,
        coalesce(daily_cum_flows_dollar_denom.transfer_volume, 0) as transfer_volume,
        '{{chain}}' as chain,
        fact_{{ chain }}_stablecoin_contracts.symbol,
        fact_{{ chain }}_stablecoin_contracts.contract_address
    from daily_cum_flows_dollar_denom
    join
        pc_dbt_db.prod.fact_{{ chain }}_stablecoin_contracts
        on lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
        = lower(daily_cum_flows_dollar_denom.contract_address)
    order by date asc, symbol
{% endmacro %}
