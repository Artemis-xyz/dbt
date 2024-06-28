-- Assumes the following:
-- 1. The table `fact_{{ chain }}_stablecoin_contracts` exists
-- 2. The table `{{ chain }}_flipside.core.ez_decoded_event_logs` exists and the
-- contracts in `fact_{{ chain }}_stablecoin_contracts` are decoded
-- 3. The table `{{ chain }}_flipside.core.fact_transactions` exists
{% macro agg_chain_stablecoin_metrics(chain) %}
    with
        transfer_transactions as (select * from {% if chain in ("ton") %} {{ chain }}.prod_raw.ez_stablecoin_transfers  {% else %} fact_{{ chain }}_stablecoin_transfers {% endif %}),
        deduped_flows as (
            select 
                date,
                sum(deduped_transfer_volume) as deduped_transfer_volume,
                contract_address
            from (
                select
                    date,
                    tx_hash,
                    max(transfer_volume) as deduped_transfer_volume,
                    lower(contract_address) as contract_address
                from transfer_transactions
                group by date, tx_hash, contract_address
            )
            group by date, contract_address
        ),
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
                daily_flows.date as date,
                sum(inflow) over (
                    partition by daily_flows.contract_address order by daily_flows.date asc
                ) as total_supply,
                txns,
                dau,
                transfer_volume,
                deduped_transfer_volume,
                daily_flows.contract_address
            from daily_flows
            left join deduped_flows on 
                lower(daily_flows.contract_address) = lower(deduped_flows.contract_address)
                and daily_flows.date = deduped_flows.date
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
                deduped_transfer_volume * price as deduped_transfer_volume,
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
        coalesce(daily_cum_flows_dollar_denom.deduped_transfer_volume, 0) as deduped_transfer_volume,
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
