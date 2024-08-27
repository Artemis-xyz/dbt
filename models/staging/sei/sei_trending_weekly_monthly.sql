{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE_MD") }}

with
    sei_contracts as (

        select address, name, app as namespace, friendly_name, category
        from {{ ref("dim_contracts_gold") }}
        where chain = 'sei'
    ),
    prices as ({{ get_coingecko_price_for_trending("sei-network") }}),
    msg_atts_base AS (
        SELECT
            max(block_timestamp) as block_timestamp,
            max(date_trunc('day', block_timestamp)) as date,
            max(tx_succeeded) as tx_succeeded,
            tx_id,
            max (case when attribute_key in ('_contract_address', 'contract_address') then attribute_value else null end) as contract_address_1,
            max (
                case 
                    when msg_type = 'transfer' then 'sei_token_transfer'
                    when msg_type = 'ibc_transfer' then 'sei_ibc_transfer'
                    when msg_type = 'message' AND attribute_key = 'action' AND attribute_value = '/seiprotocol.seichain.oracle.MsgAggregateExchangeRateVote'
                        then 'sei_oracle_votes'
                    when msg_type = 'instantiate' AND attribute_key = 'contract_address' then 'sei_create_contract'
                    when msg_type = 'message' AND attribute_key = 'action' AND attribute_value = '/cosmos.gov.v1beta1.MsgVote' then 'sei_governance_votes'
                    when msg_type = 'message' and attribute_key = 'module' and attribute_key = 'staking' then 'sei_staking'
                    when msg_type = 'message' and attribute_key = 'module' and attribute_value = 'oracle' then 'sei_oracle'
                    when msg_type = 'withdraw_rewards' then 'sei_staking_withdraw'
                    when msg_type = 'aggregate_vote' then 'sei_voting'
                    when msg_type = 'coin_spent' then 'sei_staking_coin_spent'
                    when msg_type in ('delegate','redelegate','unbond','create_validator') then 'sei_staking'
                    else null
                end
            ) as contract_address_2
        FROM
            sei_flipside.core.fact_msg_attributes
        WHERE block_timestamp >= dateadd(day, -60, current_date)
        GROUP BY tx_id
    ),
    transaction_contract_data as (
        SELECT
            tx_id as tx_hash
            , block_timestamp
            , date
            , tx_succeeded
            , coalesce(contract_address_1, contract_address_2) as contract_address
        FROM
            msg_atts_base
    ),
    last_2_month as (
        SELECT 
            t1.tx_hash
            , t1.block_timestamp
            , t1.date
            , t2.tx_from as from_address
            , (split(t2.fee, 'usei')[0] / pow(10, 6)) as tx_fee
            , (split(t2.fee, 'usei')[0] / pow(10, 6)) * t4.price as gas_usd
            , t1.contract_address as to_address
            , t3.name
            , t3.namespace
            , t3.friendly_name
            , t3.category
        from transaction_contract_data as t1
        LEFT JOIN sei_flipside.core.fact_transactions as t2 on t1.tx_hash = t2.tx_id
        left join sei_contracts as t3 on lower(t1.contract_address) = lower(t3.address)
        left join prices as t4 on t1.date= t4.date
        where
            t2.block_timestamp >= dateadd(day, -60, current_date)
        union all
        select
            tx_hash,
            block_timestamp,
            date_trunc('day', block_timestamp) date,
            t.from_address,
            tx_fee,
            (tx_fee * price) gas_usd,
            sei_contracts.address as to_address,
            sei_contracts.name,
            sei_contracts.namespace,
            sei_contracts.friendly_name,
            sei_contracts.category,
        from sei_flipside.core_evm.fact_transactions as t
        left join sei_contracts on lower(t.to_address) = lower(sei_contracts.address)
        left join prices on date_trunc('day', block_timestamp) = prices.date
        where
            block_timestamp >= dateadd(day, -60, current_date)
    ),
    last_week as (
        select
            t.to_address to_address,
            count(*) txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(gas_usd) as gas_usd,
            max(name) name,
            max(namespace) namespace,
            max(friendly_name) friendly_name,
            max(category) category
        from last_2_month as t
        where t.to_address is not null and t.date >= dateadd(day, -7, current_date)
        group by t.to_address
    ),
    two_week as (
        select
            t.to_address to_address,
            count(*) txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(gas_usd) as gas_usd
        from last_2_month as t
        where
            t.to_address is not null
            and t.date < dateadd(day, -7, current_date)
            and t.date >= dateadd(day, -14, current_date)
        group by t.to_address
    ),
    trending_week as (
        select
            last_week.to_address,
            last_week.txns,
            last_week.gas,
            last_week.gas_usd,
            last_week.dau,
            two_week.txns prev_txns,
            two_week.gas prev_gas,
            two_week.gas_usd prev_gas_usd,
            two_week.dau prev_dau,
            last_week.name,
            last_week.namespace,
            last_week.friendly_name,
            last_week.category,
            'weekly' as granularity
        from last_week
        left join two_week on lower(last_week.to_address) = lower(two_week.to_address)
    ),
    last_month as (
        select
            to_address to_address,
            count(*) as txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(gas_usd) as gas_usd,
            max(name) name,
            max(namespace) namespace,
            max(friendly_name) friendly_name,
            max(category) category
        from last_2_month
        where to_address is not null and date >= dateadd(day, -30, current_date)
        group by to_address
    ),
    two_month as (
        select
            to_address to_address,
            count(*) txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(gas_usd) as gas_usd
        from last_2_month
        where
            to_address is not null
            and date < dateadd(day, -30, current_date)
            and date >= dateadd(day, -60, current_date)
        group by to_address
    ),
    trending_month as (
        select
            last_month.to_address,
            last_month.txns,
            last_month.gas,
            last_month.gas_usd,
            last_month.dau,
            two_month.txns prev_txns,
            two_month.gas prev_gas,
            two_month.gas_usd prev_gas_usd,
            two_month.dau prev_dau,
            last_month.name,
            last_month.namespace,
            last_month.friendly_name,
            last_month.category,
            'monthly' as granularity
        from last_month
        left join
            two_month on lower(last_month.to_address) = lower(two_month.to_address)
    )
select *
from trending_week
union
select *
from trending_month
