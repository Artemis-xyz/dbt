{{ config(materialized="table", snowflake_warehouse="SOLANA") }}
with
    globalaverage as (
        select
            avg(cur_total_txns) as avg_transactions,
            avg(cur_distinct_to_address_count) as avg_unique_senders,
            avg(address_life_span) as avg_time_span_days
        from {{ ref("dim_solana_bots") }}
    ),

    summeddata as (
        select
            from_address,
            cur_total_txns,
            cur_distinct_to_address_count,
            address_life_span,
            first_transaction_timestamp,
            user_type
        from {{ ref("dim_solana_bots") }}
    ),

    scale_calc as (

        select
            summeddata.from_address,
            summeddata.cur_total_txns,
            summeddata.cur_distinct_to_address_count,
            summeddata.address_life_span,
            user_type,
            summeddata.cur_total_txns
            / globalaverage.avg_transactions as scaled_transactions,
            summeddata.cur_distinct_to_address_count
            / globalaverage.avg_unique_senders as scaled_senders,
            summeddata.address_life_span
            / globalaverage.avg_time_span_days as scaled_time_span_days,
            summeddata.cur_distinct_to_address_count
            / summeddata.cur_total_txns as unique_senders_ratio,
            first_transaction_timestamp
        from summeddata, globalaverage
    ),

    chain_balance as (

        select
            address,
            avg(stablecoin_balance) as stablecoin_balance,
            avg(native_token_balance) as native_token_balance
        from {{ ref("fact_solana_daily_balances") }}
        where
            date(date)
            between current_date() - interval '30 Days' and current_date()
        group by 1
    ),

    scoring_table as (
        select
            sc.*,
            coalesce(chain.stablecoin_balance, 0) as stablecoin_balance,
            coalesce(chain.native_token_balance, 0) as native_token_balance
        from scale_calc sc
        left join chain_balance chain on sc.from_address = chain.address
        where first_transaction_timestamp < current_date() - interval '30 Days'
    ),

    score_calc as (
        select
            from_address,
            user_type,
            cur_total_txns,
            cur_distinct_to_address_count,
            address_life_span,
            1 / (
                1 + exp(
                    - (
                        3.4136459 -- Intercept
                        + 0.00013386 * scaled_transactions
                        + -0.9438403 * scaled_senders
                        + -1.7059050 * scaled_time_span_days
                        + -0.4342306 * unique_senders_ratio
                        + -0.0010567 * stablecoin_balance
                        + -0.0001280 * native_token_balance
                    )
                )
            ) as probability
        from scoring_table
    )
select
    from_address,
    probability,
    user_type,
    cur_total_txns,
    cur_distinct_to_address_count,
    address_life_span,
    case
        when probability >= 0.5 then 'sybil' else 'not-sybil'
    end as engagement_type
from score_calc

union all

select
    from_address,
    0 as probability,
    user_type,
    cur_total_txns,
    cur_distinct_to_address_count,
    address_life_span,
    'new' as engagemnet_type
from scale_calc sc
where first_transaction_timestamp >= current_date() - interval '30 Days'