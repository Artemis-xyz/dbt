{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'BLUR',
        database = 'blur',
        schema = 'core',
        alias = 'ez_metrics'
    )
}}

with
    blur_fees as (
        select *
        from {{ ref("fact_blur_fees") }}
    )
    , blur_daus as (
        select *
        from {{ ref("fact_blur_daus") }}
    )
    , blur_daily_txns as (
        select *
        from {{ ref("fact_blur_daily_txns") }}
    )
    , market_data as (
        {{ get_coingecko_metrics("blur") }}
    )
    , derived_metrics as (
        select
            blur_daus.date

            -- Cash Flow Metrics
            , blur_fees.fees as gross_protocol_revenue
            , blur_fees.fees as foundation_cash_flow

            -- Token Metrics
            , coalesce(market_data.price, 0) as price
            , coalesce(market_data.market_cap, 0) as market_cap
            , coalesce(market_data.fdmc, 0) as fdmc
            , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
            , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
            , coalesce(market_data.token_volume, 0) as token_volume

            -- Spot Dex Metrics
            , blur_daus.dau as spot_dau
            , blur_daily_txns.daily_txns as spot_txns
            , blur_fees.fees as spot_revenue

            -- Candidate Supply Metric
            , case
                when coalesce(market_data.price,0) = 0 then null
                else coalesce(market_data.market_cap, 0) / coalesce(market_data.price, 0)
            end as candidate_supply

        from blur_daus
        left join blur_daily_txns using (date)
        left join blur_fees using (date)
        left join market_data using (date)
    )
    , ordered_metrics as (
        select
            *,
            row_number() over (order by date asc) as rn
        from derived_metrics
        where date is not null
    )
    , recursive_supply as (
        select
            rn,
            date,
            gross_protocol_revenue,
            foundation_cash_flow,
            price,
            market_cap,
            fdmc,
            token_turnover_circulating,
            token_turnover_fdv,
            token_volume,
            spot_dau,
            spot_txns,
            spot_revenue,
            candidate_supply,
            candidate_supply as circulating_supply_native
        from ordered_metrics
        where rn = 1

        union all

        select
            o.rn,
            o.date,
            o.gross_protocol_revenue,
            o.foundation_cash_flow,
            o.price,
            o.market_cap,
            o.fdmc,
            o.token_turnover_circulating,
            o.token_turnover_fdv,
            o.token_volume,
            o.spot_dau,
            o.spot_txns,
            o.spot_revenue,
            o.candidate_supply,
            case
                when r.circulating_supply_native is null then o.candidate_supply
                when ((o.candidate_supply - r.circulating_supply_native) / r.circulating_supply_native) > 0 then o.candidate_supply
                else r.circulating_supply_native
            end as circulating_supply_native
        from recursive_supply r
        join ordered_metrics o on o.rn = r.rn + 1
    )

select
    *
    -- Supply Metrics
    , circulating_supply_native - lag(circulating_supply_native) over (order by date) as premine_unlocks_native
    , circulating_supply_native - lag(circulating_supply_native) over (order by date) as net_supply_change_native

from recursive_supply
order by date desc
