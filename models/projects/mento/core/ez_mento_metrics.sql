{{
    config(
        materialized="incremental",
        snowflake_warehouse="MENTO",
        database="MENTO",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

with
    spot_volume_spot_txn_spot_dau as (
        select 
            date
            , sum(spot_volume) as spot_volume
            , sum(spot_txns) as spot_txns
            , sum(spot_dau) as spot_dau
            , sum(cumulative_spot_volume) as cumulative_spot_volume
        from {{ref("fact_mento_spot_volume_spot_txn_spot_dau")}}
        group by 1
    )
    , stablecoin_metrics as (
        select
            date
            , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
            , sum(stablecoin_daily_txns) as stablecoin_daily_txns
            , sum(stablecoin_supply) as stablecoin_supply
            , sum(p2p_stablecoin_transfer_volume) as p2p_stablecoin_transfer_volume
            , sum(p2p_stablecoin_daily_txns) as p2p_stablecoin_daily_txns
            , sum(artemis_stablecoin_transfer_volume) as artemis_stablecoin_transfer_volume
            , sum(artemis_stablecoin_daily_txns) as artemis_stablecoin_daily_txns
        from {{ref("fact_mento_stablecoin_metrics")}}
        where date < to_date(sysdate())
        group by 1
    )

select 
    date
    , spot_volume
    , spot_txns
    , spot_dau
    , cumulative_spot_volume
    , stablecoin_transfer_volume
    , stablecoin_daily_txns
    , stablecoin_supply
    , p2p_stablecoin_transfer_volume
    , p2p_stablecoin_daily_txns
    , artemis_stablecoin_transfer_volume
    , artemis_stablecoin_daily_txns

    -- Timetamp Columns
    , sysdate() as created_on
    , sysdate() as modified_on
from stablecoin_metrics
left join spot_volume_spot_txn_spot_dau using (date)
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
