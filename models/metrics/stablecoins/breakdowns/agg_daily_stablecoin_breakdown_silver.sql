{{ config(materialized="table", snowflake_warehouse="STABLECOIN_V2_LG") }}
with
    daily_data as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("ez_base_stablecoin_metrics_by_address"),
                ]
            )
        }}
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
    , stablecoin_supply
    , chain
    , unique_id
from daily_data
where date < to_date(sysdate())