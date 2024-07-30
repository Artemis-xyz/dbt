{% macro aave_liquidation_revenue(chain, protocol, contract_address) %}
with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from {{chain}}_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('{{contract_address}}')
    {% if is_incremental() %}
        and block_timestamp >= (select max(block_timestamp) from {{ this }})
    {% endif %}
)
select
    block_timestamp::date as date
    , '{{chain}}' as chain
    , '{{protocol}}' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join {{chain}}_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join {{chain}}_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour
{% endmacro %}