{{
    config(
        materialized="table",
        snowflake_warehouse="DIMO"
    )
}}

with 
    polygon_revenue as (
        select
            block_timestamp::date as date
            , 'polygon' as chain
            , sum(decoded_log:"value"::number / 1E18) as revenue_native
        from polygon_flipside.core.ez_decoded_event_logs 
        where lower(contract_address) = lower('0xE261D618a959aFfFd53168Cd07D12E37B26761db')
            and event_name = 'Transfer'
            and lower(decoded_log:"from"::string) = lower('0x0000000000000000000000000000000000000000')
        group by 1
    )
    , prices as ({{ get_coingecko_price_with_latest("dimo") }})

select
    date
    , chain
    , revenue_native
    , revenue_native * price as revenue
    , revenue_native / .01 as fees_native
    , fees_native * price as fees
from polygon_revenue
left join prices using(date)