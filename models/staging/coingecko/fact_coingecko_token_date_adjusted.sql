/*
Shift the date back one day.

This is because CG considers ETH price for March 14 to be ETH price at 12:01am UTC.
Instead users typically conceptualize ETH price on Mar 14 at 12:01am UTC to actually
be the end-of-day price of Mar 13 (not Mar 14). We want to reflect what users expect.
*/
{{ config(materialized="table") }}
select
    t1.date,
    t1.coingecko_id,
    t2.token_price_usd as shifted_token_price_usd,
    t2.token_market_cap as shifted_token_market_cap,
    t2.token_h24_volume_usd as shifted_token_h24_volume_usd,
    t2.token_circulating_supply as shifted_token_circulating_supply
from {{ ref("fact_coingecko_token") }} as t1
left join
    {{ ref("fact_coingecko_token") }} as t2
    on t1.coingecko_id = t2.coingecko_id
    and t1.date = dateadd('day', -1, t2.date)
where
    t1.coingecko_id != 'x0'  -- x0 is a memecoin with a stupidly large total supply (1^76)
    and t2.token_price_usd is not null
