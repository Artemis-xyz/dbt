{{ config(materialized="view") }}

WITH
raw AS (
    SELECT
        block_date::date AS date
        , sum(coalesce(fees_usd, 0)) AS trading_fees
    FROM osmosis_flipside.defi.fact_pool_fee_day
    WHERE
        currency NOT IN (
            'ibc/A23E590BA7E0D808706FB5085A449B3B9D6864AE4DDE7DAF936243CEBB2A3D43'
            , 'ibc/5F5B7DA5ECC80F6C7A8702D525BB0B74279B1F7B8EFAE36E423D68788F7F39FF'
            , 'factory/osmo1z0qrq605sjgcqpylfl4aa6s90x738j7m58wyatt0tdzflg2ha26q67k743/wbtc'
            , 'factory/osmo1q77cw0mmlluxu0wr29fcdd0tdnh78gzhkvhe4n6ulal9qvrtu43qtd0nh8/wiha'
            , 'factory/osmo19hdqma2mj0vnmgcxag6ytswjnr8a3y07q7e70p/wLIBRA'
            , 'factory/osmo1q77cw0mmlluxu0wr29fcdd0tdnh78gzhkvhe4n6ulal9qvrtu43qtd0nh8/turd'
        )
    AND fees_usd < 1000000 -- filter out mispriced trades
    GROUP BY date
)

SELECT
    raw.date
    , 'osmosis' AS chain
    , raw.trading_fees
FROM raw
WHERE raw.date < to_date(sysdate())
