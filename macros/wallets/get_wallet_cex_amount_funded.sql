{% macro get_wallet_cex_amount_funded(chain) %}
    select
        origin_to_address as address,
        coalesce(
            sum(case when project_name like 'indodax' then amount_usd end), 0
        ) as indodax_transfer_amt,
        coalesce(
            sum(case when project_name like 'simpleswap' then amount_usd end), 0
        ) as simpleswap_transfer_amt,
        coalesce(
            sum(case when project_name like 'hotbit' then amount_usd end), 0
        ) as hotbit_transfer_amt,
        coalesce(
            sum(case when project_name like 'mexc' then amount_usd end), 0
        ) as mexc_transfer_amt,
        coalesce(
            sum(case when project_name like 'bitso' then amount_usd end), 0
        ) as bitso_transfer_amt,
        coalesce(
            sum(case when project_name like 'bitget' then amount_usd end), 0
        ) as bitget_transfer_amt,
        coalesce(
            sum(case when project_name like 'catex' then amount_usd end), 0
        ) as catex_transfer_amt,
        coalesce(
            sum(case when project_name like 'coindcx' then amount_usd end), 0
        ) as coindcx_transfer_amt,
        coalesce(
            sum(case when project_name like 'coinbase' then amount_usd end), 0
        ) as coinbase_transfer_amt,
        coalesce(
            sum(case when project_name like 'swissborg' then amount_usd end), 0
        ) as swissborg_transfer_amt,
        coalesce(
            sum(case when project_name like 'btse' then amount_usd end), 0
        ) as btse_transfer_amt,
        coalesce(
            sum(case when project_name like 'crypto.com' then amount_usd end), 0
        ) as cryptocom_transfer_amt,
        coalesce(
            sum(case when project_name like 'maskex' then amount_usd end), 0
        ) as maskex_transfer_amt,
        coalesce(
            sum(case when project_name like 'woo network' then amount_usd end), 0
        ) as woonetwork_transfer_amt,
        coalesce(
            sum(case when project_name like 'lbank' then amount_usd end), 0
        ) as lbank_transfer_amt,
        coalesce(
            sum(case when project_name like 'mxc' then amount_usd end), 0
        ) as mxc_transfer_amt,
        coalesce(
            sum(case when project_name like 'gate.io' then amount_usd end), 0
        ) as gateio_transfer_amt,
        coalesce(
            sum(case when project_name like 'fixedfloat' then amount_usd end), 0
        ) as fixedfloat_transfer_amt,
        coalesce(
            sum(case when project_name like 'bitfinex' then amount_usd end), 0
        ) as bitfinex_transfer_amt,
        coalesce(
            sum(case when project_name like 'maicoin' then amount_usd end), 0
        ) as maicoin_transfer_amt,
        coalesce(
            sum(case when project_name like 'phemex' then amount_usd end), 0
        ) as phemex_transfer_amt,
        coalesce(
            sum(case when project_name like 'bingx' then amount_usd end), 0
        ) as bingx_transfer_amt,
        coalesce(
            sum(case when project_name like 'bitbee' then amount_usd end), 0
        ) as bitbee_transfer_amt,
        coalesce(
            sum(case when project_name like 'bybit' then amount_usd end), 0
        ) as bybit_transfer_amt,
        coalesce(
            sum(case when project_name like 'binance' then amount_usd end), 0
        ) as binance_transfer_amt,
        coalesce(
            sum(case when project_name like 'bitbank' then amount_usd end), 0
        ) as bitbank_transfer_amt,
        coalesce(
            sum(case when project_name like 'juno' then amount_usd end), 0
        ) as juno_transfer_amt,
        coalesce(
            sum(case when project_name like 'okx' then amount_usd end), 0
        ) as okx_transfer_amt,
        coalesce(
            sum(case when project_name like 'cumberland' then amount_usd end), 0
        ) as cumberland_transfer_amt,
        coalesce(
            sum(case when project_name like 'kraken' then amount_usd end), 0
        ) as kraken_transfer_amt,
        coalesce(
            sum(case when project_name like 'huobi' then amount_usd end), 0
        ) as huobi_transfer_amt,
        coalesce(
            sum(case when project_name like 'bilaxy' then amount_usd end), 0
        ) as bilaxy_transfer_amt
    from {{ chain }}_flipside.core.ez_eth_transfers et
    inner join
        {{ chain }}_flipside.core.dim_labels dl on dl.address = et.origin_from_address
    where label_type like 'cex'
    group by 1
{% endmacro %}
