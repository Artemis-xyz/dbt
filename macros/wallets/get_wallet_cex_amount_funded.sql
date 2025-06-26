{% macro get_wallet_cex_amount_funded(chain) %}
    select
        origin_to_address as address,
        coalesce(
            sum(case when artemis_application_id like 'indodax' then amount_usd end), 0
        ) as indodax_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'simpleswap' then amount_usd end), 0
        ) as simpleswap_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'hotbit' then amount_usd end), 0
        ) as hotbit_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'mexc' then amount_usd end), 0
        ) as mexc_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'bitso' then amount_usd end), 0
        ) as bitso_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'bitget' then amount_usd end), 0
        ) as bitget_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'catex' then amount_usd end), 0
        ) as catex_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'coindcx' then amount_usd end), 0
        ) as coindcx_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'coinbase' then amount_usd end), 0
        ) as coinbase_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'swissborg' then amount_usd end), 0
        ) as swissborg_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'btse' then amount_usd end), 0
        ) as btse_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'crypto.com' then amount_usd end), 0
        ) as cryptocom_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'maskex' then amount_usd end), 0
        ) as maskex_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'woo network' then amount_usd end), 0
        ) as woonetwork_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'lbank' then amount_usd end), 0
        ) as lbank_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'mxc' then amount_usd end), 0
        ) as mxc_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'gate.io' then amount_usd end), 0
        ) as gateio_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'fixedfloat' then amount_usd end), 0
        ) as fixedfloat_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'bitfinex' then amount_usd end), 0
        ) as bitfinex_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'maicoin' then amount_usd end), 0
        ) as maicoin_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'phemex' then amount_usd end), 0
        ) as phemex_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'bingx' then amount_usd end), 0
        ) as bingx_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'bitbee' then amount_usd end), 0
        ) as bitbee_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'bybit' then amount_usd end), 0
        ) as bybit_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'binance' then amount_usd end), 0
        ) as binance_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'bitbank' then amount_usd end), 0
        ) as bitbank_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'juno' then amount_usd end), 0
        ) as juno_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'okx' then amount_usd end), 0
        ) as okx_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'cumberland' then amount_usd end), 0
        ) as cumberland_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'kraken' then amount_usd end), 0
        ) as kraken_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'huobi' then amount_usd end), 0
        ) as huobi_transfer_amt,
        coalesce(
            sum(case when artemis_application_id like 'bilaxy' then amount_usd end), 0
        ) as bilaxy_transfer_amt
    from {{ chain }}_flipside.core.ez_native_transfers et
    inner join
        pc_dbt_db.prod.dim_all_addresses_labeled_gold as dl on dl.address = et.origin_from_address
    where artemis_sub_category_id like 'cex' and chain = '{{ chain }}'
    group by 1
{% endmacro %}
