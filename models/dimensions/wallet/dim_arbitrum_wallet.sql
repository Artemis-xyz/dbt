{{ config(materialized="table", snowflake_warehouse="ARBITRUM_MD") }}
with
    airdrop as (
        select from_address as address, received_date as airdrop_received_date, airdrop
        from {{ ref("airdrop_counter") }}
        where airdrop = 'arbitrum'
    )
select
    fundamental.address,
    app_used,
    category_used,
    total_gas_spent_usd,
    total_gas_spent_native,
    total_txns,
    distinct_to_address,
    latest_transaction_timestamp,
    first_transaction_timestamp,
    first_app,
    top_app,
    top_to_address,
    first_native_transfer,
    first_native_received,
    first_bridge_used,
    top_from_address,
    first_from_address,
    number_dex_trades,
    distinct_pools,
    total_dex_volume,
    avg_dex_trade,
    distinct_dex_platforms,
    distint_token_out,
    distinct_token_in,
    max_dex_trade,
    distinct_days_traded,
    indodax_transfer_amt,
    simpleswap_transfer_amt,
    hotbit_transfer_amt,
    mexc_transfer_amt,
    bitso_transfer_amt,
    bitget_transfer_amt,
    catex_transfer_amt,
    coindcx_transfer_amt,
    coinbase_transfer_amt,
    swissborg_transfer_amt,
    btse_transfer_amt,
    cryptocom_transfer_amt,
    maskex_transfer_amt,
    woonetwork_transfer_amt,
    lbank_transfer_amt,
    mxc_transfer_amt,
    gateio_transfer_amt,
    fixedfloat_transfer_amt,
    bitfinex_transfer_amt,
    maicoin_transfer_amt,
    phemex_transfer_amt,
    bingx_transfer_amt,
    bitbee_transfer_amt,
    bybit_transfer_amt,
    binance_transfer_amt,
    bitbank_transfer_amt,
    juno_transfer_amt,
    okx_transfer_amt,
    cumberland_transfer_amt,
    kraken_transfer_amt,
    huobi_transfer_amt,
    bilaxy_transfer_amt,
    first_stablecoin_to_address,
    first_stablecoin_from_address,
    avg_stablecoin_send,
    avg_stablecoin_received,
    top_stablecoin_to_address,
    top_stablecoin_from_address,
    number_of_stablecoin_transfers_txns,
    number_of_stablecoin_received_txns,
    unique_count_to_address,
    unique_count_from_address,
    first_stablecoin_transfer_date,
    latest_stablecoin_transfer_date,
    first_stablecoin_received_date,
    latest_stablecoin_received_date
from {{ ref("dim_arbitrum_wallets_fundamental_metrics") }} as fundamental
full join
    {{ ref("dim_arbitrum_wallets_stablecoin_metrics") }} as stablecoin
    on fundamental.address = stablecoin.address
full join
    {{ ref("dim_arbitrum_wallets_dex_trade") }} as dex
    on fundamental.address = dex.address
full join
    {{ ref("dim_arbitrum_wallets_cex_funded") }} as cex
    on fundamental.address = cex.address
full join airdrop on fundamental.address = airdrop.address
