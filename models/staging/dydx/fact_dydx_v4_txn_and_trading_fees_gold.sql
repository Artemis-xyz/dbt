{{ config(materialized="table") }}
select date, trading_fees, txn_fees, app, category, chain
from {{ ref("fact_dydx_v4_txn_and_trading_fees") }}
