{{ config(materialized="table") }}
select
    date,
    chain,
    total_utxo_value,
    utxo_value_under_1d,
    utxo_value_1d_1w,
    utxo_value_1w_1m,
    utxo_value_1m_3m,
    utxo_value_3m_6m,
    utxo_value_6m_12m,
    utxo_value_1y_2y,
    utxo_value_2y_3y,
    utxo_value_3y_5y,
    utxo_value_5y_7y,
    utxo_value_7y_10y,
    utxo_value_greater_10y
from {{ ref("fact_bitcoin_hodl_wave") }}
