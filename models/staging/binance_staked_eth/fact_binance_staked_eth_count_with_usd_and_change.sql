{{ config(materialized="table") }}

{{ calc_staked_eth('fact_binance_staked_eth_count') }}
