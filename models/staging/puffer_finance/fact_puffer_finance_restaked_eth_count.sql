{{ config(materialized="table") }}

{{flatten_cloudmos_json("raw_puffer_finance_restaked_eth_count", "total_supply")}}