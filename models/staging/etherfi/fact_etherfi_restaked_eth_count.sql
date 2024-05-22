{{ config(materialized="table") }}

{{flatten_cloudmos_json("raw_etherfi_restaked_eth_count", "total_supply")}}