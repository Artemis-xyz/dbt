{{ config(materialized="table") }}

{{ detect_sybil("avalanche") }}
