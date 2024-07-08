-- depends_on: {{ source("PROD_LANDING", "raw_akash_compute_fees_usdc") }}
{{ config(snowflake_warehouse="AKASH") }}

{{ flatten_cloudmos_json("raw_akash_compute_fees_usdc", "compute_fees_usdc") }}
