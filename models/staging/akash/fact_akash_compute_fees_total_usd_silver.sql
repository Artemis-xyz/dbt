-- depends_on: {{ source("PROD_LANDING", "raw_akash_compute_fees_total_usd") }}

{{
    flatten_cloudmos_json(
        "raw_akash_compute_fees_total_usd", "compute_fees_total_usd"
    )
}}
