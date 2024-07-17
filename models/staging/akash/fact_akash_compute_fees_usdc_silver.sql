-- depends_on: {{ source("PROD_LANDING", "raw_akash_compute_fees_usdc") }}

{{ flatten_cloudmos_json("raw_akash_compute_fees_usdc", "compute_fees_usdc") }}
