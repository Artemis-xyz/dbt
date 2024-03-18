-- depends_on: {{ source("PROD_LANDING", "raw_akash_compute_fees_native") }}
{{ flatten_cloudmos_json("raw_akash_compute_fees_native", "compute_fees_native") }}
