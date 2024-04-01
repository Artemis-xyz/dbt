select address, name, coalesce(new_namespace, namespace) namespace, chain
from {{ source("SIGMA", "sigma_tagged_scanner_contracts") }}
