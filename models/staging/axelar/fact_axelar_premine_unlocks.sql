select
    date,
    pre_mine_unlocks_native
from {{ source("MANUAL_STATIC_TABLES", "axelar_premine_unlocks_data") }}