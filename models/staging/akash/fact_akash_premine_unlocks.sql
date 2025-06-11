select
    date,
    pre_mine_unlocks
from {{ source("MANUAL_STATIC_TABLES", "akash_pre_mine_unlocks_data") }}