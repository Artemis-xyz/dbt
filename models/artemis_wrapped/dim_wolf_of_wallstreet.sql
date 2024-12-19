{{config(materialized='table')}}

select lower(address) as address, 'WOLF_OF_WALL_STREET' as category, array_agg(distinct symbol) as reason
from {{ ref('agg_base_tokens_held') }}
where 
    (symbol = 'DEGEN' and first_seen < '2024-03-24')
    or (symbol = 'PEPE' and first_seen < '2024-11-23')
    or (symbol = 'MIGGLES' and first_seen < '2024-11-12')
    or (symbol = 'TOSHI' and first_seen < '2024-11-06')
    or (symbol = 'KEYCAT' and first_seen < '2024-11-04')
    or (symbol = 'AKUMA' and first_seen < '2024-12-12')
    or (symbol = 'AEROBUD' and first_seen < '2024-11-29')
    or (symbol = 'HENLO' and first_seen < '2024-12-02')
    or (symbol = 'DOGINME' and first_seen < '2024-09-30')
    or (symbol = 'SKI' and first_seen < '2024-11-25')
    or (symbol = 'BRETT' and first_seen < '2024-11-07')
group by 1

union all

select lower(address) as address, 'WOLF_OF_WALL_STREET' as category, array_agg(distinct symbol) as reason
from {{ ref('agg_solana_tokens_held') }}
where 
    (symbol = 'BONK' and first_seen < '2024-10-26')
    or (symbol = 'WIF' and first_seen < '2024-02-22')
    or (symbol = 'PNUT' and first_seen < '2024-09-10')
    or (symbol = 'POPCAT' and first_seen < '2024-06-22')
    or (symbol = 'FARTCOIN' and first_seen < '2024-12-07')
    or (symbol = 'AI16Z' and first_seen < '2024-12-12')
    or (symbol = 'GOAT' and first_seen < '2024-11-25')
    or (symbol = 'MEW' and first_seen < '2024-09-01')
    or (symbol = 'GIGA' and first_seen < '2024-09-30')
    or (symbol = 'BOME' and first_seen < '2024-03-14')
    or (symbol = 'ACT' and first_seen < '2024-11-07')
    or (symbol = 'ZEREBRO' and first_seen < '2024-11-10')
    or (symbol = 'MOODENG' and first_seen < '2024-11-04')
    or (symbol = 'CHILLGUY' and first_seen < '2024-11-18')
    or (symbol = 'FWOG' and first_seen < '2024-10-26')

group by 1