{{ config(materialized="table") }}

with avg_vaults_tvl as (
    select
        id,
        avg(tvl) as avg_tvl_l7d
    from {{ ref("fact_kamino_vaults_apy") }}
    where extraction_timestamp >= dateadd(day, -7, current_date)
    group by id
),

vaults_score as (
    select
        f.id,
        f.name,
        case 
            when a.avg_tvl_l7d >= 1e9 then 5.0
            when a.avg_tvl_l7d >= 5e8 then 4.5
            when a.avg_tvl_l7d >= 1e8 then 4.0
            when a.avg_tvl_l7d >= 5e7 then 3.5
            when a.avg_tvl_l7d >= 1e7 then 3.0
            when a.avg_tvl_l7d >= 5e6 then 2.5
            when a.avg_tvl_l7d >= 1e6 then 2.0
            when a.avg_tvl_l7d >= 5e5 then 1.5
            else 1.0
        end as tvl_score,
        f.extraction_timestamp
    from {{ ref("fact_kamino_vaults_apy") }} f
    left join avg_vaults_tvl a on a.id = f.id
    and f.extraction_timestamp = a.extraction_timestamp
    qualify row_number() over (partition by f.id order by extraction_timestamp desc) = 1
),

avg_lending_tvl as (
    select
        id,
        avg(tvl) as avg_tvl_l7d
    from {{ ref("fact_kamino_lending_apy") }}
    where extraction_timestamp >= dateadd(day, -7, current_date)
    group by id
),

lending_score as (
    select
        f.id,
        f.name,
        case 
            when a.avg_tvl_l7d >= 1e9 then 5.0
            when a.avg_tvl_l7d >= 5e8 then 4.5
            when a.avg_tvl_l7d >= 1e8 then 4.0
            when a.avg_tvl_l7d >= 5e7 then 3.5
            when a.avg_tvl_l7d >= 1e7 then 3.0
            when a.avg_tvl_l7d >= 5e6 then 2.5
            when a.avg_tvl_l7d >= 1e6 then 2.0
            when a.avg_tvl_l7d >= 5e5 then 1.5
            else 1.0
        end as tvl_score,
        f.extraction_timestamp
    from {{ ref("fact_kamino_lending_apy") }} f
    left join avg_lending_tvl a on a.id = f.id
    and f.extraction_timestamp = a.extraction_timestamp
    qualify row_number() over (partition by f.id order by extraction_timestamp desc) = 1
)

select
    v.id,
    v.name,
    v.tvl_score,
    v.extraction_timestamp
from vaults_score v
union all
select
    l.id,
    l.name,
    l.tvl_score,
    l.extraction_timestamp
from lending_score l
