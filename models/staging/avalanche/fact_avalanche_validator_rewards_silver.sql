with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_avalanche_validator_rewards") }}
    ),
    data as (
        select
            date(value:date) as date,
            value:"validator_rewards"::float as validator_rewards
        from
            {{ source("PROD_LANDING", "raw_avalanche_validator_rewards") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    ),
    validator_data as (
        select
            date,
            validator_rewards,
            sum(validator_rewards) over (order by date) as cumulative_validator_rewards
        from data
    )
select
    date,
    'avalanche' as chain,
    'validator' as allocation_type,
    validator_rewards,
    cumulative_validator_rewards
from validator_data
where date < to_date(sysdate())
