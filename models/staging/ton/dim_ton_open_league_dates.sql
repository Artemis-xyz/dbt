{{ config(materialized="table") }}
select season, end_date
from
    (
        values
            (
                4,
                '2024-06-26'
            ),
            (
                5,
                '2024-08-07'
            )
    ) as results(season, end_date)
