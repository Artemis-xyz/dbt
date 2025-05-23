with a as (
    SELECT
        a.value:date::date as date
        , a.value:distributed::number as distributed
        , a.value:escrowed::number as escrowed
        , a.value:total::number as total
        , a.value:undistributed::number as undistributed
        , row_number() over (partition by date order by extraction_date desc) as rn
    FROM
    landing_database.prod_landing.raw_ripple_supply_data,
    lateral flatten (input => parse_json(source_json)['rows']) a
    QUALIFY rn = 1
    ORDER BY date DESC
)
, date_spine as (
    SELECT
        date
    FROM pc_dbt_db.prod.dim_date_spine
    WHERE date between (SELECT MIN(date) FROM a) AND to_date(sysdate())
)
, sparse as (
    SELECT
        ds.date,
        escrowed,
        distributed,
        undistributed,
        total
    FROM date_spine ds
    LEFT JOIN  pc_dbt_db.prod.fact_ripple_supply_data using(date)
)
SELECT
    date,
    COALESCE(LAST_VALUE(s.escrowed ignore nulls) over (order by s.date rows between unbounded preceding and current row), 0) as escrowed,
    COALESCE(LAST_VALUE(s.distributed ignore nulls) over (order by s.date rows between unbounded preceding and current row), 0) as distributed,
    COALESCE(LAST_VALUE(s.undistributed ignore nulls) over (order by s.date rows between unbounded preceding and current row), 0) as undistributed,
    COALESCE(LAST_VALUE(s.total ignore nulls) over (order by s.date rows between unbounded preceding and current row), 0) as total
from sparse s