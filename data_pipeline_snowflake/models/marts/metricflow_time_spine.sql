{{ config(
    meta={
        "metricflow_time_spine": {
            "time_column_name": "date_day",
            "time_granularity": "day"
        }
    }
) }}

-- Time spine required by dbt Semantic Layer / MetricFlow.
-- Grain: 1 row per day

with spine as (
    select
        dateadd(day, seq4(), to_date('1990-01-01')) as date_day
    from table(generator(rowcount => 20000))
)

select
    date_day
from spine
where date_day <= to_date('2030-12-31')