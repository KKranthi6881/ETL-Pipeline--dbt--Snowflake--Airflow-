-- Time spine required by dbt Semantic Layer / MetricFlow.
-- Grain: 1 row per day
-- Snowflake implementation using GENERATOR.

with spine as (
    select
        dateadd(day, seq4(), to_date('1990-01-01')) as date_day
    from table(generator(rowcount => 20000))
)

select
    date_day
from spine
where date_day <= to_date('2030-12-31')