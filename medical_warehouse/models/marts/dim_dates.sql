
with date_spine as (
    select
        (current_date - interval '5 years' + (n || ' days')::interval)::date as full_date
    from generate_series(0, 365*10) n
),

date_details as (
    select
        to_char(full_date, 'YYYYMMDD')::int as date_key,
        full_date,
        extract(isodow from full_date) as day_of_week,
        to_char(full_date, 'Day') as day_name,
        extract(week from full_date) as week_of_year,
        extract(month from full_date) as month,
        to_char(full_date, 'Month') as month_name,
        extract(quarter from full_date) as quarter,
        extract(year from full_date) as year,
        case 
            when extract(isodow from full_date) in (6, 7) then true 
            else false 
        end as is_weekend
    from date_spine
)

select * from date_details