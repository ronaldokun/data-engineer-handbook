INSERT INTO actors_history_scd (actorid, actor, quality_class, is_active, start_date, end_date, is_current)
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    LEAD(start_date) OVER (PARTITION BY actorid ORDER BY start_date) - 1 AS end_date,
    CASE 
        WHEN LEAD(start_date) OVER (PARTITION BY actorid ORDER BY start_date) IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current
FROM (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        MAKE_DATE(current_year,1,1) AS start_date
    FROM actors
    ORDER BY actorid, current_year
) ordered_data;