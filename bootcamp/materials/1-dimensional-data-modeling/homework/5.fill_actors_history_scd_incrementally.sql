WITH current_scd AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        start_date,
        end_date,
        is_current
    FROM actors_history_scd
    WHERE is_current = TRUE
),
new_data AS (
    SELECT
        a.actorid,
        a.actor,
        a.quality_class,
        a.is_active,
        MAKE_DATE(a.current_year,1,1) AS start_date,
        NULL::DATE AS end_date,
        TRUE AS is_current
    FROM actors a
),
merged_data AS (
    SELECT
        n.actorid,
        n.actor,
        n.quality_class,
        n.is_active,
        n.start_date,
        -- Use LEAD to calculate end_date for historical records
        LEAD(n.start_date) OVER (PARTITION BY n.actorid ORDER BY n.start_date) - 1 AS end_date,
        -- Mark as current only if there is no subsequent record
        CASE 
            WHEN LEAD(n.start_date) OVER (PARTITION BY n.actorid ORDER BY n.start_date) IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM (
        SELECT * FROM current_scd
        UNION ALL
        SELECT * FROM new_data
    ) n
)
-- Insert updated rows into the SCD table
INSERT INTO actors_history_scd (actorid, actor, quality_class, is_active, start_date, end_date, is_current)
SELECT actorid, actor, quality_class, is_active, start_date, end_date, is_current
FROM merged_data;