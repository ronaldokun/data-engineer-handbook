CREATE
OR
REPLACE
    TEMP VIEW films_struct AS
SELECT
    CAST(NULL AS STRING) AS film,
    CAST(NULL AS INT) AS votes,
    CAST(NULL AS INT) AS rating,
    CAST(NULL AS STRING) AS filmid;

-- ENUM type equivalent: Use a CASE expression or a lookup table
-- Spark SQL doesn't have ENUM types directly
CREATE
OR
REPLACE
    TEMP VIEW quality_rating AS
SELECT 'star' AS rating
UNION ALL
SELECT 'good'
UNION ALL
SELECT 'average'
UNION ALL
SELECT 'bad';

-- Create the actors table using Delta Lake
CREATE TABLE actors (
    actorid STRING,
    actor STRING,
    films ARRAY<STRUCT<film: STRING, votes: INT, rating: INT, filmid: STRING>>,
    quality_class STRING, -- Use STRING to represent ENUM
    is_active BOOLEAN DEFAULT false,
    current_year INT
)
USING DELTA;

-- Create the actors history table
CREATE TABLE actors_history_scd (
    actorid STRING,
    actor STRING,
    quality_class STRING,
    is_active BOOLEAN,
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN DEFAULT true
) USING DELTA;
-- Create a temporary view for the current SCD data
CREATE
OR
REPLACE
    TEMP VIEW current_scd AS
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    is_current
FROM actors_history_scd
WHERE
    is_current = true;

-- Create a temporary view for the new data
CREATE
OR
REPLACE
    TEMP VIEW new_data AS
SELECT
    a.actorid,
    a.actor,
    a.quality_class,
    a.is_active,
    MAKE_DATE (a.current_year, 1, 1) AS start_date,
    NULL AS end_date,
    true AS is_current
FROM actors a;

-- Combine current SCD and new data, then calculate end_date and is_current
CREATE
OR
REPLACE
    TEMP VIEW merged_data AS
SELECT
    n.actorid,
    n.actor,
    n.quality_class,
    n.is_active,
    n.start_date,
    DATE_SUB(
        LEAD(n.start_date, 1) OVER (
            PARTITION BY
                n.actorid
            ORDER BY n.start_date
        ),
        1
    ) AS end_date,
    CASE
        WHEN LEAD(n.start_date, 1) OVER (
            PARTITION BY
                n.actorid
            ORDER BY n.start_date
        ) IS NULL THEN true
        ELSE false
    END AS is_current
FROM (
        SELECT *
        FROM current_scd
        UNION ALL
        SELECT *
        FROM new_data
    ) n;

-- Merge the new data into the SCD table
MERGE INTO actors_history_scd AS target USING merged_data AS source ON target.actorid = source.actorid
AND target.start_date = source.start_date WHEN MATCHED THEN
UPDATE
SET
    actor = source.actor,
    quality_class = source.quality_class,
    is_active = source.is_active,
    start_date = source.start_date,
    end_date = source.end_date,
    is_current = source.is_current WHEN NOT MATCHED THEN
INSERT (
        actorid,
        actor,
        quality_class,
        is_active,
        start_date,
        end_date,
        is_current
    )
VALUES (
        source.actorid,
        source.actor,
        source.quality_class,
        source.is_active,
        source.start_date,
        source.end_date,
        source.is_current
    );