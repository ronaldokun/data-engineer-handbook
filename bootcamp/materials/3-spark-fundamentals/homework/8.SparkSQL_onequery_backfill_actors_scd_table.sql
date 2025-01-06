-- Define the STRUCT type for films
CREATE OR REPLACE TEMP VIEW films_struct AS
SELECT 
    CAST(NULL AS STRING) AS film,
    CAST(NULL AS INT) AS votes,
    CAST(NULL AS INT) AS rating,
    CAST(NULL AS STRING) AS filmid;

-- ENUM type equivalent: Use a CASE expression or a lookup table
-- Spark SQL doesn't have ENUM types directly
CREATE OR REPLACE TEMP VIEW quality_rating AS
SELECT 'star' AS rating UNION ALL
SELECT 'good' UNION ALL
SELECT 'average' UNION ALL
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
)
USING DELTA;

-- Insert data into actors_history_scd
INSERT INTO actors_history_scd
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    DATE_SUB(LEAD(start_date, 1) OVER (PARTITION BY actorid ORDER BY start_date), 1) AS end_date,
    CASE
        WHEN LEAD(start_date, 1) OVER (PARTITION BY actorid ORDER BY start_date) IS NULL THEN true
        ELSE false
    END AS is_current
FROM (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        MAKE_DATE(current_year, 1, 1) AS start_date
    FROM actors
) ordered_data;
