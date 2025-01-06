To convert the provided PostgreSQL query to Spark SQL, here is the equivalent code and explanation of the main differences:

### Spark SQL Equivalent
```sql
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
```

### Key Differences and Explanations
1. **ENUM Replacement**:
   - PostgreSQL supports ENUM types directly. Spark SQL does not, so we use `STRING` to represent ENUM values or create a separate table/view to simulate ENUM behavior.

2. **Composite Types**:
   - PostgreSQL allows composite types (e.g., `films_struct`). In Spark SQL, we use `STRUCT` within an `ARRAY` or other collections to achieve similar functionality.

3. **Default Values**:
   - Spark SQL supports `DEFAULT` for columns in Delta Lake, but it is less flexible than PostgreSQL. Ensure to use compatible default values.

4. **Window Functions**:
   - Both PostgreSQL and Spark SQL support window functions, but the syntax for `LEAD` and `LAG` is similar.

5. **Data Types**:
   - PostgreSQL types like `TEXT` are mapped to `STRING` in Spark SQL. Similarly, `BOOLEAN` and `DATE` are directly supported in Spark SQL.

6. **Delta Lake**:
   - The `USING DELTA` clause specifies that the table is a Delta Lake table, which supports ACID transactions and schema enforcement similar to PostgreSQL.

7. **Struct and Array Types**:
   - PostgreSQL arrays are replaced by Spark SQL's `ARRAY` type with `STRUCT` for nested data.

This translation assumes the use of Delta Lake for transactional capabilities in Spark SQL. If you need more details or additional transformations, let me know!