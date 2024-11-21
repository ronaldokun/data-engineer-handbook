-- Define the composite type for films
CREATE TYPE films_struct AS (
    film TEXT,
    votes INTEGER, 
    rating INTEGER,
    filmid TEXT
);

-- Define the ENUM type for quality_class
CREATE TYPE quality_rating AS ENUM ('star', 'good', 'average', 'bad');

drop table actors

-- Create the actors table
CREATE TABLE actors (
    actorid TEXT,                      -- Unique identifier for each actor
    actor TEXT NOT NULL,               -- Name of the actor
    films films_struct[],              -- Array of films using the composite type
    quality_class quality_rating,      -- Quality classification
    is_active BOOLEAN,                 -- Whether the actor is active
    current_year INT,                  -- Year of the data
    PRIMARY KEY (actorid, current_year)
);

-- script to insert yearly data iteratively
DO $$
DECLARE
    year_start INT := 1970; -- Starting year
    year_end INT := 2021;   -- Ending year
    asofyear INT;       -- Loop variable for previous year
BEGIN
    -- Loop from the starting year to the ending year
    FOR asofyear IN year_start..year_end LOOP
        WITH last_year AS (
		   SELECT *
		   FROM actors
		   WHERE current_year = asofyear - 1
		), 
		this_year AS (
		   SELECT
		       actorid,
		       actor,
		       asofyear AS current_year,
		       ARRAY_AGG(ROW(film, votes, rating, filmid)::films_struct) AS films
		   FROM actor_films
		   WHERE year = asofyear
		   GROUP BY actorid, actor
		)
     	INSERT INTO actors  --(actorid, actor, films, is_active, current_year)
		SELECT 
		    COALESCE(ty.actorid, ly.actorid) AS actorid,
		    COALESCE(ty.actor, ly.actor) AS actor,
		    COALESCE(ly.films, ARRAY[]::films_struct[]) || 
		        COALESCE(ty.films, ARRAY[]::films_struct[]) AS films,
		    COALESCE(ty.current_year, ly.current_year + 1) AS current_year,
            --check if the actor is active, i.e. it has made a film this year
			CASE WHEN ty.current_year IS NOT NULL THEN TRUE
				ELSE FALSE
			END AS is_active				
		FROM last_year ly
		FULL OUTER JOIN this_year ty
		    ON ly.actorid = ty.actorid;
    END LOOP;
END $$;


-- testing if the backfilling worked
WITH last_year AS (
   SELECT *
   FROM actors
   WHERE current_year = 1969
), 
this_year AS (
   SELECT
       actorid,
       actor,
       1970 AS current_year,
       coalesce(
           ARRAY_AGG(
               ROW(film, votes, rating, filmid)::films_struct
           ) filter (where film is not null), 
           array[]::films_struct[]
       ) as films,
               
       CASE
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 THEN 'good'
            WHEN AVG(rating) > 6 THEN 'average'
            ELSE 'bad'
       end::quality_rating AS quality_class, -- Explicit cast to quality_rating enum
       CASE
            WHEN COUNT(film) > 0 THEN TRUE
            ELSE FALSE
       END AS is_active -- Actor is active if they have films this year
   FROM actor_films
   WHERE year = 1970
   GROUP BY actorid, actor -- Ensure grouping by non-aggregated columns
)
INSERT INTO actors (actorid, actor, films, quality_class, is_active, current_year)

--SELECT actorid, actor, films, quality_class, is_active, current_year
--FROM this_year ty
--where is_active = false;
--SELECT 
--    COALESCE(ty.actorid, ly.actorid) AS actorid,
--    COALESCE(ty.actor, ly.actor) AS actor,
--    COALESCE(ly.films, ARRAY[]::films_struct[]) || 
--    COALESCE(ty.films, ARRAY[]::films_struct[]) AS films,
--    quality_class,
--    is_active,
--    COALESCE(ty.current_year, ly.current_year + 1) AS current_year
--FROM last_year ly
--FULL OUTER JOIN this_year ty
--    ON ly.actorid = ty.actorid;
   
 select * from actor_films af 
 where af."year" = 1970



