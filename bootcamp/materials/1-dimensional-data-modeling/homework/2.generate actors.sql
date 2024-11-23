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
                ARRAY_AGG(ROW(film, votes, rating, filmid)::films_struct ORDER BY rating DESC) AS films,
                asofyear AS current_year -- Explicitly set the current year
            FROM actor_films
            WHERE year = asofyear
            GROUP BY actorid, actor
        ),
        unnested_films AS (
            SELECT
                actorid,
                actor,
                current_year,
                UNNEST(films) AS film_details
            FROM this_year
        ),
        computed_quality_class AS (
            SELECT
                actorid,
                actor,
                AVG((film_details).rating) AS avg_rating, -- Extract rating from the UNNESTed struct
                CASE 
                    WHEN AVG((film_details).rating) > 8 THEN 'star'
                    WHEN AVG((film_details).rating) > 7 THEN 'good'
                    WHEN AVG((film_details).rating) > 6 THEN 'average'
                    ELSE 'bad'
                END::quality_rating AS quality_class
            FROM unnested_films
            GROUP BY actorid, actor
        )
        INSERT INTO actors (actorid, actor, films, quality_class, is_active, current_year)
        SELECT 
            COALESCE(ty.actorid, ly.actorid) AS actorid,
            COALESCE(ty.actor, ly.actor) AS actor,
            CASE 
                WHEN ty.films IS NULL THEN ly.films
                WHEN ly.films IS NULL THEN ty.films
                ELSE ly.films || ty.films
            END AS films, -- Append films from this year to last year's films
            COALESCE(cqc.quality_class, ly.quality_class) AS quality_class,
            ty.current_year IS NOT NULL AS is_active, -- Actor is active if they appear in this year's data
            COALESCE(ty.current_year, ly.current_year + 1) AS current_year
        FROM last_year ly
        FULL OUTER JOIN this_year ty ON ly.actorid = ty.actorid
        LEFT JOIN computed_quality_class cqc ON ty.actorid = cqc.actorid;
    END LOOP;
END $$;
