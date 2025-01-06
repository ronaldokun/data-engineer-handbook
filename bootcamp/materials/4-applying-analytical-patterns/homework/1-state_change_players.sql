WITH
    yesterday AS (
        SELECT *
        FROM players
        WHERE
            current_season = 2001
    ),
    today AS (
        SELECT
            player_name,
            height,
            college,
            country,
            draft_year,
            draft_round,
            draft_number,
            season,
            gp,
            pts,
            reb,
            ast
        FROM player_seasons
        WHERE
            season = 2002
    )

INSERT INTO
    players (
        player_name,
        height,
        college,
        country,
        draft_year,
        draft_round,
        draft_number,
        season_stats,
        current_season
    )
    -- Step 2: Merge yesterday and today, and insert into players
SELECT
    -- Merge player information
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
    COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(
        t.draft_number,
        y.draft_number
    ) AS draft_number,
    -- Build or extend season_stats array
    CASE
        WHEN y.season_stats IS NULL THEN ARRAY[
            ROW (
                t.season,
                t.gp,
                t.pts,
                t.reb,
                t.ast
            )::season_stats
        ] -- Initialize array for new players
        WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[
            ROW (
                t.season,
                t.gp,
                t.pts,
                t.reb,
                t.ast
            )::season_stats
        ] -- Append to existing array
        ELSE y.season_stats -- Retain existing stats for players without new data
    END AS season_stats,

-- Update current season
COALESCE(
    t.season,
    y.current_season + 1
) AS current_season
FROM today t
    FULL OUTER JOIN yesterday y ON t.player_name = y.player_name;

--iterate the current_season for all years
WITH
    yesterday AS (
        SELECT player_name
        FROM players
        WHERE
            current_season = 2000
    ),
    today AS (
        SELECT player_name
        FROM players
        WHERE
            current_season = 2001
    )
SELECT
    COALESCE(y.player_name, t.player_name) AS player_name,
    CASE
        WHEN y.player_name IS NULL
        AND t.player_name IS NOT NULL THEN 'New' -- Entered the league
        WHEN y.player_name IS NOT NULL
        AND t.player_name IS NULL THEN 'Retired' -- Left the league
        WHEN y.player_name IS NOT NULL
        AND t.player_name IS NOT NULL THEN 'Continued Playing' -- Stayed in the league
        WHEN y.player_name IS NULL
        AND t.player_name IS NOT NULL
        AND EXISTS (
            SELECT 1
            FROM players p
            WHERE
                p.player_name = t.player_name
                AND p.current_season < 2000
        ) THEN 'Returned from Retirement' -- Came out of retirement
        WHEN y.player_name IS NULL
        AND t.player_name IS NULL
        AND EXISTS (
            SELECT 1
            FROM players p
            WHERE
                p.player_name = y.player_name
                AND p.current_season < 2000
        ) THEN 'Stayed Retired' -- Stayed out of the league
    END AS status
FROM yesterday y
    FULL OUTER JOIN today t ON y.player_name = t.player_name;