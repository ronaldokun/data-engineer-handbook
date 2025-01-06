WITH
    -- Extract relevant game data, including wins for home and visitor teams
    season_games AS (
        SELECT
            game_id,
            season,
            home_team_id,
            visitor_team_id,
            home_team_wins,
            CASE
                WHEN home_team_wins = 1 THEN home_team_id -- Home team wins
                ELSE visitor_team_id -- Visitor team wins
            END AS winning_team_id
        FROM games
    ),
    -- Aggregate wins for each team
    team_wins AS (
        SELECT
            winning_team_id AS team_id,
            COUNT(*) AS total_wins
        FROM season_games
        GROUP BY winning_team_id
    ),
    -- Combine team wins with other aggregated data
    grouped_data AS (
        SELECT
            COALESCE(player_name, 'All Players') AS player_name,
            COALESCE(team_abbreviation, 'All Teams') AS team_abbreviation,
            COALESCE(season_games.season, 1) AS season,
            COUNT(DISTINCT game_id) AS games_played,
            SUM(pts) AS total_points,
            SUM(reb) AS total_rebounds,
            SUM(ast) AS total_assists,
            SUM(plus_minus) AS total_plus_minus,
            COALESCE(tw.total_wins, 0) AS total_wins, -- Include total wins
            GROUPING(player_name) AS is_player_aggregated,
            GROUPING(team_abbreviation) AS is_team_aggregated,
            GROUPING(season_games.season) AS is_season_aggregated
        FROM game_details
        LEFT JOIN season_games USING (game_id)
        LEFT JOIN team_wins tw ON game_details.team_id = tw.team_id
        GROUP BY
            GROUPING SETS (
                (player_name, team_abbreviation), -- Player and team
                (player_name, season), -- Player and season
                (team_abbreviation, tw.total_wins) -- Team only
            )
        ORDER BY
            is_player_aggregated,
            is_team_aggregated,
            is_season_aggregated,
            total_wins DESC, -- Order by total wins
            total_points DESC
    )

--who scored the most points playing for one team?
SELECT player_name, team_abbreviation, total_points
FROM grouped_data
WHERE is_player_aggregated = 0 AND is_team_aggregated = 0 AND is_season_aggregated = 1 AND total_points IS NOT NULL
LIMIT 1 -- Get the player with the highest points for a single team
