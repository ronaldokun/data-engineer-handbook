WITH
    team_wins AS (
        SELECT
            g.game_id,
            g.season,
            g.home_team_id AS team_id,
            CASE
                WHEN g.home_team_win THEN 1
                ELSE 0
            END AS win
        FROM games g
        UNION ALL
        SELECT
            g.game_id,
            g.season,
            g.visitor_team_id AS team_id,
            CASE
                WHEN NOT g.home_team_win THEN 1
                ELSE 0
            END AS win
        FROM games g
    ),
    team_win_streak AS (
        SELECT
            team_id,
            game_id,
            season,
            SUM(win) OVER (
                PARTITION BY
                    team_id
                ORDER BY game_id ROWS BETWEEN 89 PRECEDING
                    AND CURRENT ROW
            ) AS wins_in_90_games
        FROM team_wins
    )
SELECT team_id, MAX(wins_in_90_games) AS max_wins_in_90_games
FROM team_win_streak
GROUP BY
    team_id
ORDER BY max_wins_in_90_games DESC
LIMIT 1;
-- Team with the most wins in a 90-game stretch