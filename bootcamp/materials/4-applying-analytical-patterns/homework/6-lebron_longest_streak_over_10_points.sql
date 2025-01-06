WITH
    lebron_scores AS (
        SELECT
            game_id,
            player_name,
            pts,
            CASE
                WHEN pts > 10 THEN 0
                ELSE 1
            END AS below_10_flag
        FROM game_details
        WHERE
            player_name = 'LeBron James'
    ),
    streak_groups AS (
        SELECT
            game_id,
            player_name,
            pts,
            SUM(below_10_flag) OVER (
                PARTITION BY
                    player_name
                ORDER BY game_id
            ) AS streak_group
        FROM lebron_scores
    ),
    streak_lengths AS (
        SELECT
            player_name,
            streak_group,
            COUNT(*) AS streak_length
        FROM streak_groups
        WHERE
            pts > 10
        GROUP BY
            player_name,
            streak_group
    )

SELECT player_name, MAX(streak_length) AS longest_streak
FROM streak_lengths
GROUP BY
    player_name;