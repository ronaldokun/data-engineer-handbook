--A query to deduplicate `game_details` from Day 1 so there's no duplicates

WITH
    ranked_game_details AS (
        SELECT gd.*, ROW_NUMBER() OVER (
                PARTITION BY
                    gd.game_id, gd.team_id, gd.player_id
                ORDER BY gd.game_id
            ) AS row_num
        FROM game_details gd
    )
SELECT *
FROM ranked_game_details
WHERE
    row_num = 1;