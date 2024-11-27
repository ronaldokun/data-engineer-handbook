CREATE TABLE players_scd (
	player_name TEXT,
	scoring_class scoring_class,
	is_active BOOLEAN,
	start_season INTEGER,
	end_date INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name, current_season)		
);

WITH with_previous AS (
	SELECT
		player_name,
		current_season
		scoring_class,
		is_active,
		LAG(scoring_class) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
		LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active,
	FROM players
)



