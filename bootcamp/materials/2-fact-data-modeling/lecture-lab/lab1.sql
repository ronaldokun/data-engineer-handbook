INSERT INTO fct_game_details

WITH deduped AS (
	SELECT 
	g.game_date_est,
	g.season,
	g.home_team_id,
	gd.*,
	ROW_NUMBER() OVER (PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY g.game_date_est) AS row_num
	FROM game_details gd
		JOIN games g ON gd.game_id = g.game_id
)


SELECT 
	game_date_est AS dim_game_date,
	season AS dim_season, 
	team_id AS dim_team_id,
	player_id AS dim_player_id, 
	player_name AS dim_player_name,
   start_position AS dim_start_position,
	team_id = home_team_id AS dim_is_playing_at_home,
	COALESCE(POSITION('DNP' IN comment), 0) > 0 AS dim_did_not_play,
	COALESCE(POSITION('DND' IN comment), 0) > 0 AS dim_did_not_dress,
	COALESCE(POSITION('NWT' IN comment), 0) > 0 AS dim_not_with_team,
	CAST(split_part(min, ':', 1) AS REAL) + CAST(split_part(min, ':', 2) AS REAL)/60 AS m_minutes, 
	fgm as m_fgm, 
	fga as m_fga, 
	fg3m AS m_fg3m,
	fg3a AS m_fg3a,
	ftm as m_ftm,
	fta as m_fta,
	oreb as m_oreb,
	dreb as m_dreb,
	reb as m_reb,
	ast as m_ast,
	stl as m_stl,
	blk as m_blk,
	"TO" AS m_turnovers,
	pf AS m_pf,
	pts AS m_pts,
	plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1;

CREATE TABLE fct_game_details (
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_id INTEGER,
	dim_player_id INTEGER,
	dim_player_name TEXT, 
	dim_start_position TEXT,
	dim_is_playing_at_home BOOLEAN,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	m_minutes REAL,
	m_fgm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER, 
	m_fg3a INTEGER,
	m_ftm INTEGER,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER, 
	m_reb INTEGER, 
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER, 
	m_turnovers INTEGER,
	m_pf INTEGER,
	m_pts INTEGER,
	m_plus_minus INTEGER,
	PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
);

SELECT 
	dim_player_name,
	dim_is_playing_at_home,
	count(1) AS num_games,
	sum(m_pts) AS total_points,
	count(CASE WHEN dim_not_with_team THEN 1 END) AS bailed_num, 
	cast(count(CASE WHEN dim_not_with_team THEN 1 END) AS REAL)/count(1) AS bail_pct
FROM fct_game_details
GROUP BY 1,2
ORDER BY 6 DESC;
	   