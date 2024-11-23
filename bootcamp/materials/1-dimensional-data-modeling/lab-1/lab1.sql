-- select * from player_seasons ps;
--
-- create type season_stats as ( 
-- season integer,
-- gp integer, 
-- pts real, 
-- reb real,
-- ast real
-- );
--
-- create table players (
-- player_name text,
-- height text,
-- college text,
-- country text,
-- draft_year text,
-- draft_round text,
-- draft_number text,
-- season_stats season_stats[],
-- current_season integer,
-- primary key(player_name, current_season)
-- );

with yesterday as (
	select * from players 
	where current_season = 2000

), today as (
		select * from player_seasons
		where season = 2001
		)

		
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
    case 
        when y.season_stats is null
            then array[row(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats] -- if there is no season_stats, then we need to create an array with one element
        when t.season is not null 
            then y.season_stats || array[row(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats] -- if there is a season_stats and the today season is not null, then we need to append the new element to the array
        else y.season_stats -- otherwise, we just return the season_stats from yesterday. For example, a retired player would not have a seasons array
    end as season_stats,
    coalesce(t.season, y.current_season + 1) as current_season
from today t full outer join yesterday y
	on t.player_name = y.player_name;

with unnested as (
	select player_name,
		unnest(season_stats)::season_stats as season_stats
	from players
	where current_season = 2001
	and player_name = 'Michael Jordan'
)
	
select player_name,
	(season_stats::season_stats).*
from unnested
