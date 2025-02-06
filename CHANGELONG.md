# `ncaa_stats_py` Changelog

## 0.0.10: The "Football" update

## 0.0.9: The "Emergency Fix #1" update
- Pushed an emergency fix that would cause the package to catastrophically fail to parse a player's name that is formatted as `{last name}, {suffix}, {first name}`.
- Fixed a bug where `ncaa_stats_py.field_hockey.get_field_hockey_team_schedule()` would generate the incorrect URL for a player.
- Fixed a bug where `ncaa_stats_py.volleyball.get_volleyball_team_schedule()` would generate the incorrect URL for a player.
- Fixed a bug where `ncaa_stats_py.baseball.get_baseball_team_schedule()` would generate the incorrect URL for a player.
- Implemented `ncaa_stats_py.football.get_football_teams()`, a function that allows you to get a list of NCAA football teams for a season and level.
- Implemented `ncaa_stats_py.football.load_football_teams()`, a function that builds on top of `ncaa_stats_py.football.get_football_teams()`, and retrieves a list of all possible football teams from 2008 to present day.
- Implemented `ncaa_stats_py.football.get_football_team_schedule()`, a function that allows you to get a schedule for a specific football team.
- Implemented `ncaa_stats_py.football.get_football_day_schedule()`, a function that allows you to get a football schedule for a specific date, and NCAA level.
- Implemented `ncaa_stats_py.football.get_full_football_schedule()`, a function that builds on top of `ncaa_stats_py.football.get_football_team_schedule()` and allows you to get the entire football schedule for a given season and level.
- Implemented `ncaa_stats_py.football.get_football_team_roster()`, a function that allows one to get a full team roster from a given team ID.

**NOTE**: Other football functions are indeed present, but are not fully implemented and will raise a `NotImplementedError` exception.

- Set the package version to `0.0.9`.

## 0.0.8: The "Convenience" update
- Fixed a bug in `ncaa_stats_py.lacrosse.load_lacrosse_teams()` that would cause the function to crash in certain conditions before games would be played in a calendar year.
- For the following functions, `[player_first_name]` and `[player_last_name]` are now added as columns within the returned `pandas` `DataFrame`:
    - `ncaa_stats_py.baseball.get_baseball_team_roster()`
    - `ncaa_stats_py.basketball.get_basketball_team_roster()`
    - `ncaa_stats_py.field_hockey.get_field_hockey_team_roster()`
    - `ncaa_stats_py.hockey.get_hockey_team_roster()`
    - `ncaa_stats_py.lacrosse.get_lacrosse_team_roster()`
    - `ncaa_stats_py.softball.get_softball_team_roster()`
    - `ncaa_stats_py.volleyball.get_volleyball_team_roster()`
- For `ncaa_stats_py.baseball.get_baseball_game_player_stats()`, a `[game_datetime]` column has been added.
- For the following functions, columns will now be ordered and standardized across function calls:
    - `ncaa_stats_py.baseball.get_baseball_team_roster()`
    - `ncaa_stats_py.baseball.get_baseball_player_season_batting_stats()`
    - `ncaa_stats_py.baseball.get_baseball_player_season_pitching_stats()`
    - `ncaa_stats_py.baseball.get_baseball_player_season_fielding_stats()`
    - `ncaa_stats_py.baseball.get_baseball_player_game_batting_stats()`
    - `ncaa_stats_py.baseball.get_baseball_player_game_pitching_stats()`
    - `ncaa_stats_py.baseball.get_baseball_player_game_fielding_stats()`
    - `ncaa_stats_py.basketball.get_basketball_team_roster()`
    - `ncaa_stats_py.field_hockey.get_field_hockey_team_roster()`
    - `ncaa_stats_py.field_hockey.get_field_hockey_player_season_stats()`
    - `ncaa_stats_py.hockey.get_hockey_player_season_stats()`
- Fixed an issue within `ncaa_stats_py.baseball.get_baseball_player_season_pitching_stats()` where pitching appearances and games played would be combined, regardless of the position(s) the player played in that season.
- Added the ability to get all games for a sport, a season, and an NCAA level/division with the help of these functions:
    - `ncaa_stats_py.baseball.get_baseball_day_schedule()`
    - `ncaa_stats_py.basketball.get_basketball_day_schedule()`
    - `ncaa_stats_py.field_hockey.get_field_hockey_day_schedule()`
    - `ncaa_stats_py.hockey.get_hockey_day_schedule()`
    - `ncaa_stats_py.lacrosse.get_lacrosse_day_schedule()`
    - `ncaa_stats_py.softball.get_softball_day_schedule()`
    - `ncaa_stats_py.volleyball.get_volleyball_day_schedule()`
- Altered the warning message that will appear when team information is not cached, or the cached data needs to be refreshed.
- Added the `python-dateutil` python package to the list of required python packages for `ncaa_stats_py` to install.
- Upped the required version of `beautifulsoup4` from `4.12.2` to `4.12.3`
- Upped the required version of `pytz` from `2024.1` to `2025.1`
- Set the package version to `0.0.8`.

## 0.0.7: The "Volleyball" update
- Implemented `ncaa_stats_py.volleyball.get_volleyball_teams()`, a function that allows you to get a list of NCAA volleyball teams for a season and level.
- Implemented `ncaa_stats_py.volleyball.load_volleyball_teams()`, a function that builds on top of `ncaa_stats_py.volleyball.get_volleyball_teams()`, and retrieves a list of all possible volleyball teams from 2008 to present day.
- Implemented `ncaa_stats_py.volleyball.get_volleyball_team_schedule()`, a function that allows you to get a schedule for a specific volleyball team.
- Implemented `ncaa_stats_py.volleyball.get_full_volleyball_schedule()`, a function that builds on top of `ncaa_stats_py.volleyball.get_volleyball_team_schedule()` and allows you to get the entire volleyball schedule for a given season and level.
- Implemented `ncaa_stats_py.volleyball.get_volleyball_team_roster()`, a function that allows one to get a full team roster from a given team ID.
- Implemented `ncaa_stats_py.volleyball.get_volleyball_player_season_stats()`, a function that allows one to get a the season stats of players from a team through their team ID.
- Implemented `ncaa_stats_py.volleyball.get_volleyball_player_game_stats()`, a function that allows one to get a the game stats of a player given a season, and player ID.
- Implemented `ncaa_stats_py.volleyball.get_volleyball_game_player_stats()`, a function that allows one to get a the game stats of a player given a valid game ID.
- Implemented `ncaa_stats_py.volleyball.get_volleyball_raw_pbp()`, to parse raw play-by-play data, and normalize the data into something that can be parsed at a later date.
- Implemented `ncaa_stats_py.volleyball.get_parsed_volleyball_pbp()`, to parse play-by-play data into a format that can be queried at a later date.
- Fixed an issue related to an impending change/warning in pandas where `.fillna()` will no longer automatically convert columns with an `object` datatype, before a function can convert the column to the correct datatype (More information: https://stackoverflow.com/a/77941616).
- Set the package version to `0.0.7`.

## 0.0.6: The "Lacrosse" update
- Implemented `ncaa_stats_py.lacrosse.get_lacrosse_teams()`, a function that allows you to get a list of NCAA lacrosse teams for a season and level.
- Implemented `ncaa_stats_py.lacrosse.load_lacrosse_teams()`, a function that builds on top of `ncaa_stats_py.lacrosse.get_lacrosse_teams()`, and retrieves a list of all possible lacrosse teams from 2008 to present day.
- Implemented `ncaa_stats_py.lacrosse.get_lacrosse_team_schedule()`, a function that allows you to get a schedule for a specific lacrosse team.
- Implemented `ncaa_stats_py.lacrosse.get_full_lacrosse_schedule()`, a function that builds on top of `ncaa_stats_py.lacrosse.get_lacrosse_team_schedule()` and allows you to get the entire lacrosse schedule for a given season and level.
- Implemented `ncaa_stats_py.lacrosse.get_lacrosse_team_roster()`, a function that allows one to get a full team roster from a given team ID.
- Implemented `ncaa_stats_py.lacrosse.get_lacrosse_player_season_stats()`, a function that allows one to get a the season stats of players from a team through their team ID.
- Implemented `ncaa_stats_py.lacrosse.get_lacrosse_player_game_stats()`, a function that allows one to get a the game stats of a player given a season, and player ID.
- Implemented `ncaa_stats_py.lacrosse.get_lacrosse_game_player_stats()`, a function that allows one to get a the game stats of a player given a valid game ID.
- Implemented `ncaa_stats_py.lacrosse.get_lacrosse_game_team_stats()`, a function that allows one to get a the team game stats given a valid game ID.
- Implemented `ncaa_stats_py.lacrosse.get_lacrosse_raw_pbp()`, to parse play-by-play data, and normalize the data into something that can be parsed at a later date.
- Set the package version to `0.0.6`.

## 0.0.5: The "Softball" update
- Implemented `ncaa_stats_py.softball.get_softball_teams()`, a function that allows you to get a list of NCAA softball teams for a season and level.
- Implemented `ncaa_stats_py.softball.load_softball_teams()`, a function that builds on top of `ncaa_stats_py.softball.get_softball_teams()`, and retrieves a list of all possible softball teams from 2008 to present day.
- Implemented `ncaa_stats_py.softball.get_softball_team_schedule()`, a function that allows you to get a schedule for a specific softball team.
- Implemented `ncaa_stats_py.softball.get_full_softball_schedule()`, a function that builds on top of `ncaa_stats_py.softball.get_softball_team_schedule()` and allows you to get the entire softball schedule for a given season and level.
- Implemented `ncaa_stats_py.softball.get_softball_player_season_batting_stats()`, `ncaa_stats_py.softball.get_softball_player_season_pitching_stats()`, and `ncaa_stats_py.softball.get_softball_player_season_fielding_stats()` to enable a user to get season player stats from an NCAA softball team.
- Implemented `ncaa_stats_py.softball.get_softball_player_game_batting_stats()`, `ncaa_stats_py.softball.get_softball_player_game_pitching_stats()`, and `ncaa_stats_py.softball.get_softball_player_game_fielding_stats()` to enable a user to get game-level player stats from an NCAA softball team.
- Implemented `ncaa_stats_py.softball.get_softball_game_player_stats()` and `ncaa_stats_py.softball.get_softball_game_team_stats()` to get stats from a specific game.
- Implemented `ncaa_stats_py.softball.get_raw_softball_game_pbp()` to play-by-play, and normalize the data into something that can be parsed at a later date.
- Fixed a bug with `ncaa_stats_py.baseball.get_raw_softball_game_pbp()` where data parsed by that function wouldn't get cached.
- Set the package version to `0.0.5`.

## 0.0.4: The "Hockey" update
- Implemented `ncaa_stats_py.hockey.get_hockey_teams()`, a function that allows one to get a list of all hockey teams given a season and a NCAA level.
- Implemented `ncaa_stats_py.hockey.load_hockey_teams()`, a function that allows one to load in every hockey team from a starting year (default is 2009) to present day.
- Implemented `ncaa_stats_py.hockey.get_hockey_team_schedule()`, a function that allows you to get a schedule for a specific hockey team.
- Implemented `ncaa_stats_py.hockey.get_full_hockey_schedule()`, a function that builds on top of `ncaa_stats_py.hockey.get_hockey_team_schedule()` and allows you to get the entire hockey schedule for a given season and level.
- Implemented `ncaa_stats_py.hockey.get_hockey_team_roster()`, a function that allows one to get a full team roster from a given team ID.
- Implemented `ncaa_stats_py.hockey.get_hockey_player_season_stats()`, a function that allows one to get a the season stats of players from a team through their team ID.
- Implemented `ncaa_stats_py.hockey.get_hockey_player_game_stats()`, a function that allows one to get a the game stats of a player given a season, and player ID.
- Implemented `ncaa_stats_py.hockey.get_hockey_game_player_stats()`, a function that allows one to get a the game stats of a player given a valid game ID.
- Implemented `ncaa_stats_py.hockey.get_hockey_game_team_stats()`, a function that allows one to get a the team game stats given a valid game ID.
- Implemented `ncaa_stats_py.hockey.get_hockey_raw_pbp()`, to parse play-by-play data, and normalize the data into something that can be parsed at a later date.
- For `ncaa_stats_py.baseball.get_basketball_raw_pbp()`, additional logic has been added to parse time formatted as `"MM:SS"` as well as `"MM:SS:ms"`.
- Renamed `ncaa_stats_py.field_hockey.get_baseball_game_team_stats()` to `ncaa_stats_py.field_hockey.get_field_hockey_game_team_stats()`
- Fixed the logic in `ncaa_stats_py.field_hockey.get_field_hockey_player_game_stats()` to catch a potential edge case where a player is acknowledge to exist on a field hockey team, but has absolutely zero games played.
- Added additional logic to the following functions to improve the ability to get teams that are transitioning up a level within the NCAA division structure:
    - `ncaa_stats_py.baseball.get_baseball_teams()`
    - `ncaa_stats_py.basketball.get_basketball_teams()`
    - `ncaa_stats_py.field_hockey.get_field_hockey_teams()`
- Set the package version to `0.0.4`.


## 0.0.3: The "Field Hockey" update
- Implemented `ncaa_stats_py.field_hockey.get_field_hockey_teams()`, a function that allows one to get a list of all field hockey teams given a season and a NCAA level.
- Implemented `ncaa_stats_py.field_hockey.load_field_hockey_teams()`, a function that allows one to load in every field hockey team from a starting year (default is 2009) to present day.
- Implemented `ncaa_stats_py.field_hockey.get_field_hockey_team_schedule()`, a function that allows you to get a schedule for a specific field hockey team.
- Implemented `ncaa_stats_py.field_hockey.get_full_field_hockey_schedule()`, a function that builds on top of `ncaa_stats_py.field_hockey.get_field_hockey_team_schedule()` and allows you to get the entire field hockey schedule for a given season and level.
- Implemented `ncaa_stats_py.field_hockey.get_field_hockey_team_roster()`, a function that allows one to get a full team roster from a given team ID.
- Implemented `ncaa_stats_py.field_hockey.get_field_hockey_player_season_stats()`, a function that allows one to get a the season stats of players from a team through their team ID.
- Implemented `ncaa_stats_py.field_hockey.get_field_hockey_player_game_stats()`, a function that allows one to get a the game stats of a player given a season, and player ID.
- Implemented `ncaa_stats_py.field_hockey.get_field_hockey_game_player_stats()`, a function that allows one to get a the game stats of a player given a valid game ID.
- Implemented `ncaa_stats_py.field_hockey.get_field_hockey_game_team_stats()`, a function that allows one to get a the team game stats given a valid game ID.
- Implemented `ncaa_stats_py.field_hockey.get_field_hockey_raw_pbp()`, to parse play-by-play data, and normalize the data into something that can be parsed at a later date.
- Implemented `ncaa_stats_py.utils._get_seconds_from_time_str()` to convert an `"MM:SS"` time string into an integer.
    > EXAMPLE: 
    > Given a string of `"60:01"`, the function would split the string into `"60"` and `"01"`, converting the two into integers (`60` and `1`), multiplying the "minutes" by 60 (`60 * 60 = 2,700`), and then adding any additional seconds left, which is `1` in this case.
    > Meaning, that the returned integer would be `2,701` in this case.
- Redesigned `ncaa_stats_py.utils._stat_id_dict()` to no longer have a `year_id` option within the dictionary.
- Preemptively fixed a potential rounding issue when counting the number of triple doubles and double doubles a player has in `ncaa_stats_py.basketball.get_basketball_game_player_stats()`.
- Fixed a rounding issue found in `ncaa_stats_py.basketball.get_basketball_player_season_stats()` when calculating the total number of seconds a player spent on a court in a game. 
- For `ncaa_stats_py.baseball.get_baseball_game_player_stats()`, any columns previously set to be an unsigned 8-bit integer (`uint8`) has now been set to an unsigned 16-bit integer (`uint16`)
- For `ncaa_stats_py.baseball.get_baseball_game_player_stats()`, any stats that are associated to the team specifically, and not to any player, will have those stats set to a player ID equal to the negative of that team's ID. Previously, `[player_id]` would have been a null value.
    > EXAMPLE: 
    > Team A has a team ID of `123`. If there are stats associated to Team A specifically, and not to any player, those stats will appear in a row where `[player_id]` is set to `-123`.
- For baseball and basketball related functions, the `[player_name]` column has been renamed to `[player_full_name]`.
- For baseball and basketball related functions, the `[player_jersey_num]` column has been renamed to `[player_jersey_num]`.
- Fixed an edge case found in `ncaa_stats_py.baseball.get_baseball_team_schedule()` and `ncaa_stats_py.basketball.get_basketball_team_schedule()` which would cause the function to crash if a scheduled game had an unknown result.
- Set the package version to `0.0.3`.


## 0.0.2: The "Basketball" update
- Implemented `ncaa_stats_py.basketball.get_basketball_teams()`, a function that allows one to get a list of all men's basketball (MBB) or women's basketball (WBB) teams given a season and a NCAA level.
- Implemented `ncaa_stats_py.basketball.load_basketball_teams()`, a function that allows one to load in every basketball team from a starting year (default is 2011) to present day.
- Implemented `ncaa_stats_py.basketball.get_basketball_team_schedule()`, a function that allows you to get a schedule for a specific basketball team.
- Implemented `ncaa_stats_py.basketball.get_full_basketball_schedule()`, a function that builds on top of `ncaa_stats_py.basketball.get_basketball_team_schedule()` and allows you to get the entire basketball schedule for a given season and level.
- Implemented `ncaa_stats_py.basketball.get_basketball_team_roster()`, a function that allows one to get a full team roster from a given team ID.
- Implemented `ncaa_stats_py.basketball.get_basketball_player_season_stats()`, a function that allows one to get a the season stats of players from a team through their team ID.
- Implemented `ncaa_stats_py.basketball.get_basketball_player_game_stats()`, a function that allows one to get a the game stats of a player given a season, and player ID.
- Implemented `ncaa_stats_py.basketball.get_basketball_game_player_stats()`, a function that allows one to get a the game stats of a player given a valid game ID.
- Implemented `ncaa_stats_py.basketball.get_basketball_game_team_stats()`, a function that allows one to get a the team game stats given a valid game ID.
- Implemented `ncaa_stats_py.basketball.get_basketball_raw_pbp()`, to parse play-by-play data, and normalize the data into something that can be parsed at a later date.
- Implemented  `ncaa_stats_py.basketball.get_basketball_game_starters()`, to parse play-by-play data, and find the starters for a given basketball game.
- Fixed an issue found in `ncaa_stats_py.baseball.get_raw_baseball_game_pbp()` where data would not get cached properly due to a misformed folder location.
- Fixed an issue identified in #1 where a try-except-continue was not refactored to a better solution prior to `0.0.1`.
- Fixed an issue in the following functions where a season and/or school ID (values that are integers) would have a trailing `.0` when caching data:
    - `ncaa_stats_py.baseball.get_baseball_player_season_batting_stats()`
    - `ncaa_stats_py.baseball.get_baseball_player_season_pitching_stats()`
    - `ncaa_stats_py.baseball.get_baseball_player_season_fielding_stats()`
- Explicitly cast `school_id` as an `int` for the following functions:
    - `ncaa_stats_py.baseball.get_baseball_team_roster()`
    - `ncaa_stats_py.baseball.get_baseball_player_season_batting_stats()`
    - `ncaa_stats_py.baseball.get_baseball_player_season_pitching_stats()`
    - `ncaa_stats_py.baseball.get_baseball_player_season_fielding_stats()`
- Added a `[sport_id]` column to the all baseball functions.
- `ncaa_stats_py.utils._stat_id_dict()`
    - Added year ID's for baseball.
    - Added year ID's for basketball (both MBB and WBB).
- Added a 10 second timeout for HTML requests made by `ncaa_stats_py.utils._get_webpage()` to fix an issue identified in #2.
- Set the package version to `0.0.2`.

## 0.0.1: The "Baseball" update
- Implemented `ncaa_stats_py.baseball.get_baseball_teams()`, a function that allows you to get a list of NCAA baseball teams for a season and level.
- Implemented `ncaa_stats_py.baseball.load_baseball_teams()`, a function that builds on top of `ncaa_stats_py.baseball.get_baseball_teams()`, and retrieves a list of all possible baseball teams from 2008 to present day.
- Implemented `ncaa_stats_py.baseball.get_baseball_team_schedule()`, a function that allows you to get a schedule for a specific baseball team.
- Implemented `ncaa_stats_py.baseball.get_full_baseball_schedule()`, a function that builds on top of `ncaa_stats_py.baseball.get_baseball_team_schedule()` and allows you to get the entire baseball schedule for a given season and level.
- Implemented `ncaa_stats_py.baseball.get_baseball_player_season_batting_stats()`, `ncaa_stats_py.baseball.get_baseball_player_season_pitching_stats()`, and `ncaa_stats_py.baseball.get_baseball_player_season_fielding_stats()` to enable a user to get season player stats from an NCAA baseball team.
- Implemented `ncaa_stats_py.baseball.get_baseball_player_game_batting_stats()`, `ncaa_stats_py.baseball.get_baseball_player_game_pitching_stats()`, and `ncaa_stats_py.baseball.get_baseball_player_game_fielding_stats()` to enable a user to get game-level player stats from an NCAA baseball team.
- Implemented `ncaa_stats_py.baseball.get_baseball_game_player_stats()` and `ncaa_stats_py.baseball.get_baseball_game_team_stats()` to get stats from a specific game.
- Implemented `ncaa_stats_py.baseball.get_raw_baseball_game_pbp()` to play-by-play, and normalize the data into something that can be parsed at a later date.
- Set the package version to `0.0.1`.
