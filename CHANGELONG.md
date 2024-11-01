# `ncaa_stats_py` Changelog

## 0.0.2: The "Basketball" update
- Implemented `ncaa_stats_py.basketball.get_basketball_teams()`, a function that allows one to get a list of all men's basketball (MBB) or women's basketball (WBB) teams given a season and a NCAA level.
- Implemented `ncaa_stats_py.basketball.load_basketball_teams()`, a function that allows one to load in every basketball team from a starting year (default is 2011) to present day.
- Implemented `ncaa_stats_py.basketball.get_basketball_team_schedule()`, a function that allows you to get a schedule for a specific basketball team.
- Implemented `ncaa_stats_py.basketball.get_full_basketball_schedule()`, a function that builds on top of `ncaa_stats_py.basketball.get_baseball_team_schedule()` and allows you to get the entire basketball schedule for a given season and level.
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