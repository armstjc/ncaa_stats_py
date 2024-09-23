# `ncaa_stats_py` Changelog

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