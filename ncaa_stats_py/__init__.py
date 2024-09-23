"""
# Welcome!
This is the official docs page for the `ncaa_stats_py` python package.

# Basic Setup

## How to Install

This package is is available through the
[`pip` package manager](https://en.wikipedia.org/wiki/Pip_(package_manager)),
and can be installed through one of the following commands
in your terminal/shell:
```
pip install ncaa_stats_py
```
OR
```
python -m pip install ncaa_stats_py
```

If you are using a Linux/Mac instance,
you may need to specify `python3` when installing, as shown below:
```
python3 -m pip install ncaa_stats_py
```

Alternatively, `cfbd-json-py` can be installed from
this GitHub repository with the following command through pip:
```
pip install git+https://github.com/armstjc/ncaa_stats_py
```
OR
```
python -m pip install git+https://github.com/armstjc/ncaa_stats_py
```
OR
```
python3 -m pip install git+https://github.com/armstjc/ncaa_stats_py
```

## How to Use
`ncaa_stats_py` separates itself by doing the following
things when attempting to get data:
1. Automatically caching any data that is already parsed
2. Automatically forcing a 5 second sleep timer for any HTML call,
    to ensure that any function call from this package
    won't result in you getting IP banned
    (you do not *need* to add sleep timers if you're looping through, 
    and calling functions in this python package).
3. Automatically refreshing any cached data if it's stale.

For example, the following code will work as-is,
    and in the second loop, the code will load in the teams
    even faster because the data is cached
    on the device you're running this code.

```python
from timeit import default_timer as timer

from ncaa_stats_py.baseball import (
    get_baseball_team_roster,
    load_baseball_teams
)

# Loads in a table with every NCAA baseball team from 2008 to present day.
teams_df = load_baseball_teams()

# Gets 5 random D1 teams from 2023
teams_df = teams_df.sample(5)
print(teams_df)
print()

# Let's send this to a list to make the loop slightly faster
team_ids_list = teams_df["team_id"].to_list()

# First loop
# If the data isn't cached, it should take 35-40 seconds to do this loop
start_time = timer()

for t_id in team_ids_list:
    print(f"On Team ID: {t_id}")
    df = get_baseball_team_roster(team_id=t_id)
    # print(df)

end_time = timer()

time_elapsed = end_time - start_time
print(f"Elapsed time: {time_elapsed:03f} seconds.\n\n")

# Second loop
# Because the data has been parsed and cached,
# this shouldn't take that long to loop through
start_time = timer()

for t_id in team_ids_list:
    print(f"On Team ID: {t_id}")
    df = get_baseball_team_roster(team_id=t_id)
    # print(df)

end_time = timer()
time_elapsed = end_time - start_time
print(f"Elapsed time: {time_elapsed:03f} seconds.\n\n")

```


"""

from ncaa_stats_py.baseball import *  # noqa: F403
