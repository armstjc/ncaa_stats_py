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
3. Automatically refreshing any cached data if the data
    hasn't been refreshed in a while.

For example, the following code will work as-is,
    and in the second loop, the code will load in the teams
    even faster because the data is cached
    on the device you're running this code.

```python
from timeit import default_timer as timer

from ncaa_stats_py.baseball import (
    get_baseball_team_roster,
    get_baseball_teams
)

start_time = timer()

# Loads in a table with every DI NCAA baseball team in the 2024 season.
# If this is the first time you run this script,
# it may take some time to repopulate the NCAA baseball team information data.

teams_df = get_baseball_teams(season=2024, level="I")

end_time = timer()

time_elapsed = end_time - start_time
print(f"Elapsed time: {time_elapsed:03f} seconds.\n\n")

# Gets 5 random D1 teams from 2024
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

# Dependencies

`ncaa_stats_py` is dependent on the following python packages:
- [`beautifulsoup4`](https://www.crummy.com/software/BeautifulSoup/):
    To assist with parsing HTML data.
- [`lxml`](https://lxml.de/): To work with `beautifulsoup4`
    in assisting with parsing HTML data.
- [`pandas`](https://github.com/pandas-dev/pandas):
    For `DataFrame` creation within package functions.
- [`pytz`](https://pythonhosted.org/pytz/):
    Used to attach timezone information for any date/date time objects
    encountered by this package.
- [`requests`](https://github.com/psf/requests): Used to make HTTPS requests.
- [`tqdm`](https://github.com/tqdm/tqdm):
    Used to show progress bars for actions in functions
    that are known to take minutes to load.

# License

This package is licensed under the MIT license.
You can view the package's license
[here](https://github.com/armstjc/ncaa_stats_py/blob/main/LICENSE).


"""

from ncaa_stats_py.baseball import *  # noqa: F403
from ncaa_stats_py.basketball import *  # noqa: F403
from ncaa_stats_py.field_hockey import *  # noqa: F403
from ncaa_stats_py.hockey import *  # noqa: F403
from ncaa_stats_py.softball import *  # noqa: F403
