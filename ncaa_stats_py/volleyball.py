# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: `volleyball.py`
# Purpose: Houses functions that allows one to access NCAA volleyball data
# Creation Date: 2024-09-20 08:15 PM EDT
# Update History:
# - 2024-09-20 08:15 PM EDT
# - 2025-01-04 03:00 PM EDT
# - 2025-01-18 02:44 PM EDT
# - 2025-02-01 02:40 PM EDT
# - 2025-02-05 08:50 PM EDT


import logging
import re
from datetime import date, datetime
from os import mkdir
from os.path import exists, expanduser, getmtime

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser
from pytz import timezone
from tqdm import tqdm

from ncaa_stats_py.helpers.volleyball import _volleyball_pbp_helper
from ncaa_stats_py.utls import (
    _format_folder_str,
    _get_schools,
    _get_webpage,
    _name_smother,
)


def get_volleyball_teams(
    season: int,
    level: str | int,
    get_mens_data: bool = False
) -> pd.DataFrame:
    """
    Retrieves a list of volleyball teams from the NCAA.

    Parameters
    ----------
    `season` (int, mandatory):
        Required argument.
        Specifies the season you want NCAA volleyball team information from.

    `level` (int, mandatory):
        Required argument.
        Specifies the level/division you want
        NCAA volleyball team information from.
        This can either be an integer (1-3) or a string ("I"-"III").

    `get_mens_data` (bool, optional):
        Optional argument.
        If you want men's volleyball data instead of women's volleyball data,
        set this to `True`.

    Usage
    ----------
    ```python

    from ncaa_stats_py.volleyball import get_volleyball_teams

    ########################################
    #          Men's volleyball            #
    ########################################

    # Get all D1 men's volleyball teams for the 2024 season.
    print("Get all D1 men's volleyball teams for the 2024 season.")
    df = get_volleyball_teams(2024, 1)
    print(df)

    # Get all D2 men's volleyball teams for the 2023 season.
    print("Get all D2 men's volleyball teams for the 2023 season.")
    df = get_volleyball_teams(2023, 2)
    print(df)

    # Get all D3 men's volleyball teams for the 2022 season.
    print("Get all D3 men's volleyball teams for the 2022 season.")
    df = get_volleyball_teams(2022, 3)
    print(df)

    # Get all D1 men's volleyball teams for the 2021 season.
    print("Get all D1 men's volleyball teams for the 2021 season.")
    df = get_volleyball_teams(2021, "I")
    print(df)

    # Get all D2 men's volleyball teams for the 2020 season.
    print("Get all D2 men's volleyball teams for the 2020 season.")
    df = get_volleyball_teams(2020, "II")
    print(df)

    # Get all D3 men's volleyball teams for the 2019 season.
    print("Get all D3 men's volleyball teams for the 2019 season.")
    df = get_volleyball_teams(2019, "III")
    print(df)

    ########################################
    #          Women's volleyball          #
    ########################################

    # Get all D1 women's volleyball teams for the 2024 season.
    print(
        "Get all D1 women's volleyball teams for the 2024 season."
    )
    df = get_volleyball_teams(2024, 1)
    print(df)

    # Get all D2 women's volleyball teams for the 2023 season.
    print(
        "Get all D2 women's volleyball teams for the 2023 season."
    )
    df = get_volleyball_teams(2023, 2)
    print(df)

    # Get all D3 women's volleyball teams for the 2022 season.
    print(
        "Get all D3 women's volleyball teams for the 2022 season."
    )
    df = get_volleyball_teams(2022, 3)
    print(df)

    # Get all D1 women's volleyball teams for the 2021 season.
    print(
        "Get all D1 women's volleyball teams for the 2021 season."
    )
    df = get_volleyball_teams(2021, "I")
    print(df)

    # Get all D2 women's volleyball teams for the 2020 season.
    print(
        "Get all D2 women's volleyball teams for the 2020 season."
    )
    df = get_volleyball_teams(2020, "II")
    print(df)

    # Get all D3 women's volleyball teams for the 2019 season.
    print(
        "Get all D3 women's volleyball teams for the 2019 season."
    )
    df = get_volleyball_teams(2019, "III")
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a list of college volleyball teams
    in that season and NCAA level.
    """
    # def is_comment(elem):
    #     return isinstance(elem, Comment)
    sport_id = ""
    # stat_sequence = 0
    load_from_cache = True
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)
    teams_df = pd.DataFrame()
    teams_df_arr = []
    temp_df = pd.DataFrame()
    formatted_level = ""
    ncaa_level = 0

    if get_mens_data is True:
        sport_id = "MVB"
        stat_sequence = 528
    elif get_mens_data is False:
        sport_id = "WVB"
        stat_sequence = 48

    if isinstance(level, int) and level == 1:
        formatted_level = "I"
        ncaa_level = 1
    elif isinstance(level, int) and level == 2:
        formatted_level = "II"
        ncaa_level = 2
    elif isinstance(level, int) and level == 3:
        formatted_level = "III"
        ncaa_level = 3
    elif isinstance(level, str) and (
        level.lower() == "i" or level.lower() == "d1" or level.lower() == "1"
    ):
        ncaa_level = 1
        formatted_level = level.upper()
    elif isinstance(level, str) and (
        level.lower() == "ii" or level.lower() == "d2" or level.lower() == "2"
    ):
        ncaa_level = 2
        formatted_level = level.upper()
    elif isinstance(level, str) and (
        level.lower() == "iii" or level.lower() == "d3" or level.lower() == "3"
    ):
        ncaa_level = 3
        formatted_level = level.upper()

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/teams/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/teams/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/teams/"
        + f"{season}_{formatted_level}_teams.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/teams/"
            + f"{season}_{formatted_level}_teams.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/teams/"
                + f"{season}_{formatted_level}_teams.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days > 1 and
        season >= (now.year - 1) and
        now.month <= 7
    ):
        load_from_cache = False
    elif age.days >= 35:
        load_from_cache = False

    if load_from_cache is True:
        return teams_df

    logging.warning(
        f"Either we could not load {season} D{level} schools from cache, "
        + "or it's time to refresh the cached data."
    )
    schools_df = _get_schools()

    # Volleyball
    if sport_id == "MVB":
        url = (
            "https://stats.ncaa.org/rankings/change_sport_year_div?"
            + f"academic_year={season}.0&division={ncaa_level}.0" +
            f"&sport_code={sport_id}"
        )
    elif sport_id == "WVB":
        url = (
            "https://stats.ncaa.org/rankings/change_sport_year_div?"
            + f"academic_year={season+1}.0&division={ncaa_level}.0" +
            f"&sport_code={sport_id}"
        )

    response = _get_webpage(url=url)

    soup = BeautifulSoup(response.text, features="lxml")
    ranking_periods = soup.find("select", {"name": "rp", "id": "rp"})
    ranking_periods = ranking_periods.find_all("option")

    rp_value = 0
    found_value = False

    while found_value is False:
        # print("check")
        for rp in ranking_periods:
            if "final" in rp.text.lower():
                rp_value = rp.get("value")
                found_value = True
                break
                # pass
            elif "-" in rp.text.lower():
                pass
            else:
                rp_value = rp.get("value")
                found_value = True
                break

    if sport_id == "MVB":
        url = (
            "https://stats.ncaa.org/rankings/institution_trends?"
            + f"academic_year={season}.0&division={ncaa_level}.0&"
            + f"ranking_period={rp_value}&sport_code={sport_id}"
        )
    elif sport_id == "WVB":
        url = (
            "https://stats.ncaa.org/rankings/institution_trends?"
            + f"academic_year={season+1}.0&division={ncaa_level}.0&"
            + f"ranking_period={rp_value}&sport_code={sport_id}"
        )

    best_method = True
    if (
        (season < 2017 and sport_id == "MVB")
    ):
        url = (
            "https://stats.ncaa.org/rankings/national_ranking?"
            + f"academic_year={season}.0&division={ncaa_level}.0&"
            + f"ranking_period={rp_value}&sport_code={sport_id}"
            + f"&stat_seq={stat_sequence}.0"
        )
        response = _get_webpage(url=url)
        best_method = False
    elif (
        (season < 2017 and sport_id == "WVB")
    ):
        url = (
            "https://stats.ncaa.org/rankings/national_ranking?"
            + f"academic_year={season+1}.0&division={ncaa_level}.0&"
            + f"ranking_period={rp_value}&sport_code={sport_id}"
            + f"&stat_seq={stat_sequence}.0"
        )
        response = _get_webpage(url=url)
        best_method = False
    elif sport_id == "MVB":
        try:
            response = _get_webpage(url=url)
        except Exception as e:
            logging.info(f"Found exception when loading teams `{e}`")
            logging.info("Attempting backup method.")
            url = (
                "https://stats.ncaa.org/rankings/national_ranking?"
                + f"academic_year={season}.0&division={ncaa_level}.0&"
                + f"ranking_period={rp_value}&sport_code={sport_id}"
                + f"&stat_seq={stat_sequence}.0"
            )
            response = _get_webpage(url=url)
            best_method = False
    else:
        try:
            response = _get_webpage(url=url)
        except Exception as e:
            logging.info(f"Found exception when loading teams `{e}`")
            logging.info("Attempting backup method.")
            url = (
                "https://stats.ncaa.org/rankings/national_ranking?"
                + f"academic_year={season+1}.0&division={ncaa_level}.0&"
                + f"ranking_period={rp_value}&sport_code={sport_id}"
                + f"&stat_seq={stat_sequence}.0"
            )
            response = _get_webpage(url=url)
            best_method = False

    soup = BeautifulSoup(response.text, features="lxml")

    if best_method is True:
        soup = soup.find(
            "table",
            {"id": "stat_grid"},
        )
        soup = soup.find("tbody")
        t_rows = soup.find_all("tr")

        for t in t_rows:
            team_id = t.find("a")
            team_id = team_id.get("href")
            team_id = team_id.replace("/teams/", "")
            team_id = int(team_id)
            team_name = t.find_all("td")[0].text
            team_conference_name = t.find_all("td")[1].text
            # del team
            temp_df = pd.DataFrame(
                {
                    "season": season,
                    "ncaa_division": ncaa_level,
                    "ncaa_division_formatted": formatted_level,
                    "team_conference_name": team_conference_name,
                    "team_id": team_id,
                    "school_name": team_name,
                    "sport_id": sport_id,
                },
                index=[0],
            )
            teams_df_arr.append(temp_df)
            del temp_df
    else:
        soup = soup.find(
            "table",
            {"id": "rankings_table"},
        )
        soup = soup.find("tbody")
        t_rows = soup.find_all("tr")

        for t in t_rows:
            team_id = t.find("a")
            team_id = team_id.get("href")
            team_id = team_id.replace("/teams/", "")
            team_id = int(team_id)
            team = t.find_all("td")[1].get("data-order")
            team_name, team_conference_name = team.split(",")
            del team
            temp_df = pd.DataFrame(
                {
                    "season": season,
                    "ncaa_division": ncaa_level,
                    "ncaa_division_formatted": formatted_level,
                    "team_conference_name": team_conference_name,
                    "team_id": team_id,
                    "school_name": team_name,
                    "sport_id": sport_id,
                },
                index=[0],
            )
            teams_df_arr.append(temp_df)
            del temp_df

    teams_df = pd.concat(teams_df_arr, ignore_index=True)
    teams_df = pd.merge(
        left=teams_df,
        right=schools_df,
        on=["school_name"],
        how="left"
    )
    teams_df.sort_values(by=["team_id"], inplace=True)

    teams_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/teams/"
        + f"{season}_{formatted_level}_teams.csv",
        index=False,
    )

    return teams_df


def load_volleyball_teams(
    start_year: int = 2011,
    get_mens_data: bool = False
) -> pd.DataFrame:
    """
    Compiles a list of known NCAA volleyball teams in NCAA volleyball history.

    Parameters
    ----------
    `start_year` (int, optional):
        Optional argument.
        Specifies the first season you want
        NCAA volleyball team information from.

    `get_mens_data` (bool, optional):
        Optional argument.
        If you want men's volleyball data instead of women's volleyball data,
        set this to `True`.

    Usage
    ----------
    ```python

    from ncaa_stats_py.volleyball import load_volleyball_teams

    # WARNING: Running this script "as-is" for the first time may
    #          take some time.
    #          The *N*th time you run this script will be faster.

    # Load in every women's volleyball team
    # from 2011 to present day.
    print(
        "Load in every women's volleyball team " +
        "from 2011 to present day."
    )
    df = load_volleyball_teams(get_mens_data=True)
    print(df)

    # Load in every men's volleyball team
    # from 2011 to present day.
    print(
        "Load in every men's volleyball team " +
        "from 2011 to present day."
    )
    df = load_volleyball_teams()
    print(df)

    # Load in every men's volleyball team
    # from 2020 to present day.
    print(
        "Load in every men's volleyball team " +
        "from 2020 to present day."
    )
    df = load_volleyball_teams(start_year=2020)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a list of
    all known college volleyball teams.

    """
    # start_year = 2008

    # if get_mens_data is True:
    #     sport_id = "WVB"
    # else:
    #     sport_id = "MVB"

    teams_df = pd.DataFrame()
    teams_df_arr = []
    temp_df = pd.DataFrame()

    now = datetime.now()
    mens_ncaa_divisions = ["I", "III"]
    womens_ncaa_divisions = ["I", "II", "III"]
    if now.month > 5 and get_mens_data is False:
        ncaa_seasons = [x for x in range(start_year, (now.year + 2))]
    elif now.month < 5 and get_mens_data is True:
        ncaa_seasons = [x for x in range(start_year, (now.year + 1))]
    else:
        ncaa_seasons = [x for x in range(start_year, (now.year + 1))]

    logging.info(
        "Loading in all NCAA volleyball teams. "
        + "If this is the first time you're seeing this message, "
        + "it may take some time (3-10 minutes) for this to load."
    )

    if get_mens_data is True:
        for s in ncaa_seasons:
            logging.info(
                f"Loading in men's volleyball teams for the {s} season."
            )
            for d in mens_ncaa_divisions:
                temp_df = get_volleyball_teams(
                    season=s,
                    level=d,
                    get_mens_data=True
                )
                teams_df_arr.append(temp_df)
                del temp_df
    else:
        for s in ncaa_seasons:
            logging.info(
                f"Loading in women's volleyball teams for the {s} season."
            )
            for d in womens_ncaa_divisions:
                temp_df = get_volleyball_teams(
                    season=s,
                    level=d
                )
                teams_df_arr.append(temp_df)
                del temp_df

    teams_df = pd.concat(teams_df_arr, ignore_index=True)
    teams_df = teams_df.infer_objects()
    return teams_df


def get_volleyball_team_schedule(team_id: int) -> pd.DataFrame:
    """
    Retrieves a team schedule, from a valid NCAA volleyball team ID.

    Parameters
    ----------
    `team_id` (int, mandatory):
        Required argument.
        Specifies the team you want a schedule from.
        This is separate from a school ID, which identifies the institution.
        A team ID should be unique to a school, and a season.

    Usage
    ----------
    ```python

    from ncaa_stats_py.volleyball import get_volleyball_team_schedule

    ########################################
    #          Women's volleyball          #
    ########################################

    # Get the team schedule for the
    # 2024 Toledo WVB team (D1, ID: 585329).
    print(
        "Get the team schedule for the " +
        "2024 Toledo WVB team (D1, ID: 585329)."
    )
    df = get_volleyball_team_schedule(585329)
    print(df)

    # Get the team schedule for the
    # 2023 Black Hills St. WVB team (D2, ID: 559709).
    print(
        "Get the team schedule for the " +
        "2023 Black Hills St. WVB team (D2, ID: 559709)."
    )
    df = get_volleyball_team_schedule(559709)
    print(df)

    # Get the team schedule for the
    # 2022 Mount Mary WVB team (D3, ID: 539750).
    print(
        "Get the team schedule for the " +
        "2022 Mount Mary WVB team (D3, ID: 539750)."
    )
    df = get_volleyball_team_schedule(539750)
    print(df)

    # Get the team schedule for the
    # 2021 TCU WVB team (D1, ID: 522750).
    print(
        "Get the team schedule for the " +
        "2024 TCU WVB team (D1, ID: 522750)."
    )
    df = get_volleyball_team_schedule(522750)
    print(df)

    # Get the team schedule for the
    # 2020 Purdue Northwest WVB team (D2, ID: 504832).
    print(
        "Get the team schedule for the " +
        "2020 Purdue Northwest WVB team (D2, ID: 504832)."
    )
    df = get_volleyball_team_schedule(504832)
    print(df)

    # Get the team schedule for the
    # 2019 Juniata WVB team (D3, ID: 482642).
    print(
        "Get the team schedule for the " +
        "2019 Juniata WVB team (D3, ID: 482642)."
    )
    df = get_volleyball_team_schedule(482642)
    print(df)

    ########################################
    #          Men's volleyball            #
    ########################################

    # Get the team schedule for the
    # 2024 Missouri S&T MVB team (D1, ID: 573720).
    print(
        "Get the team schedule for the " +
        "2024 Missouri S&T MVB team (D1, ID: 573720)."
    )
    df = get_volleyball_team_schedule(573720)
    print(df)

    # Get the team schedule for the
    # 2023 Rockford MVB team (D3, ID: 550890).
    print(
        "Get the team schedule for the " +
        "2023 Rockford MVB team (D3, ID: 550890)."
    )
    df = get_volleyball_team_schedule(550890)
    print(df)

    # Get the team schedule for the
    # 2022 McKendree MVB team (D1, ID: 529896).
    print(
        "Get the team schedule for the " +
        "2022 McKendreeMaritime MVB team (D1, ID: 529896)."
    )
    df = get_volleyball_team_schedule(529896)
    print(df)

    # Get the team schedule for the
    # 2021 Concordia Chicago MVB team (D3, ID: 508505).
    print(
        "Get the team schedule for the " +
        "2021 Concordia Chicago MVB team (D3, ID: 508505)."
    )
    df = get_volleyball_team_schedule(508505)
    print(df)

    # Get the team schedule for the
    # 2020 St. Francis Brooklyn MVB team (D1, ID: 487992).
    print(
        "Get the team schedule for the " +
        "2020 St. Francis Brooklyn MVB team (D1, ID: 487992)."
    )
    df = get_volleyball_team_schedule(487992)
    print(df)

    # Get the team schedule for the
    # 2019 Loras MVB team (D3, ID: 453845).
    print(
        "Get the team schedule for the " +
        "2019 Loras MVB team (D3, ID: 453845)."
    )
    df = get_volleyball_team_schedule(453845)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with an NCAA volleyball team's schedule.

    """

    sport_id = ""
    schools_df = _get_schools()
    games_df = pd.DataFrame()
    games_df_arr = []
    season = 0
    temp_df = pd.DataFrame()
    load_from_cache = True

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    url = f"https://stats.ncaa.org/teams/{team_id}"

    try:
        team_df = load_volleyball_teams()
        team_df = team_df[team_df["team_id"] == team_id]
        season = team_df["season"].iloc[0]
        ncaa_division = team_df["ncaa_division"].iloc[0]
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        sport_id = "WVB"
    except Exception:
        team_df = load_volleyball_teams(get_mens_data=True)
        team_df = team_df[team_df["team_id"] == team_id]
        season = team_df["season"].iloc[0]
        ncaa_division = team_df["ncaa_division"].iloc[0]
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        sport_id = "MVB"
    # team_conference_name = team_df["team_conference_name"].iloc[0]
    # school_name = team_df["school_name"].iloc[0]
    # school_id = int(team_df["school_id"].iloc[0])

    del team_df

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/team_schedule/"
    ):
        pass
    else:
        mkdir(
            f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/team_schedule/"
        )

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/team_schedule/"
        + f"{team_id}_team_schedule.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/team_schedule/"
            + f"{team_id}_team_schedule.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/"
                + f"volleyball_{sport_id}/team_schedule/"
                + f"{team_id}_team_schedule.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime
    if (
        age.days > 1 and
        season >= now.year
    ):
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")

    school_name = soup.find("div", {"class": "card"}).find("img").get("alt")
    season_name = (
        soup.find("select", {"id": "year_list"})
        .find("option", {"selected": "selected"})
        .text
    )

    soup = soup.find_all(
        "div",
        {"class": "col p-0"},
    )

    # declaring it here to prevent potential problems down the road.
    table_data = ""
    for s in soup:
        try:
            temp_name = s.find("div", {"class": "card-header"})
            temp_name = temp_name.text
        except Exception as e:
            logging.warning(
                f"Could not parse card header. Full exception `{e}`. "
                + "Attempting alternate method."
            )
            temp_name = s.find("tr", {"class": "heading"}).find("td").text

        if "schedule" in temp_name.lower():
            table_data = s.find("table")

    t_rows = table_data.find_all("tr", {"class": "underline_rows"})

    if len(t_rows) == 0:
        t_rows = table_data.find_all("tr")

    for g in t_rows:
        is_valid_row = True
        game_num = 1
        ot_periods = 0
        is_home_game = True
        is_neutral_game = False

        cells = g.find_all("td")
        if len(cells) <= 1:
            # Because of how *well* designed
            # stats.ncaa.org is, if we have to use execute
            # the `if len(t_rows) == 0:` code,
            # we need to catch any cases where every element in a
            # table row (`<tr>`) is a table header (`<th>`),
            # instead of a table data cell (`<td>`)
            continue

        game_date = cells[0].text

        # If "(" is in the same cell as the date,
        # this means that this game is an extra innings game.
        # The number encased in `()` is the actual number of innings.
        # We need to remove that from the date,
        # and move it into a separate variable.
        if "(" in game_date:
            game_date = game_date.replace(")", "")
            game_date, game_num = game_date.split("(")
            game_date = game_date.strip()
            game_num = int(game_num.strip())

        if ":" in game_date and ("PM" in game_date or "AM" in game_date):
            game_date = datetime.strptime(
                game_date,
                "%m/%d/%Y %I:%M %p"
            ).date()
        else:
            game_date = datetime.strptime(
                game_date,
                "%m/%d/%Y"
            ).date()

        try:
            opp_team_id = cells[1].find("a").get("href")
        except IndexError:
            logging.info(
                "Skipping row because it is clearly "
                + "not a row that has schedule data."
            )
            is_valid_row = False
        except AttributeError as e:
            logging.info(
                "Could not extract a team ID for this game. " +
                f"Full exception {e}"
            )
            opp_team_id = "-1"
        except Exception as e:
            logging.warning(
                "An unhandled exception has occurred when "
                + "trying to get the opposition team ID for this game. "
                f"Full exception `{e}`."
            )
            raise e
        if is_valid_row is True:
            if opp_team_id is not None:
                opp_team_id = opp_team_id.replace("/teams/", "")
                opp_team_id = int(opp_team_id)

                try:
                    opp_team_name = cells[1].find("img").get("alt")
                except AttributeError:
                    logging.info(
                        "Couldn't find the opposition team name "
                        + "for this row from an image element. "
                        + "Attempting a backup method"
                    )
                    opp_team_name = cells[1].text
                except Exception as e:
                    logging.info(
                        "Unhandled exception when trying to get the "
                        + "opposition team name from this game. "
                        + f"Full exception `{e}`"
                    )
                    raise e
            else:
                opp_team_name = cells[1].text

            if opp_team_name[0] == "@":
                # The logic for determining if this game was a
                # neutral site game doesn't care if that info is in
                # `opp_team_name`.
                opp_team_name = opp_team_name.strip().replace("@", "")
            elif "@" in opp_team_name:
                opp_team_name = opp_team_name.strip().split("@")[0]
            # opp_team_show_name = cells[1].text.strip()

            opp_text = cells[1].text
            opp_text = opp_text.strip()
            if "@" in opp_text and opp_text[0] == "@":
                is_home_game = False
            elif "@" in opp_text and opp_text[0] != "@":
                is_neutral_game = True
                is_home_game = False
            # This is just to cover conference and NCAA championship
            # tournaments.
            elif "championship" in opp_text.lower():
                is_neutral_game = True
                is_home_game = False
            elif "ncaa" in opp_text.lower():
                is_neutral_game = True
                is_home_game = False

            del opp_text

            score = cells[2].text.strip()
            if len(score) == 0:
                score_1 = 0
                score_2 = 0
            elif (
                "canceled" not in score.lower() and
                "ppd" not in score.lower()
            ):
                score_1, score_2 = score.split("-")

                # `score_1` should be "W `n`", "L `n`", or "T `n`",
                # with `n` representing the number of runs this team
                # scored in this game.
                # Let's remove the "W", "L", or "T" from `score_1`,
                # and determine which team won later on in this code.
                if any(x in score_1 for x in ["W", "L", "T"]):
                    score_1 = score_1.split(" ")[1]

                if "(" in score_2:
                    score_2 = score_2.replace(")", "")
                    score_2, ot_periods = score_2.split("(")
                    ot_periods = ot_periods.replace("OT", "")
                    ot_periods = ot_periods.replace(" ", "")
                    ot_periods = int(ot_periods)

                if ot_periods is None:
                    ot_periods = 0
                score_1 = int(score_1)
                score_2 = int(score_2)
            else:
                score_1 = None
                score_2 = None

            try:
                game_id = cells[2].find("a").get("href")
                game_id = game_id.replace("/contests", "")
                game_id = game_id.replace("/box_score", "")
                game_id = game_id.replace("/", "")
                game_id = int(game_id)
                game_url = (
                    f"https://stats.ncaa.org/contests/{game_id}/box_score"
                )
            except AttributeError as e:
                logging.info(
                    "Could not parse a game ID for this game. "
                    + f"Full exception `{e}`."
                )
                game_id = None
                game_url = None
            except Exception as e:
                logging.info(
                    "An unhandled exception occurred when trying "
                    + "to find a game ID for this game. "
                    + f"Full exception `{e}`."
                )
                raise e

            try:
                attendance = cells[3].text
                attendance = attendance.replace(",", "")
                attendance = attendance.replace("\n", "")
                attendance = int(attendance)
            except IndexError as e:
                logging.info(
                    "It doesn't appear as if there is an attendance column "
                    + "for this team's schedule table."
                    f"Full exception `{e}`."
                )
                attendance = None
            except ValueError as e:
                logging.info(
                    "There doesn't appear as if "
                    + "there is a recorded attendance. "
                    + "for this game/row. "
                    f"Full exception `{e}`."
                )
                attendance = None
            except Exception as e:
                logging.info(
                    "An unhandled exception occurred when trying "
                    + "to find this game's attendance. "
                    + f"Full exception `{e}`."
                )
                raise e

            if is_home_game is True:
                temp_df = pd.DataFrame(
                    {
                        "season": season,
                        "season_name": season_name,
                        "game_id": game_id,
                        "game_date": game_date,
                        "game_num": game_num,
                        "ot_periods": ot_periods,
                        "home_team_id": team_id,
                        "home_team_name": school_name,
                        "away_team_id": opp_team_id,
                        "away_team_name": opp_team_name,
                        "home_team_sets_won": score_1,
                        "away_team_sets_won": score_2,
                        "is_neutral_game": is_neutral_game,
                        "game_url": game_url,
                    },
                    index=[0],
                )
                games_df_arr.append(temp_df)
                del temp_df
            elif is_neutral_game is True:
                # For the sake of simplicity,
                # order both team ID's,
                # and set the lower number of the two as
                # the "away" team in this neutral site game,
                # just so there's no confusion if someone
                # combines a ton of these team schedule `DataFrame`s,
                # and wants to remove duplicates afterwards.
                t_ids = [opp_team_id, team_id]
                t_ids.sort()

                if t_ids[0] == team_id:
                    # home
                    temp_df = pd.DataFrame(
                        {
                            "season": season,
                            "season_name": season_name,
                            "game_id": game_id,
                            "game_date": game_date,
                            "game_num": game_num,
                            "ot_periods": ot_periods,
                            "home_team_id": team_id,
                            "home_team_name": school_name,
                            "away_team_id": opp_team_id,
                            "away_team_name": opp_team_name,
                            "home_team_sets_won": score_1,
                            "away_team_sets_won": score_2,
                            "is_neutral_game": is_neutral_game,
                            "game_url": game_url,
                        },
                        index=[0],
                    )

                else:
                    # away
                    temp_df = pd.DataFrame(
                        {
                            "season": season,
                            "season_name": season_name,
                            "game_id": game_id,
                            "game_date": game_date,
                            "game_num": game_num,
                            "ot_periods": ot_periods,
                            "home_team_id": opp_team_id,
                            "home_team_name": opp_team_name,
                            "away_team_id": team_id,
                            "away_team_name": school_name,
                            "home_team_sets_won": score_2,
                            "away_team_sets_won": score_1,
                            "is_neutral_game": is_neutral_game,
                            "game_url": game_url,
                        },
                        index=[0],
                    )

                games_df_arr.append(temp_df)
                del temp_df
            else:
                temp_df = pd.DataFrame(
                    {
                        "season": season,
                        "season_name": season_name,
                        "game_id": game_id,
                        "game_date": game_date,
                        "game_num": game_num,
                        "ot_periods": ot_periods,
                        "home_team_id": opp_team_id,
                        "home_team_name": opp_team_name,
                        "away_team_id": team_id,
                        "away_team_name": school_name,
                        "home_team_sets_won": score_2,
                        "away_team_sets_won": score_1,
                        "is_neutral_game": is_neutral_game,
                        "game_url": game_url,
                    },
                    index=[0],
                )

                games_df_arr.append(temp_df)
                del temp_df

        # team_photo = team_id.find("img").get("src")

    games_df = pd.concat(games_df_arr, ignore_index=True)

    temp_df = schools_df.rename(
        columns={
            "school_name": "home_team_name",
            "school_id": "home_school_id"
        }
    )
    games_df = games_df.merge(right=temp_df, on="home_team_name", how="left")

    temp_df = schools_df.rename(
        columns={
            "school_name": "away_team_name",
            "school_id": "away_school_id"
        }
    )
    games_df = games_df.merge(right=temp_df, on="away_team_name", how="left")
    games_df["ncaa_division"] = ncaa_division
    games_df["ncaa_division_formatted"] = ncaa_division_formatted

    # games_df["game_url"] = games_df["game_url"].str.replace("/box_score", "")
    games_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/"
        + f"volleyball_{sport_id}/team_schedule/"
        + f"{team_id}_team_schedule.csv",
        index=False,
    )

    return games_df


def get_volleyball_day_schedule(
    game_date: str | date | datetime,
    level: str | int = "I",
    get_mens_data: bool = False
):
    """
    Given a date and NCAA level, this function retrieves volleyball every game
    for that date.

    Parameters
    ----------
    `game_date` (int, mandatory):
        Required argument.
        Specifies the date you want a volleyball schedule from.
        For best results, pass a string formatted as "YYYY-MM-DD".

    `level` (int, mandatory):
        Required argument.
        Specifies the level/division you want a
        NCAA volleyball schedule from.
        This can either be an integer (1-3) or a string ("I"-"III").

    `get_mens_data` (bool, optional):
        Optional argument.
        If you want men's volleyball data instead of women's volleyball data,
        set this to `True`.

    Usage
    ----------
    ```python

    from ncaa_stats_py.volleyball import get_volleyball_day_schedule

    ########################################
    #         Women's Volleyball           #
    ########################################

    # Get all DI games (if any) that were played on December 22th, 2024.
    print("Get all games (if any) that were played on December 22th, 2024.")
    df = get_volleyball_day_schedule("2024-12-22", level=1)
    print(df)

    # Get all division II games that were played on November 24th, 2024.
    print("Get all division II games that were played on November 24th, 2024.")
    df = get_volleyball_day_schedule("2024-11-24", level="II")
    print(df)

    # Get all DIII games that were played on October 27th, 2024.
    print("Get all DIII games that were played on October 27th, 2024.")
    df = get_volleyball_day_schedule("2024-10-27", level="III")
    print(df)

    # Get all DI games (if any) that were played on September 29th, 2024.
    print(
        "Get all DI games (if any) that were played on September 29th, 2024."
    )
    df = get_volleyball_day_schedule("2024-09-29")
    print(df)

    # Get all DII games played on August 30th, 2024.
    print("Get all DI games played on August 30th, 2024.")
    df = get_volleyball_day_schedule("2024-08-30")
    print(df)

    # Get all division III games played on September 23rd, 2023.
    print("Get all division III games played on September 23rd, 2023.")
    df = get_volleyball_day_schedule("2023-09-23", level="III")
    print(df)

    ########################################
    #          Men's Volleyball            #
    ########################################

    # Get all DI games that will be played on April 12th, 2025.
    print("Get all games that will be played on April 12th, 2025.")
    df = get_volleyball_day_schedule("2025-04-12", level=1, get_mens_data=True)
    print(df)

    # Get all DI games that were played on January 30th, 2025.
    print("Get all games that were played on January 30th, 2025.")
    df = get_volleyball_day_schedule(
        "2025-01-30", level="I", get_mens_data=True
    )
    print(df)

    # Get all division III games that were played on April 6th, 2024.
    print("Get all division III games that were played on April 6th, 2024.")
    df = get_volleyball_day_schedule(
        "2025-04-05", level="III", get_mens_data=True
    )
    print(df)

    # Get all DI games (if any) that were played on March 30th, 2024.
    print("Get all DI games (if any) that were played on March 30th, 2024.")
    df = get_volleyball_day_schedule("2024-03-30", get_mens_data=True)
    print(df)

    # Get all DI games played on February 23rd, 2024.
    print("Get all DI games played on February 23rd, 2024.")
    df = get_volleyball_day_schedule("2024-02-23", get_mens_data=True)
    print(df)

    # Get all division III games played on February 11th, 2023.
    print("Get all division III games played on February 11th, 2023.")
    df = get_volleyball_day_schedule("2024-02-11", level=3, get_mens_data=True)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with all volleyball games played on that day,
    for that NCAA division/level.

    """

    season = 0
    sport_id = "WVB"

    schedule_df = pd.DataFrame()
    schedule_df_arr = []

    if isinstance(game_date, date):
        game_datetime = datetime.combine(
            game_date, datetime.min.time()
        )
    elif isinstance(game_date, datetime):
        game_datetime = game_date
    elif isinstance(game_date, str):
        game_datetime = parser.parse(
            game_date
        )
    else:
        unhandled_datatype = type(game_date)
        raise ValueError(
            f"Unhandled datatype for `game_date`: `{unhandled_datatype}`"
        )

    if isinstance(level, int) and level == 1:
        formatted_level = "I"
        ncaa_level = 1
    elif isinstance(level, int) and level == 2:
        formatted_level = "II"
        ncaa_level = 2
    elif isinstance(level, int) and level == 3:
        formatted_level = "III"
        ncaa_level = 3
    elif isinstance(level, str) and (
        level.lower() == "i" or level.lower() == "d1" or level.lower() == "1"
    ):
        ncaa_level = 1
        formatted_level = level.upper()
    elif isinstance(level, str) and (
        level.lower() == "ii" or level.lower() == "d2" or level.lower() == "2"
    ):
        ncaa_level = 2
        formatted_level = level.upper()
    elif isinstance(level, str) and (
        level.lower() == "iii" or level.lower() == "d3" or level.lower() == "3"
    ):
        ncaa_level = 3
        formatted_level = level.upper()

    del level

    if get_mens_data is True:
        sport_id = "MVB"
    elif get_mens_data is False:
        sport_id = "WVB"
    else:
        raise ValueError(
            f"Unhandled value for `get_wbb_data`: `{get_mens_data}`"
        )

    season = game_datetime.year
    game_month = game_datetime.month
    game_day = game_datetime.day
    game_year = game_datetime.year

    if game_month > 7:
        season += 1
        url = (
            "https://stats.ncaa.org/contests/" +
            f"livestream_scoreboards?utf8=%E2%9C%93&sport_code={sport_id}" +
            f"&academic_year={season}&division={ncaa_level}" +
            f"&game_date={game_month:00d}%2F{game_day:00d}%2F{game_year}" +
            "&commit=Submit"
        )
    else:
        url = (
            "https://stats.ncaa.org/contests/" +
            f"livestream_scoreboards?utf8=%E2%9C%93&sport_code={sport_id}" +
            f"&academic_year={season}&division={ncaa_level}" +
            f"&game_date={game_month:00d}%2F{game_day:00d}%2F{game_year}" +
            "&commit=Submit"
        )

    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")

    game_boxes = soup.find_all("div", {"class": "table-responsive"})

    for box in game_boxes:
        game_id = None
        game_alt_text = None
        game_num = 1
        # t_box = box.find("table")
        table_box = box.find("table")
        table_rows = table_box.find_all("tr")

        # Date/attendance
        game_date_str = table_rows[0].find("div", {"class": "col-6 p-0"}).text
        game_date_str = game_date_str.replace("\n", "")
        game_date_str = game_date_str.strip()
        game_date_str = game_date_str.replace("TBA ", "TBA")
        game_date_str = game_date_str.replace("TBD ", "TBD")
        game_date_str = game_date_str.replace("PM ", "PM")
        game_date_str = game_date_str.replace("AM ", "AM")
        game_date_str = game_date_str.strip()
        attendance_str = table_rows[0].find(
            "div",
            {"class": "col p-0 text-right"}
        ).text

        attendance_str = attendance_str.replace("Attend:", "")
        attendance_str = attendance_str.replace(",", "")
        attendance_str = attendance_str.replace("\n", "")
        if (
            "st" in attendance_str.lower() or
            "nd" in attendance_str.lower() or
            "rd" in attendance_str.lower() or
            "th" in attendance_str.lower()
        ):
            # This is not an attendance,
            # this is whatever quarter/half/inning this game is in.
            attendance_num = None
        elif "final" in attendance_str.lower():
            attendance_num = None
        elif len(attendance_str) > 0:
            attendance_num = int(attendance_str)
        else:
            attendance_num = None

        if "(" in game_date_str:
            game_date_str = game_date_str.replace(")", "")
            game_date_str, game_num = game_date_str.split("(")
            game_num = int(game_num)

        if "TBA" in game_date_str:
            game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y TBA')
        elif "tba" in game_date_str:
            game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y tba')
        elif "TBD" in game_date_str:
            game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y TBD')
        elif "tbd" in game_date_str:
            game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y tbd')
        elif (
            "tbd" not in game_date_str.lower() and
            ":" not in game_date_str.lower()
        ):
            game_date_str = game_date_str.replace(" ", "")
            game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y')
        else:
            game_datetime = datetime.strptime(
                game_date_str,
                '%m/%d/%Y %I:%M %p'
            )
        game_datetime = game_datetime.astimezone(timezone("US/Eastern"))

        game_alt_text = table_rows[1].find_all("td")[0].text
        if game_alt_text is not None and len(game_alt_text) > 0:
            game_alt_text = game_alt_text.replace("\n", "")
            game_alt_text = game_alt_text.strip()

        if len(game_alt_text) == 0:
            game_alt_text = None

        urls_arr = box.find_all("a")

        for u in urls_arr:
            url_temp = u.get("href")
            if "contests" in url_temp:
                game_id = url_temp
                del url_temp

        if game_id is None:
            for r in range(0, len(table_rows)):
                temp = table_rows[r]
                temp_id = temp.get("id")

                if temp_id is not None and len(temp_id) > 0:
                    game_id = temp_id

        del urls_arr

        game_id = game_id.replace("/contests", "")
        game_id = game_id.replace("/box_score", "")
        game_id = game_id.replace("/livestream_scoreboards", "")
        game_id = game_id.replace("/", "")
        game_id = game_id.replace("contest_", "")
        game_id = int(game_id)

        table_rows = table_box.find_all("tr", {"id": f"contest_{game_id}"})
        away_team_row = table_rows[0]
        home_team_row = table_rows[1]

        # Away team
        td_arr = away_team_row.find_all("td")

        try:
            away_team_name = td_arr[0].find("img").get("alt")
        except Exception:
            away_team_name = td_arr[1].text
        away_team_name = away_team_name.replace("\n", "")
        away_team_name = away_team_name.strip()

        try:
            away_team_id = td_arr[1].find("a").get("href")
            away_team_id = away_team_id.replace("/teams/", "")
            away_team_id = int(away_team_id)
        except AttributeError:
            away_team_id = None
            logging.info("No team ID found for the away team")
        except Exception as e:
            raise e

        away_sets_scored = td_arr[-1].text
        away_sets_scored = away_sets_scored.replace("\n", "")
        away_sets_scored = away_sets_scored.replace("\xa0", "")

        if "ppd" in away_sets_scored.lower():
            continue
        elif "cancel" in away_sets_scored.lower():
            continue

        if len(away_sets_scored) > 0:
            away_sets_scored = int(away_sets_scored)
        else:
            away_sets_scored = 0

        del td_arr

        # Home team
        td_arr = home_team_row.find_all("td")

        try:
            home_team_name = td_arr[0].find("img").get("alt")
        except Exception:
            home_team_name = td_arr[1].text
        home_team_name = home_team_name.replace("\n", "")
        home_team_name = home_team_name.strip()

        try:
            home_team_id = td_arr[1].find("a").get("href")
            home_team_id = home_team_id.replace("/teams/", "")
            home_team_id = int(home_team_id)
        except AttributeError:
            home_team_id = None
            logging.info("No team ID found for the home team")
        except Exception as e:
            raise e

        home_sets_scored = td_arr[-1].text
        home_sets_scored = home_sets_scored.replace("\n", "")
        home_sets_scored = home_sets_scored.replace("\xa0", "")

        if "ppd" in home_sets_scored.lower():
            continue
        elif "cancel" in home_sets_scored.lower():
            continue

        if len(home_sets_scored) > 0:
            home_sets_scored = int(home_sets_scored)
        else:
            home_sets_scored = 0

        temp_df = pd.DataFrame(
            {
                "season": season,
                "sport_id": sport_id,
                "game_date": game_datetime.strftime("%Y-%m-%d"),
                "game_datetime": game_datetime.isoformat(),
                "game_id": game_id,
                "formatted_level": formatted_level,
                "ncaa_level": ncaa_level,
                "game_alt_text": game_alt_text,
                "away_team_id": away_team_id,
                "away_team_name": away_team_name,
                "home_team_id": home_team_id,
                "home_team_name": home_team_name,
                "home_sets_scored": home_sets_scored,
                "away_sets_scored": away_sets_scored,
                "attendance": attendance_num
            },
            index=[0]
        )
        schedule_df_arr.append(temp_df)

        del temp_df

    if len(schedule_df_arr) >= 1:
        schedule_df = pd.concat(schedule_df_arr, ignore_index=True)
    else:
        logging.warning(
            "Could not find any game(s) for "
            + f"{game_datetime.year:00d}-{game_datetime.month:00d}"
            + f"-{game_datetime.day:00d}. "
            + "If you believe this is an error, "
            + "please raise an issue at "
            + "\n https://github.com/armstjc/ncaa_stats_py/issues \n"
        )
    return schedule_df


def get_full_volleyball_schedule(
    season: int,
    level: str | int = "I",
    get_mens_data: bool = True
) -> pd.DataFrame:
    """
    Retrieves a full volleyball schedule,
    from an NCAA level (`"I"`, `"II"`, `"III"`).
    The way this is done is by going through every team in a division,
    and parsing the schedules of every team in a division.

    This function will take time when first run (30-60 minutes)!
    You have been warned.

    Parameters
    ----------
    `season` (int, mandatory):
        Specifies the season you want a schedule from.

    `level` (int | str, mandatory):
        Specifies the team you want a schedule from.

    `get_mens_data` (bool, optional):
        Optional argument.
        If you want men's volleyball data instead of women's volleyball data,
        set this to `True`.

    Usage
    ----------
    ```python

    from ncaa_stats_py.volleyball import get_full_volleyball_schedule

    ##############################################################################
    # NOTE
    # This function will easily take an hour or more
    # to run for the first time in a given season and NCAA level!
    # You have been warned!
    ##############################################################################

    # Get the entire 2024 schedule for the 2024 women's D1 volleyball season.
    print(
        "Get the entire 2024 schedule " +
        "for the 2024 women's D1 volleyball season."
    )
    df = get_full_volleyball_schedule(season=2024, level="I")
    print(df)

    # Get the entire 2024 schedule for the 2024 men's D1 volleyball season.
    # print(
    #     "Get the entire 2024 schedule for " +
    #     "the 2024 men's D1 volleyball season."
    # )
    # df = get_full_volleyball_schedule(
    #     season=2024,
    #     level="I",
    #     get_mens_data=True
    # )
    # print(df)

    # You can also input `level` as an integer.
    # In addition, this and other functions cache data,
    # so this should load very quickly
    # compared to the first run of this function.
    print("You can also input `level` as an integer.")
    print(
        "In addition, this and other functions cache data, "
        + "so this should load very quickly "
        + "compared to the first run of this function."
    )
    df = get_full_volleyball_schedule(season=2024, level=1)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with an NCAA volleyball
    schedule for a specific season and level.
    """

    sport_id = ""
    load_from_cache = True
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)
    schedule_df = pd.DataFrame()
    schedule_df_arr = []
    temp_df = pd.DataFrame()
    formatted_level = ""
    ncaa_level = 0

    if get_mens_data is True:
        sport_id = "MVB"
    else:
        sport_id = "WVB"

    if isinstance(level, int) and level == 1:
        formatted_level = "I"
        ncaa_level = 1
    elif isinstance(level, int) and level == 2:
        formatted_level = "II"
        ncaa_level = 2
    elif isinstance(level, int) and level == 3:
        formatted_level = "III"
        ncaa_level = 3
    elif isinstance(level, str) and (
        level.lower() == "i" or level.lower() == "d1" or level.lower() == "1"
    ):
        ncaa_level = 1
        formatted_level = level.upper()
    elif isinstance(level, str) and (
        level.lower() == "ii" or level.lower() == "d2" or level.lower() == "2"
    ):
        ncaa_level = 2
        formatted_level = level.upper()
    elif isinstance(level, str) and (
        level.lower() == "iii" or level.lower() == "d3" or level.lower() == "3"
    ):
        ncaa_level = 3
        formatted_level = level.upper()

    del level

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/" +
        f"volleyball_{sport_id}/full_schedule/"
    ):
        pass
    else:
        mkdir(
            f"{home_dir}/.ncaa_stats_py/" +
            f"volleyball_{sport_id}/full_schedule/"
        )

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/full_schedule/"
        + f"{season}_{formatted_level}_full_schedule.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/full_schedule/"
            + f"{season}_{formatted_level}_full_schedule.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/" +
                f"volleyball_{sport_id}/full_schedule/"
                + f"{season}_{formatted_level}_full_schedule.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days > 1 and
        season >= now.year
    ):
        load_from_cache = False

    if load_from_cache is True:
        return teams_df

    teams_df = load_volleyball_teams()
    teams_df = teams_df[
        (teams_df["season"] == season) &
        (teams_df["ncaa_division"] == ncaa_level)
    ]
    team_ids_arr = teams_df["team_id"].to_numpy()

    for team_id in tqdm(team_ids_arr):
        temp_df = get_volleyball_team_schedule(team_id=team_id)
        schedule_df_arr.append(temp_df)

    schedule_df = pd.concat(schedule_df_arr, ignore_index=True)
    schedule_df = schedule_df.drop_duplicates(subset="game_id", keep="first")
    schedule_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/"
        + f"volleyball_{sport_id}/full_schedule/"
        + f"{season}_{formatted_level}_full_schedule.csv",
        index=False,
    )
    return schedule_df


def get_volleyball_team_roster(team_id: int) -> pd.DataFrame:
    """
    Retrieves a volleyball team's roster from a given team ID.

    Parameters
    ----------
    `team_id` (int, mandatory):
        Required argument.
        Specifies the team you want a roster from.
        This is separate from a school ID, which identifies the institution.
        A team ID should be unique to a school, and a season.

    Usage
    ----------
    ```python

    from ncaa_stats_py.volleyball import get_volleyball_team_roster

    ########################################
    #          Women's volleyball          #
    ########################################

    # Get the volleyball roster for the
    # 2024 Weber St. WVB team (D1, ID: 585347).
    print(
        "Get the volleyball roster for the " +
        "2024 Weber St. WVB team (D1, ID: 585347)."
    )
    df = get_volleyball_team_roster(585347)
    print(df)

    # Get the volleyball roster for the
    # 2023 Montevallo WVB team (D2, ID: 559599).
    print(
        "Get the volleyball roster for the " +
        "2023 Montevallo WVB team (D2, ID: 559599)."
    )
    df = get_volleyball_team_roster(559599)
    print(df)

    # Get the volleyball roster for the
    # 2022 Millsaps team (D3, ID: 539944).
    print(
        "Get the volleyball roster for the " +
        "2022 Millsaps team (D3, ID: 539944)."
    )
    df = get_volleyball_team_roster(539944)
    print(df)

    # Get the volleyball roster for the
    # 2021 Binghamton WVB team (D1, ID: 522893).
    print(
        "Get the volleyball roster for the " +
        "2021 Binghamton WVB team (D1, ID: 522893)."
    )
    df = get_volleyball_team_roster(522893)
    print(df)

    # Get the volleyball roster for the
    # 2020 Holy Family WVB team (D2, ID: 504760).
    print(
        "Get the volleyball roster for the " +
        "2020 Holy Family WVB team (D2, ID: 504760)."
    )
    df = get_volleyball_team_roster(504760)
    print(df)

    # Get the volleyball roster for the
    # 2019 Franciscan team (D3, ID: 482939).
    print(
        "Get the volleyball roster for the " +
        "2019 Franciscan team (D3, ID: 482939)."
    )
    df = get_volleyball_team_roster(482939)
    print(df)

    ########################################
    #          Men's volleyball            #
    ########################################

    # Get the volleyball roster for the
    # 2024 Hawaii MVB team (D1, ID: 573674).
    print(
        "Get the volleyball roster for the " +
        "2024 Hawaii MVB team (D1, ID: 573674)."
    )
    df = get_volleyball_team_roster(573674)
    print(df)

    # Get the volleyball roster for the
    # 2023 Widener MVB team (D3, ID: 550860).
    print(
        "Get the volleyball roster for the " +
        "2023 Widener MVB team (D3, ID: 550860)."
    )
    df = get_volleyball_team_roster(550860)
    print(df)

    # Get the volleyball roster for the
    # 2022 Alderson Broaddus MVB team (D1, ID: 529880).
    print(
        "Get the volleyball roster for the " +
        "2022 Alderson Broaddus MVB team (D1, ID: 529880)."
    )
    df = get_volleyball_team_roster(529880)
    print(df)

    # Get the volleyball roster for the
    # 2021 Geneva MVB team (D3, ID: 508506).
    print(
        "Get the volleyball roster for the " +
        "2021 Geneva MVB team (D3, ID: 508506)."
    )
    df = get_volleyball_team_roster(508506)
    print(df)

    # Get the volleyball roster for the
    # 2020 Urbana MVB team (D1, ID: 484975).
    print(
        "Get the volleyball roster for the " +
        "2020 Urbana MVB team (D1, ID: 484975)."
    )
    df = get_volleyball_team_roster(484975)
    print(df)

    # Get the volleyball roster for the
    # 2019 Eastern Nazarene MVB team (D3, ID: 453876).
    print(
        "Get the volleyball roster for the " +
        "2019 Eastern Nazarene MVB team (D3, ID: 453876)."
    )
    df = get_volleyball_team_roster(453876)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with
    an NCAA volleyball team's roster for that season.
    """
    sport_id = ""
    roster_df = pd.DataFrame()
    roster_df_arr = []
    temp_df = pd.DataFrame()
    url = f"https://stats.ncaa.org/teams/{team_id}/roster"
    load_from_cache = True
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    stat_columns = [
        "season",
        "season_name",
        "sport_id",
        "ncaa_division",
        "ncaa_division_formatted",
        "team_conference_name",
        "school_id",
        "school_name",
        "player_id",
        "player_jersey_num",
        "player_full_name",
        "player_first_name",
        "player_last_name",
        "player_class",
        "player_positions",
        "player_height_string",
        "player_weight",
        "player_hometown",
        "player_high_school",
        "player_G",
        "player_GS",
        "player_url",
    ]

    try:
        team_df = load_volleyball_teams()
        team_df = team_df[team_df["team_id"] == team_id]

        season = team_df["season"].iloc[0]
        ncaa_division = team_df["ncaa_division"].iloc[0]
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        team_conference_name = team_df["team_conference_name"].iloc[0]
        school_name = team_df["school_name"].iloc[0]
        school_id = int(team_df["school_id"].iloc[0])
        sport_id = "WVB"
    except Exception:
        team_df = load_volleyball_teams(get_mens_data=True)
        team_df = team_df[team_df["team_id"] == team_id]

        season = team_df["season"].iloc[0]
        ncaa_division = team_df["ncaa_division"].iloc[0]
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        team_conference_name = team_df["team_conference_name"].iloc[0]
        school_name = team_df["school_name"].iloc[0]
        school_id = int(team_df["school_id"].iloc[0])
        school_id = int(team_df["school_id"].iloc[0])
        sport_id = "MVB"

    del team_df

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/rosters/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/rosters/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/rosters/" +
        f"{team_id}_roster.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/rosters/" +
            f"{team_id}_roster.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/rosters/" +
                f"{team_id}_roster.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days >= 14 and
        season >= now.year
    ):
        load_from_cache = False

    if load_from_cache is True:
        return teams_df

    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")
    try:
        school_name = soup.find(
            "div",
            {"class": "card"}
        ).find("img").get("alt")
    except Exception:
        school_name = soup.find("div", {"class": "card"}).find("a").text
        school_name = school_name.rsplit(" ", maxsplit=1)[0]

    season_name = (
        soup.find("select", {"id": "year_list"})
        .find("option", {"selected": "selected"})
        .text
    )

    try:
        table = soup.find(
            "table",
            {"class": "dataTable small_font"},
        )

        table_headers = table.find("thead").find_all("th")
    except Exception:
        table = soup.find(
            "table",
            {"class": "dataTable small_font no_padding"},
        )

        table_headers = table.find("thead").find_all("th")
    table_headers = [x.text for x in table_headers]

    t_rows = table.find("tbody").find_all("tr")

    for t in t_rows:
        t_cells = t.find_all("td")
        t_cells = [x.text for x in t_cells]

        temp_df = pd.DataFrame(
            data=[t_cells],
            columns=table_headers,
            # index=[0]
        )

        player_id = t.find("a").get("href")
        # temp_df["school_name"] = school_name
        temp_df["player_url"] = f"https://stats.ncaa.org{player_id}"

        player_id = player_id.replace("/players", "").replace("/", "")
        player_id = int(player_id)

        temp_df["player_id"] = player_id

        roster_df_arr.append(temp_df)
        del temp_df

    roster_df = pd.concat(roster_df_arr, ignore_index=True)
    roster_df = roster_df.infer_objects()
    roster_df["season"] = season
    roster_df["season_name"] = season_name
    roster_df["ncaa_division"] = ncaa_division
    roster_df["ncaa_division_formatted"] = ncaa_division_formatted
    roster_df["team_conference_name"] = team_conference_name
    roster_df["school_id"] = school_id
    roster_df["school_name"] = school_name
    roster_df["sport_id"] = sport_id

    roster_df.rename(
        columns={
            "GP": "player_G",
            "GS": "player_GS",
            "#": "player_jersey_num",
            "Name": "player_full_name",
            "Class": "player_class",
            "Position": "player_positions",
            "Height": "player_height_string",
            "Bats": "player_batting_hand",
            "Throws": "player_throwing_hand",
            "Hometown": "player_hometown",
            "High School": "player_high_school",
        },
        inplace=True
    )

    # print(roster_df.columns)

    roster_df[["player_first_name", "player_last_name"]] = roster_df[
        "player_full_name"
    ].str.split(" ", n=1, expand=True)
    roster_df = roster_df.infer_objects().replace(r'^\s*$', np.nan, regex=True)

    for i in roster_df.columns:
        if i in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )

    roster_df = roster_df.infer_objects().reindex(columns=stat_columns)

    roster_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/rosters/" +
        f"{team_id}_roster.csv",
        index=False,
    )
    return roster_df


def get_volleyball_player_season_stats(
    team_id: int,
) -> pd.DataFrame:
    """
    Given a team ID, this function retrieves and parses
    the season stats for all of the players in a given volleyball team.

    Parameters
    ----------
    `team_id` (int, mandatory):
        Required argument.
        Specifies the team you want volleyball stats from.
        This is separate from a school ID, which identifies the institution.
        A team ID should be unique to a school, and a season.

    Usage
    ----------
    ```python

    from ncaa_stats_py.volleyball import get_volleyball_player_season_stats


    ########################################
    #          Women's volleyball          #
    ########################################

    # Get the season stats for the
    # 2024 Ohio St. team (D1, ID: 585398).
    print(
        "Get the season stats for the " +
        "2024 Ohio St. WVB team (D1, ID: 585398)."
    )
    df = get_volleyball_player_season_stats(585398)
    print(df)

    # Get the season stats for the
    # 2023 Emory & Henry WVB team (D2, ID: 559738).
    print(
        "Get the season stats for the " +
        "2023 Emory & Henry WVB team (D2, ID: 559738)."
    )
    df = get_volleyball_player_season_stats(559738)
    print(df)

    # Get the season stats for the
    # 2022 Fredonia WVB team (D3, ID: 539881).
    print(
        "Get the season stats for the " +
        "2022 Fredonia WVB team (D3, ID: 539881)."
    )
    df = get_volleyball_player_season_stats(539881)
    print(df)

    # Get the season stats for the
    # 2021 Oklahoma WVB team (D1, ID: 523163).
    print(
        "Get the season stats for the " +
        "2021 Oklahoma WVB team (D1, ID: 523163)."
    )
    df = get_volleyball_player_season_stats(523163)
    print(df)

    # Get the season stats for the
    # 2020 North Greenville WVB team (D2, ID: 504820).
    print(
        "Get the season stats for the " +
        "2020 North Greenville WVB team (D2, ID: 504820)."
    )
    df = get_volleyball_player_season_stats(504820)
    print(df)

    # Get the season stats for the
    # 2019 SUNY Potsdam team (D3, ID: 482714).
    print(
        "Get the season stats for the " +
        "2019 SUNY Potsdam team (D3, ID: 482714)."
    )
    df = get_volleyball_player_season_stats(482714)
    print(df)

    ########################################
    #          Men's volleyball            #
    ########################################

    # Get the season stats for the
    # 2024 Lees-McRae MVB team (D1, ID: 573699).
    print(
        "Get the season stats for the " +
        "2024 Lees-McRae MVB team (D1, ID: 573699)."
    )
    df = get_volleyball_player_season_stats(573699)
    print(df)

    # Get the season stats for the
    # 2023 Elizabethtown MVB team (D3, ID: 550871).
    print(
        "Get the season stats for the " +
        "2023 Elizabethtown MVB team (D3, ID: 550871)."
    )
    df = get_volleyball_player_season_stats(550871)
    print(df)

    # Get the season stats for the
    # 2022 Limestone MVB team (D1, ID: 529884).
    print(
        "Get the season stats for the " +
        "2022 Limestone MVB team (D1, ID: 529884)."
    )
    df = get_volleyball_player_season_stats(529884)
    print(df)

    # Get the season stats for the
    # 2021 Maranatha Baptist MVB team (D3, ID: 508471).
    print(
        "Get the season stats for the " +
        "2021 Maranatha Baptist MVB team (D3, ID: 508471)."
    )
    df = get_volleyball_player_season_stats(508471)
    print(df)

    # Get the season stats for the
    # 2020 CUI MVB team (D1, ID: 484972).
    print(
        "Get the season stats for the " +
        "2020 CUI MVB team (D1, ID: 484972)."
    )
    df = get_volleyball_player_season_stats(484972)
    print(df)

    # Get the season stats for the
    # 2019 SUNY New Paltz MVB team (D3, ID: 453851).
    print(
        "Get the season stats for the " +
        "2019 SUNY New Paltz MVB team (D3, ID: 453851)."
    )
    df = get_volleyball_player_season_stats(453851)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with the season batting stats for
    all players with a given NCAA volleyball team.
    """

    sport_id = ""
    load_from_cache = True
    stats_df = pd.DataFrame()
    stats_df_arr = []
    temp_df = pd.DataFrame()

    stat_columns = [
        "season",
        "season_name",
        "sport_id",
        "team_id",
        "team_conference_name",
        "school_id",
        "school_name",
        "ncaa_division",
        "ncaa_division_formatted",
        "player_id",
        "player_jersey_number",
        "player_last_name",
        "player_first_name",
        "player_full_name",
        "player_class",
        "player_position",
        "player_height",
        "GP",
        "GS",
        "sets_played",
        "MS",
        "kills",
        "errors",
        "total_attacks",
        "hit%",
        "assists",
        "aces",
        "serve_errors",
        "digs",
        "return_attacks",
        "return_errors",
        "solo_blocks",
        "assisted_blocks",
        "block_errors",
        "total_blocks",
        "points",
        "BHE",
        "serve_attempts",
        "DBL_DBL",
        "TRP_DBL",
    ]

    try:
        team_df = load_volleyball_teams()

        team_df = team_df[team_df["team_id"] == team_id]

        season = team_df["season"].iloc[0]
        ncaa_division = int(team_df["ncaa_division"].iloc[0])
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        team_conference_name = team_df["team_conference_name"].iloc[0]
        school_name = team_df["school_name"].iloc[0]
        school_id = int(team_df["school_id"].iloc[0])
        sport_id = "WVB"
    except Exception:
        team_df = load_volleyball_teams(get_mens_data=True)

        team_df = team_df[team_df["team_id"] == team_id]

        season = team_df["season"].iloc[0]
        ncaa_division = int(team_df["ncaa_division"].iloc[0])
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        team_conference_name = team_df["team_conference_name"].iloc[0]
        school_name = team_df["school_name"].iloc[0]
        school_id = int(team_df["school_id"].iloc[0])
        sport_id = "MVB"

    del team_df

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    url = f"https://stats.ncaa.org/teams/{team_id}/season_to_date_stats"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/" +
        f"volleyball_{sport_id}/player_season_stats/"
    ):
        pass
    else:
        mkdir(
            f"{home_dir}/.ncaa_stats_py/" +
            f"volleyball_{sport_id}/player_season_stats/"
        )

    if exists(
        f"{home_dir}/.ncaa_stats_py/" +
        f"volleyball_{sport_id}/player_season_stats/"
        + f"{season:00d}_{school_id:00d}_player_season_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/" +
            f"volleyball_{sport_id}/player_season_stats/"
            + f"{season:00d}_{school_id:00d}_player_season_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/" +
                f"volleyball_{sport_id}/player_season_stats/"
                + f"{season:00d}_{school_id:00d}_player_season_stats.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days > 1 and
        season >= now.year
    ):
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    response = _get_webpage(url=url)

    soup = BeautifulSoup(response.text, features="lxml")

    season_name = (
        soup.find("select", {"id": "year_list"})
        .find("option", {"selected": "selected"})
        .text
    )

    if sport_id == "MVB":
        season = f"{season_name[0:2]}{season_name[-2:]}"
        season = int(season)
    elif sport_id == "WVB":
        season = f"{season_name[0:4]}"
        season = int(season)

    table_data = soup.find(
        "table",
        {"id": "stat_grid", "class": "small_font dataTable table-bordered"},
    )

    temp_table_headers = table_data.find("thead").find("tr").find_all("th")
    table_headers = [x.text for x in temp_table_headers]

    del temp_table_headers

    t_rows = table_data.find("tbody").find_all("tr", {"class": "text"})
    for t in t_rows:
        p_last = ""
        p_first = ""
        t_cells = t.find_all("td")
        if "team" in t_cells[1].text.lower():
            continue
        p_sortable = t_cells[1].get("data-order")
        if len(p_sortable) == 2:
            p_last, p_first = p_sortable.split(",")
        elif len(p_sortable) == 3:
            p_last, temp_name, p_first = p_sortable.split(",")
            p_last = f"{p_last} {temp_name}"

        t_cells = [x.text.strip() for x in t_cells]
        t_cells = [x.replace(",", "") for x in t_cells]

        temp_df = pd.DataFrame(
            data=[t_cells],
            columns=table_headers,
            # index=[0]
        )

        player_id = t.find("a").get("href")

        # temp_df["player_url"] = f"https://stats.ncaa.org{player_id}"
        player_id = player_id.replace("/players", "").replace("/", "")

        player_id = int(player_id)

        temp_df["player_id"] = player_id
        temp_df["player_last_name"] = p_last.strip()
        temp_df["player_first_name"] = p_first.strip()

        stats_df_arr.append(temp_df)
        del temp_df

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    stats_df = stats_df.replace("", None)

    # stats_df["stat_id"] = stat_id
    stats_df["season"] = season
    stats_df["season_name"] = season_name
    stats_df["school_id"] = school_id
    stats_df["school_name"] = school_name
    stats_df["ncaa_division"] = ncaa_division
    stats_df["ncaa_division_formatted"] = ncaa_division_formatted
    stats_df["team_conference_name"] = team_conference_name
    stats_df["sport_id"] = sport_id
    stats_df["team_id"] = team_id

    stats_df = stats_df.infer_objects()

    stats_df.rename(
        columns={
            "#": "player_jersey_number",
            "Player": "player_full_name",
            "Yr": "player_class",
            "Pos": "player_position",
            "Ht": "player_height",
            "S": "sets_played",
            "Kills": "kills",
            "Errors": "errors",
            "Total Attacks": "total_attacks",
            "Hit Pct": "hit%",
            "Assists": "assists",
            "Aces": "aces",
            "SErr": "serve_errors",
            "Digs": "digs",
            "RetAtt": "return_attacks",
            "RErr": "return_errors",
            "Block Solos": "solo_blocks",
            "Block Assists": "assisted_blocks",
            "BErr": "block_errors",
            "PTS": "points",
            "Trpl Dbl": "TRP_DBL",
            "Dbl Dbl": "DBL_DBL",
            "TB": "total_blocks",
            "SrvAtt": "serve_attempts",
        },
        inplace=True,
    )

    for i in stats_df.columns:
        if i in stat_columns:
            pass
        elif "Attend" in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )
    stats_df = stats_df.reindex(columns=stat_columns)

    stats_df = stats_df.infer_objects().fillna(0)
    stats_df = stats_df.astype(
        {
            "GP": "uint16",
            "GS": "uint16",
            "sets_played": "uint16",
            "kills": "uint16",
            "errors": "uint16",
            "total_attacks": "uint16",
            "hit%": "float32",
            "assists": "uint16",
            "aces": "uint16",
            "serve_errors": "uint16",
            "digs": "uint16",
            "return_attacks": "uint16",
            "return_errors": "uint16",
            "solo_blocks": "uint16",
            "assisted_blocks": "uint16",
            "block_errors": "uint16",
            "points": "float32",
            "BHE": "uint16",
            "TRP_DBL": "uint16",
            "serve_attempts": "uint16",
            "total_blocks": "float32",
            "DBL_DBL": "uint16",
            "school_id": "uint32",
        }
    )

    stats_df["hit%"] = stats_df["hit%"].round(3)
    stats_df["points"] = stats_df["points"].round(1)

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/" +
        f"volleyball_{sport_id}/player_season_stats/" +
        f"{season:00d}_{school_id:00d}_player_season_stats.csv",
        index=False,
    )

    return stats_df


def get_volleyball_player_game_stats(
    player_id: int
) -> pd.DataFrame:
    """
    Given a valid player ID and season,
    this function retrieves the game stats for this player at a game level.

    Parameters
    ----------
    `player_id` (int, mandatory):
        Required argument.
        Specifies the player you want game stats from.

    `season` (int, mandatory):
        Required argument.
        Specifies the season you want game stats from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.volleyball import (
        get_volleyball_player_game_stats
    )

    ########################################
    #          Women's volleyball          #
    ########################################

    # Get the game stats of Zuzanna Wieczorek in 2024 (Idaho).
    print(
        "Get the game stats of Zuzanna Wieczorek in 2024 (Idaho)."
    )
    df = get_volleyball_player_game_stats(player_id=8432514)
    print(df)

    # Get the game stats of Jalyn Stevenson in 2023 (Washburn, D2).
    print(
        "Get the game stats of Jalyn Stevenson in 2023 (Washburn, D2)."
    )
    df = get_volleyball_player_game_stats(player_id=8145555)
    print(df)

    # Get the game stats of Lauren Gips in 2022 (Babson, D3).
    print(
        "Get the game stats of Lauren Gips in 2022 (Babson, D3)."
    )
    df = get_volleyball_player_game_stats(player_id=7876821)
    print(df)

    # Get the game stats of Rhett Robinson in 2021 (North Texas).
    print(
        "Get the game stats of Rhett Robinson in 2021 (North Texas)."
    )
    df = get_volleyball_player_game_stats(player_id=7234089)
    print(df)

    # Get the game stats of Audrey Keenan in 2020 (Florida Tech, D2).
    print(
        "Get the game stats of Audrey Keenan in 2020 (Florida Tech, D2)."
    )
    df = get_volleyball_player_game_stats(player_id=6822147)
    print(df)

    # Get the game stats of Ta'korya Green in 2019 (Oglethorpe, D3).
    print(
        "Get the game stats of Ta'korya Green in 2019 (Oglethorpe, D3)."
    )
    df = get_volleyball_player_game_stats(player_id=6449807)
    print(df)

    ########################################
    #          Men's volleyball            #
    ########################################

    # Get the game stats of Matthew Gentry in 2024 (Lincoln Memorial).
    print(
        "Get the game stats of Matthew Gentry in 2024 (Lincoln Memorial)."
    )
    df = get_volleyball_player_game_stats(player_id=8253076)
    print(df)

    # Get the game stats of Ray Rodriguez in 2023 (Lehman, D3).
    print(
        "Get the game stats of Ray Rodriguez in 2023 (Lehman, D3)."
    )
    df = get_volleyball_player_game_stats(player_id=7883459)
    print(df)

    # Get the game stats of Gannon Chinen in 2022 (Alderson Broaddus).
    print(
        "Get the game stats of Gannon Chinen in 2022 (Alderson Broaddus)."
    )
    df = get_volleyball_player_game_stats(player_id=7413984)
    print(df)

    # Get the game stats of Tyler Anderson in 2021 (Alvernia, D3).
    print(
        "Get the game stats of Tyler Anderson in 2021 (Alvernia, D3)."
    )
    df = get_volleyball_player_game_stats(player_id=7118023)
    print(df)

    # Get the game stats of Jaylen Jasper in 2020 (Stanford).
    print(
        "Get the game stats of Jaylen Jasper in 2020 (Stanford)."
    )
    df = get_volleyball_player_game_stats(player_id=6357146)
    print(df)

    # Get the game stats of Brian Sheddy in 2019 (Penn St.-Altoona, D3).
    print(
        "Get the game stats of Brian Sheddy in 2019 (Penn St.-Altoona, D3)."
    )
    df = get_volleyball_player_game_stats(player_id=5816111)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a player's batting game logs
    in a given season.
    """
    sport_id = ""

    stat_columns = [
        "season",
        "sport_id",
        "game_id",
        "game_num",
        "player_id",
        "date",
        "opponent",
        "Result",
        "team_sets_won",
        "opponent_sets_won",
        "GP",
        # "GS",
        "sets_played",
        "MS",
        "kills",
        "errors",
        "total_attacks",
        "hit%",
        "assists",
        "aces",
        "serve_errors",
        "digs",
        "return_attacks",
        "return_errors",
        "solo_blocks",
        "assisted_blocks",
        "block_errors",
        "total_blocks",
        "points",
        "BHE",
        "serve_attempts",
        "DBL_DBL",
        "TRP_DBL",
    ]

    load_from_cache = True
    stats_df = pd.DataFrame()
    stats_df_arr = []
    temp_df = pd.DataFrame()
    sport_id = ""
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    # stat_id = _get_stat_id(
    #     sport="volleyball",
    #     season=season,
    #     stat_type="batting"
    # )
    url = f"https://stats.ncaa.org/players/{player_id}"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/player_game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/player_game_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_MVB/player_game_stats/"
        + f"{player_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_MVB/player_game_stats/"
            + f"{player_id}_player_game_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/volleyball_MVB/"
                + "player_game_stats/"
                + f"{player_id}_player_game_stats.csv"
            )
        )
        games_df = games_df.infer_objects()
        load_from_cache = True
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/player_game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/player_game_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_WVB/player_game_stats/"
        + f"{player_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_WVB/player_game_stats/"
            + f"{player_id}_player_game_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/volleyball_WVB/"
                + "player_game_stats/"
                + f"{player_id}_player_game_stats.csv"
            )
        )
        games_df = games_df.infer_objects()
        load_from_cache = True
    else:
        logging.info("Could not find a WVB player game stats file")

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days >= 1
    ):
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    # team_df = load_volleyball_teams()

    # team_df = team_df[team_df["team_id"] == team_id]

    # season = team_df["season"].iloc[0]
    # ncaa_division = team_df["ncaa_division"].iloc[0]
    # ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
    # team_conference_name = team_df["team_conference_name"].iloc[0]
    # school_name = team_df["school_name"].iloc[0]
    # school_id = int(team_df["school_id"].iloc[0])

    # del team_df
    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")

    table_navigation = soup.find("ul", {"class": "nav nav-tabs padding-nav"})
    table_nav_card = table_navigation.find_all("a")

    for u in table_nav_card:
        url_str = u.get("href")
        if "MVB" in url_str.upper():
            sport_id = "MVB"
        elif "WVB" in url_str.upper():
            sport_id = "WVB"

    if sport_id is None or len(sport_id) == 0:
        # This should **never** be the case IRL,
        # but in case something weird happened and
        # we can't make a determination of if this is a
        # MVB player or a WVB player, and we somehow haven't
        # crashed by this point, set the sport ID to
        # "MVB" by default so we don't have other weirdness.
        logging.error(
            f"Could not determine if player ID {player_id} " +
            "is a MVB or a WVB player. " +
            "Because this cannot be determined, " +
            "we will make the automatic assumption that this is a MVB player."
        )
        sport_id = "MVB"

    table_data = soup.find_all(
        "table", {"class": "small_font dataTable table-bordered"}
    )[1]

    temp_table_headers = table_data.find("thead").find("tr").find_all("th")
    table_headers = [x.text for x in temp_table_headers]

    del temp_table_headers

    temp_t_rows = table_data.find("tbody")
    temp_t_rows = temp_t_rows.find_all("tr")
    season_name = (
        soup.find("select", {"id": "year_list"})
        .find("option", {"selected": "selected"})
        .text
    )

    if sport_id == "MVB":
        season = f"{season_name[0:2]}{season_name[-2:]}"
        season = int(season)
    elif sport_id == "WVB":
        season = f"{season_name[0:4]}"
        season = int(season)

    for t in temp_t_rows:
        game_num = 1
        ot_periods = 0
        # innings = 9
        row_id = t.get("id")
        opp_team_name = ""

        if "contest" not in row_id:
            continue
        del row_id

        t_cells = t.find_all("td")
        t_cells = [x.text.strip() for x in t_cells]

        g_date = t_cells[0]

        if "(" in g_date:
            g_date, game_num = g_date.split("(")
            g_date = g_date.strip()

            game_num = game_num.replace(")", "")
            game_num = int(game_num)

        try:
            opp_team_id = t.find_all("td")[1].find("a").get("href")
        except AttributeError as e:
            logging.info(
                "Could not extract a team ID for this game. " +
                f"Full exception {e}"
            )
        except Exception as e:
            logging.warning(
                "An unhandled exception has occurred when "
                + "trying to get the opposition team ID for this game. "
                f"Full exception `{e}`."
            )
            raise e

        try:
            opp_team_id = opp_team_id.replace("/teams/", "")
            opp_team_id = opp_team_id.replace(
                "javascript:toggleDefensiveStats(", ""
            )
            opp_team_id = opp_team_id.replace(");", "")
            opp_team_id = int(opp_team_id)

            temp_df["opponent_team_id"] = opp_team_id
        except Exception:
            logging.info(
                "Couldn't find the opposition team naIDme "
                + "for this row. "
            )
            opp_team_id = None
        # print(i.find("td").text)
        try:
            opp_team_name = t.find_all("td")[1].find_all("img")[1].get("alt")
        except AttributeError:
            logging.info(
                "Couldn't find the opposition team name "
                + "for this row from an image element. "
                + "Attempting a backup method"
            )
            opp_team_name = t_cells[1]
        except IndexError:
            logging.info(
                "Couldn't find the opposition team name "
                + "for this row from an image element. "
                + "Attempting a backup method"
            )
            opp_team_name = t_cells[1]
        except Exception as e:
            logging.warning(
                "Unhandled exception when trying to get the "
                + "opposition team name from this game. "
                + f"Full exception `{e}`"
            )
            raise e

        if opp_team_name == "Defensive Stats":
            opp_team_name = t_cells[1]

        if "@" in opp_team_name:
            opp_team_name = opp_team_name.split("@")[0]

        result_str = t_cells[2]

        result_str = (
            result_str.lower().replace("w", "").replace("l", "").replace(
                "t", ""
            )
        )

        if (
            result_str.lower() == "ppd" or
            result_str.lower() == "" or
            result_str.lower() == "canceed"
        ):
            continue

        result_str = result_str.replace("\n", "")
        result_str = result_str.replace("*", "")

        tm_score, opp_score = result_str.split("-")
        t_cells = [x.replace("*", "") for x in t_cells]
        t_cells = [x.replace("/", "") for x in t_cells]
        t_cells = [x.replace("\\", "") for x in t_cells]

        temp_df = pd.DataFrame(
            data=[t_cells],
            columns=table_headers,
            # index=[0]
        )

        tm_score = int(tm_score)
        if "(" in opp_score:
            opp_score = opp_score.replace(")", "")
            opp_score, ot_periods = opp_score.split("(")
            temp_df["ot_periods"] = ot_periods

        if "\n" in opp_score:
            opp_score = opp_score.strip()
            # opp_score = opp_score
        opp_score = int(opp_score)

        temp_df["team_sets_won"] = tm_score
        temp_df["opponent_sets_won"] = opp_score

        del tm_score
        del opp_score

        try:
            g_id = t.find_all("td")[2].find("a").get("href")

            g_id = g_id.replace("/contests", "")
            g_id = g_id.replace("/box_score", "")
            g_id = g_id.replace("/", "")

            g_id = int(g_id)
            temp_df["game_id"] = g_id
            del g_id
        except AttributeError:
            logging.warning(
                f"Could not find a game ID for a {g_date} game " +
                f"against {opp_team_name}."
            )
            temp_df["game_id"] = None
        except Exception as e:
            raise e

        temp_df.rename(
            columns={"Opponent": "opponent", "Date": "date"},
            inplace=True,
        )
        game_date = datetime.strptime(g_date, "%m/%d/%Y").date()

        temp_df["date"] = game_date
        temp_df["game_num"] = game_num
        # temp_df["game_innings"] = innings

        if len(opp_team_name) > 0:
            temp_df["opponent"] = opp_team_name
        del opp_team_name

        duplicate_cols = temp_df.columns[temp_df.columns.duplicated()]
        temp_df.drop(columns=duplicate_cols, inplace=True)

        stats_df_arr.append(temp_df)
        del temp_df

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    stats_df = stats_df.replace("/", "", regex=True)
    stats_df = stats_df.replace("", np.nan)
    stats_df = stats_df.infer_objects()

    stats_df["player_id"] = player_id
    stats_df["sport_id"] = sport_id
    stats_df["season"] = season

    stats_df.rename(
        columns={
            "#": "player_jersey_number",
            "Player": "player_full_name",
            "Yr": "player_class",
            "Pos": "player_position",
            "Ht": "player_height",
            "S": "sets_played",
            "Kills": "kills",
            "Errors": "errors",
            "Total Attacks": "total_attacks",
            "TotalAttacks": "total_attacks",
            "Hit Pct": "hit%",
            "HitPct": "hit%",
            "Assists": "assists",
            "Aces": "aces",
            "SErr": "serve_errors",
            "Digs": "digs",
            "RetAtt": "return_attacks",
            "RErr": "return_errors",
            "Block Solos": "solo_blocks",
            "BlockSolos": "solo_blocks",
            "Block Assists": "assisted_blocks",
            "BlockAssists": "assisted_blocks",
            "BErr": "block_errors",
            "PTS": "points",
            "Trpl Dbl": "TRP_DBL",
            "Dbl Dbl": "DBL_DBL",
            "TB": "total_blocks",
            "SrvAtt": "serve_attempts",
        },
        inplace=True,
    )
    # This is a separate function call because these stats
    # *don't* exist in every season.

    if "serve_attempts" not in stats_df.columns:
        stats_df["serve_attempts"] = None

    if "return_attacks" not in stats_df.columns:
        stats_df["return_attacks"] = None

    stats_df = stats_df.infer_objects().fillna(0)
    stats_df = stats_df.astype(
        {
            "GP": "uint16",
            "sets_played": "uint16",
            # "MS": "uint16",
            "kills": "uint16",
            "errors": "uint16",
            "total_attacks": "uint16",
            "hit%": "float32",
            "assists": "uint16",
            "aces": "uint16",
            "serve_errors": "uint16",
            "digs": "uint16",
            "return_attacks": "uint16",
            "return_errors": "uint16",
            "solo_blocks": "uint16",
            "assisted_blocks": "uint16",
            "block_errors": "uint16",
            # "total_blocks": "uint16",
            "points": "float32",
            "BHE": "uint16",
            "serve_attempts": "uint16",
            # "DBL_DBL": "uint8",
            # "TRP_DBL": "uint8",
        }
    )

    stats_df.loc[
        (stats_df["solo_blocks"] > 0) | (stats_df["assisted_blocks"] > 0),
        "total_blocks"
    ] = (
        stats_df["solo_blocks"] +
        (stats_df["assisted_blocks"] / 2)
    )
    stats_df["total_blocks"] = stats_df["total_blocks"].astype("float32")

    # Columns used to calculate double doubles and triple doubles.
    # Credits:
    # https://en.wikipedia.org/wiki/Double_(volleyball)
    # https://stackoverflow.com/a/54381918
    double_stats_arr = [
        "aces",
        "kills",
        "total_blocks",
        "digs",
        "assists",
    ]
    stats_df["DBL_DBL"] = (
        (
            (stats_df[double_stats_arr] >= 10).sum(1)
        ) >= 2
    )
    stats_df["DBL_DBL"] = stats_df["DBL_DBL"].astype(int)

    stats_df["TRP_DBL"] = (
        (
            (stats_df[double_stats_arr] >= 10).sum(1)
        ) >= 3
    )
    stats_df["TRP_DBL"] = stats_df["TRP_DBL"].astype(int)

    for i in stats_df.columns:
        if i in stat_columns:
            pass
        elif "Attend" in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )
    stats_df = stats_df.reindex(columns=stat_columns)

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/"
        + "player_game_stats/"
        + f"{player_id}_player_game_stats.csv",
        index=False,
    )
    return stats_df


def get_volleyball_game_player_stats(game_id: int) -> pd.DataFrame:
    """
    Given a valid game ID,
    this function will attempt to get all player game stats, if possible.

    Parameters
    ----------
    `game_id` (int, mandatory):
        Required argument.
        Specifies the game you want player game stats from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.volleyball import get_volleyball_game_player_stats

    ########################################
    #          Women's volleyball          #
    ########################################

    # Get the game stats of the
    # 2024 NCAA D1 Women's Volleyball National Championship game.
    print(
        "Get the game stats of the "
        + "2024 NCAA D1 Women's volleyball National Championship game"
    )
    df = get_volleyball_game_player_stats(6080706)
    print(df)

    # Get the game stats of a September 14th, 2024
    # game between the UNC Asheville Bulldogs and the Iona Gaels.
    print(
        "Get the game stats of a September 14th, 2024 "
        + "game between the UNC Asheville Bulldogs "
        + "and the Iona Gaels"
    )
    df = get_volleyball_game_player_stats(5670752)
    print(df)

    # Get the game stats of a September 16th, 2023
    # game between the Saginaw Valley Cardinals
    # and the Lake Superior St. Lakes.
    print(
        "Get the game stats of a September 16th, 2023 "
        + "game between the Saginaw Valley Cardinals "
        + "and the Lake Superior St. Lakes."
    )
    df = get_volleyball_game_player_stats(3243563)
    print(df)

    # Get the game stats of a October 15th, 2022
    # game between the Macalester Scots
    # and the St. Scholastica Saints (D3).
    print(
        "Get the game stats of a October 15th, 2022 "
        + "game between the Macalester Scots and "
        + "the St. Scholastica Saints (D3)."
    )
    df = get_volleyball_game_player_stats(2307684)
    print(df)

    # Get the game stats of a October 24th, 2021
    # game between the Howard Bison and the UMES Hawks.
    print(
        "Get the game stats of a October 24th, 2021 "
        + "game between the Howard Bison and the UMES Hawks."
    )
    df = get_volleyball_game_player_stats(2113627)
    print(df)

    # Get the game stats of a March 5th, 2021
    # game between the Notre Dame (OH) Falcons
    # and the Alderson Broaddus Battlers.
    print(
        "Get the game stats of a March 5th, 2021 "
        + "game between the Notre Dame (OH) Falcons "
        + "and the Alderson Broaddus Battlers."
    )
    df = get_volleyball_game_player_stats(2005442)
    print(df)

    # Get the game stats of a November 14th, 2019
    # game between the Wittenberg Tigers
    # and the Muskingum Fighting Muskies (D3).
    print(
        "Get the game stats of a November 14th, 2019 "
        + "game between the Wittenberg Tigers and "
        + "the Muskingum Fighting Muskies (D3)."
    )
    df = get_volleyball_game_player_stats(1815514)
    print(df)

    ########################################
    #          Men's volleyball            #
    ########################################

    # Get the game stats of the
    # 2024 NCAA D1 Men's Volleyball National Championship game.
    print(
        "Get the game stats of the "
        + "2024 NCAA D1 Men's volleyball National Championship game"
    )
    df = get_volleyball_game_player_stats(5282845)
    print(df)

    # Get the game stats of a January 14th, 2025
    # game between the Kean Cougars and the Arcadia Knights.
    print(
        "Get the game stats of a January 14th, 2025 "
        + "game between the UNC Asheville Bulldogs "
        + "and the Iona Gaels"
    )
    df = get_volleyball_game_player_stats(6081598)
    print(df)

    # Get the game stats of a January 13th, 2024
    # game between the Purdue Fort Wayne Mastodons and the NJIT Highlanders.
    print(
        "Get the game stats of a September 14th, 2024 "
        + "game between the Purdue Fort Wayne Mastodons "
        + "and the NJIT Highlanders."
    )
    df = get_volleyball_game_player_stats(4473231)
    print(df)

    # Get the game stats of a January 21st, 2023
    # game between the Baruch Bearcats and the Widener Pride.
    print(
        "Get the game stats of a January 21st, 2023 "
        + "game between the Baruch Bearcats and the Widener Pride."
    )
    df = get_volleyball_game_player_stats(2355323)
    print(df)

    # Get the game stats of a February 24th, 2022
    # game between the Ball St. Cardinals and the Lindenwood Lions.
    print(
        "Get the game stats of a February 24th, 2022 "
        + "game between the Ball St. Cardinals and the Lindenwood Lions."
    )
    df = get_volleyball_game_player_stats(2162239)
    print(df)

    # Get the game stats of a March 20th, 2021
    # game between the SUNY New Paltz Hawks and the St. John Fisher Cardinals.
    print(
        "Get the game stats of a March 20th, 2021 "
        + "game between the SUNY New Paltz Hawks "
        + "and the St. John Fisher Cardinals."
    )
    df = get_volleyball_game_player_stats(2059180)
    print(df)

    # Get the game stats of a March 1th, 2020
    # game between the USC Trojans and the CUI Golden Eagles.
    print(
        "Get the game stats of a March 1th, 2020 "
        + "game between the USC Trojans and the CUI Golden Eagles."
    )
    df = get_volleyball_game_player_stats(1820058)
    print(df)

    # Get the game stats of an April 4th, 2019
    # game between the Lesly Lynx and the Pine Manor Gators (D3).
    print(
        "Get the game stats of an April 4th, 2019 "
        + "game between the Lesly Lynx and the Pine Manor Gators (D3)."
    )
    df = get_volleyball_game_player_stats(1723131)
    print(df)


    ```

    Returns
    ----------
    A pandas `DataFrame` object with player game stats in a given game.

    """
    load_from_cache = True

    sport_id = ""
    season = 0

    MVB_teams_df = load_volleyball_teams(get_mens_data=True)
    MVB_team_ids_arr = MVB_teams_df["team_id"].to_list()

    WVB_teams_df = load_volleyball_teams(get_mens_data=False)
    WVB_team_ids_arr = WVB_teams_df["team_id"].to_list()

    stats_df = pd.DataFrame()
    stats_df_arr = []

    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    stat_columns = [
        "season",
        "sport_id",
        "game_datetime",
        "game_id",
        "team_id",
        "team_name",
        "player_id",
        "player_num",
        "player_full_name",
        "player_position",
        "GP",
        "sets_played",
        "kills",
        "errors",
        "total_attacks",
        "hit%",
        "assists",
        "aces",
        "serve_errors",
        "digs",
        "return_attacks",
        "return_errors",
        "solo_blocks",
        "assisted_blocks",
        "block_errors",
        "total_blocks",
        "points",
        "BHE",
        "DBL_DBL",
        "TRP_DBL",
    ]

    url = f"https://stats.ncaa.org/contests/{game_id}/individual_stats"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/game_stats/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/game_stats/player/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/game_stats/player/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_MVB/game_stats/player/"
        + f"{game_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_MVB/game_stats/player/"
            + f"{game_id}_player_game_stats.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/volleyball_MVB/game_stats/player/"
                + f"{game_id}_player_game_stats.csv"
            )
        )
        load_from_cache = True
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/game_stats/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/game_stats/player/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/game_stats/player/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_WVB/game_stats/player/"
        + f"{game_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_WVB/game_stats/player/"
            + f"{game_id}_player_game_stats.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/volleyball_WVB/game_stats/player/"
                + f"{game_id}_player_game_stats.csv"
            )
        )
        load_from_cache = True
    else:
        logging.info("Could not find a WVB player game stats file")

    now = datetime.today()

    age = now - file_mod_datetime

    if age.days >= 35:
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")

    info_table = soup.find(
        "td",
        {
            "style": "padding: 0px 30px 0px 30px",
            "class": "d-none d-md-table-cell"
        }
    ).find(
        "table",
        {"style": "border-collapse: collapse"}
    )

    info_table_rows = info_table.find_all("tr")

    game_date_str = info_table_rows[3].find("td").text
    if "TBA" in game_date_str:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y TBA')
    elif "tba" in game_date_str:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y tba')
    elif "TBD" in game_date_str:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y TBD')
    elif "tbd" in game_date_str:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y tbd')
    elif (
        "tbd" not in game_date_str.lower() and
        ":" not in game_date_str.lower()
    ):
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y')
    else:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y %I:%M %p')
    game_datetime = game_datetime.astimezone(timezone("US/Eastern"))
    game_date_str = game_datetime.isoformat()
    del game_datetime

    table_boxes = soup.find_all("div", {"class": "card p-0 table-responsive"})

    for box in table_boxes:
        t_header = box.find(
            "div", {"class": "card-header"}
        ).find(
            "div", {"class": "row"}
        )

        t_header_str = t_header.text
        t_header_str = t_header_str.replace("Period Stats", "")
        t_header_str = t_header_str.replace("\n", "")
        t_header_str = t_header_str.strip()

        team_id = t_header.find("a").get("href")
        team_id = team_id.replace("/teams", "")
        team_id = team_id.replace("/", "")
        team_id = int(team_id)

        table_data = box.find(
            "table",
            {"class": "display dataTable small_font"}
        )
        table_headers = box.find("thead").find_all("th")
        table_headers = [x.text for x in table_headers]

        temp_t_rows = table_data.find("tbody")
        temp_t_rows = temp_t_rows.find_all("tr")

        spec_stats_df = pd.DataFrame()
        spec_stats_df_arr = []
        for t in temp_t_rows:
            # row_id = t.get("id")
            game_played = 1
            # game_started = 0

            try:
                player_id = t.find("a").get("href")
                player_id = player_id.replace("/players", "")
                player_id = player_id.replace("/player", "")
                player_id = player_id.replace("/", "")
            except Exception as e:
                logging.debug(
                    "Could not replace player IDs. " +
                    f"Full exception: `{e}`"
                )

            t_cells = t.find_all("td")
            p_name = t_cells[1].text.replace("\n", "")
            p_name = p_name.strip()

            if t_header_str in p_name:
                continue
            elif p_name.lower() == "team":
                continue
            # if "\xa0" in p_name:
            #     game_started = 0

            t_cells = [x.text.strip() for x in t_cells]
            player_id = int(player_id)

            temp_df = pd.DataFrame(
                data=[t_cells],
                columns=table_headers
            )

            duplicate_cols = temp_df.columns[temp_df.columns.duplicated()]
            temp_df.drop(columns=duplicate_cols, inplace=True)

            temp_df["player_id"] = player_id
            temp_df["GP"] = game_played
            # temp_df["GS"] = game_started

            spec_stats_df_arr.append(temp_df)
            del temp_df

        spec_stats_df = pd.concat(
            spec_stats_df_arr,
            ignore_index=True
        )

        if team_id in MVB_team_ids_arr:
            sport_id = "MVB"
            df = MVB_teams_df[MVB_teams_df["team_id"] == team_id]
            season = df["season"].iloc[0]
        elif team_id in WVB_team_ids_arr:
            sport_id = "WVB"
            df = WVB_teams_df[WVB_teams_df["team_id"] == team_id]
            season = df["season"].iloc[0]
        else:
            raise ValueError(
                f"Unhandled team ID {team_id}"
            )

        spec_stats_df["team_id"] = team_id
        spec_stats_df["team_name"] = t_header_str
        stats_df_arr.append(spec_stats_df)
        del spec_stats_df

    stats_df = pd.concat(stats_df_arr)
    stats_df["season"] = season
    stats_df.rename(
        columns={
            "#": "player_num",
            "Name": "player_full_name",
            "P": "player_position",
            "Ht": "player_height",
            "S": "sets_played",
            "Kills": "kills",
            "Errors": "errors",
            "Total Attacks": "total_attacks",
            "TotalAttacks": "total_attacks",
            "Hit Pct": "hit%",
            "HitPct": "hit%",
            "Assists": "assists",
            "Aces": "aces",
            "SErr": "serve_errors",
            "Digs": "digs",
            "RetAtt": "return_attacks",
            "RErr": "return_errors",
            "Block Solos": "solo_blocks",
            "BlockSolos": "solo_blocks",
            "Block Assists": "assisted_blocks",
            "BlockAssists": "assisted_blocks",
            "BErr": "block_errors",
            "PTS": "points",
            "Trpl Dbl": "TRP_DBL",
            "Dbl Dbl": "DBL_DBL",
            "TB": "total_blocks",
            "SrvAtt": "serve_attempts",
        },
        inplace=True,
    )

    if "return_attacks" not in stats_df.columns:
        stats_df["return_attacks"] = None

    if "serve_attempts" not in stats_df.columns:
        stats_df["serve_attempts"] = None

    stats_df = stats_df.infer_objects().fillna(0)
    stats_df = stats_df.astype(
        {
            "GP": "uint16",
            "sets_played": "uint16",
            # "MS": "uint16",
            "kills": "uint16",
            "errors": "uint16",
            "total_attacks": "uint16",
            "hit%": "float32",
            "assists": "uint16",
            "aces": "uint16",
            "serve_errors": "uint16",
            "digs": "uint16",
            "return_attacks": "uint16",
            "return_errors": "uint16",
            "solo_blocks": "uint16",
            "assisted_blocks": "uint16",
            "block_errors": "uint16",
            # "total_blocks": "uint16",
            "points": "float32",
            "BHE": "uint16",
            "serve_attempts": "uint16",
            # "DBL_DBL": "uint8",
            # "TRP_DBL": "uint8",
        }
    )
    # print(stats_df.columns)
    stats_df["game_datetime"] = game_date_str
    stats_df["sport_id"] = sport_id

    stats_df["game_id"] = game_id

    stats_df["total_blocks"] = (
        stats_df["solo_blocks"] +
        (stats_df["assisted_blocks"] / 2)
    )
    stats_df["total_blocks"] = stats_df["total_blocks"].astype("float32")

    # Columns used to calculate double doubles and triple doubles.
    # Credits:
    # https://en.wikipedia.org/wiki/Double_(volleyball)
    # https://stackoverflow.com/a/54381918
    double_stats_arr = [
        "aces",
        "kills",
        "total_blocks",
        "digs",
        "assists",
    ]
    stats_df["DBL_DBL"] = ((stats_df[double_stats_arr] >= 10).sum(1)) >= 2
    stats_df["DBL_DBL"] = stats_df["DBL_DBL"].astype(int)

    stats_df["TRP_DBL"] = ((stats_df[double_stats_arr] >= 10).sum(1)) >= 3
    stats_df["TRP_DBL"] = stats_df["TRP_DBL"].astype(int)

    for i in stats_df.columns:
        if i in stat_columns:
            pass
        elif "Attend" in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )

    stats_df = stats_df.reindex(
        columns=stat_columns
    )

    # print(stats_df.columns)
    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/game_stats/player/"
        + f"{game_id}_player_game_stats.csv",
        index=False
    )
    return stats_df


def get_volleyball_game_team_stats(game_id: int) -> pd.DataFrame:
    """
    Given a valid game ID,
    this function will attempt to get all team game stats, if possible.

    Parameters
    ----------
    `game_id` (int, mandatory):
        Required argument.
        Specifies the game you want team game stats from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.volleyball import get_volleyball_game_team_stats

    ########################################
    #          Women's volleyball          #
    ########################################

    # Get the game stats of the
    # 2024 NCAA D1 Women's Volleyball National Championship game.
    print(
        "Get the game stats of the "
        + "2024 NCAA D1 Women's volleyball National Championship game"
    )
    df = get_volleyball_game_team_stats(6080706)
    print(df)

    # Get the game stats of a September 14th, 2024
    # game between the UNC Asheville Bulldogs and the Iona Gaels.
    print(
        "Get the game stats of a September 14th, 2024 "
        + "game between the UNC Asheville Bulldogs "
        + "and the Iona Gaels"
    )
    df = get_volleyball_game_team_stats(5670752)
    print(df)

    # Get the game stats of a September 16th, 2023
    # game between the Saginaw Valley Cardinals
    # and the Lake Superior St. Lakes.
    print(
        "Get the game stats of a September 16th, 2023 "
        + "game between the Saginaw Valley Cardinals "
        + "and the Lake Superior St. Lakes."
    )
    df = get_volleyball_game_team_stats(3243563)
    print(df)

    # Get the game stats of a October 15th, 2022
    # game between the Macalester Scots
    # and the St. Scholastica Saints (D3).
    print(
        "Get the game stats of a October 15th, 2022 "
        + "game between the Macalester Scots and "
        + "the St. Scholastica Saints (D3)."
    )
    df = get_volleyball_game_team_stats(2307684)
    print(df)

    # Get the game stats of a October 24th, 2021
    # game between the Howard Bison and the UMES Hawks.
    print(
        "Get the game stats of a October 24th, 2021 "
        + "game between the Howard Bison and the UMES Hawks."
    )
    df = get_volleyball_game_team_stats(2113627)
    print(df)

    # Get the game stats of a March 5th, 2021
    # game between the Notre Dame (OH) Falcons
    # and the Alderson Broaddus Battlers.
    print(
        "Get the game stats of a March 5th, 2021 "
        + "game between the Notre Dame (OH) Falcons "
        + "and the Alderson Broaddus Battlers."
    )
    df = get_volleyball_game_team_stats(2005442)
    print(df)

    # Get the game stats of a November 14th, 2019
    # game between the Wittenberg Tigers
    # and the Muskingum Fighting Muskies (D3).
    print(
        "Get the game stats of a November 14th, 2019 "
        + "game between the Wittenberg Tigers and "
        + "the Muskingum Fighting Muskies (D3)."
    )
    df = get_volleyball_game_team_stats(1815514)
    print(df)

    ########################################
    #          Men's volleyball            #
    ########################################

    # Get the game stats of the
    # 2024 NCAA D1 Men's Volleyball National Championship game.
    print(
        "Get the game stats of the "
        + "2024 NCAA D1 Men's volleyball National Championship game"
    )
    df = get_volleyball_game_team_stats(5282845)
    print(df)

    # Get the game stats of a January 14th, 2025
    # game between the Kean Cougars and the Arcadia Knights.
    print(
        "Get the game stats of a January 14th, 2025 "
        + "game between the UNC Asheville Bulldogs "
        + "and the Iona Gaels"
    )
    df = get_volleyball_game_team_stats(6081598)
    print(df)

    # Get the game stats of a January 13th, 2024
    # game between the Purdue Fort Wayne Mastodons and the NJIT Highlanders.
    print(
        "Get the game stats of a September 14th, 2024 "
        + "game between the Purdue Fort Wayne Mastodons "
        + "and the NJIT Highlanders."
    )
    df = get_volleyball_game_team_stats(4473231)
    print(df)

    # Get the game stats of a January 21st, 2023
    # game between the Baruch Bearcats and the Widener Pride.
    print(
        "Get the game stats of a January 21st, 2023 "
        + "game between the Baruch Bearcats and the Widener Pride."
    )
    df = get_volleyball_game_team_stats(2355323)
    print(df)

    # Get the game stats of a February 24th, 2022
    # game between the Ball St. Cardinals and the Lindenwood Lions.
    print(
        "Get the game stats of a February 24th, 2022 "
        + "game between the Ball St. Cardinals and the Lindenwood Lions."
    )
    df = get_volleyball_game_team_stats(2162239)
    print(df)

    # Get the game stats of a March 20th, 2021
    # game between the SUNY New Paltz Hawks and the St. John Fisher Cardinals.
    print(
        "Get the game stats of a March 20th, 2021 "
        + "game between the SUNY New Paltz Hawks "
        + "and the St. John Fisher Cardinals."
    )
    df = get_volleyball_game_team_stats(2059180)
    print(df)

    # Get the game stats of a March 1th, 2020
    # game between the USC Trojans and the CUI Golden Eagles.
    print(
        "Get the game stats of a March 1th, 2020 "
        + "game between the USC Trojans and the CUI Golden Eagles."
    )
    df = get_volleyball_game_team_stats(1820058)
    print(df)

    # Get the game stats of an April 4th, 2019
    # game between the Lesly Lynx and the Pine Manor Gators (D3).
    print(
        "Get the game stats of an April 4th, 2019 "
        + "game between the Lesly Lynx and the Pine Manor Gators (D3)."
    )
    df = get_volleyball_game_team_stats(1723131)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with team game stats in a given game.

    """
    df = get_volleyball_game_player_stats(game_id=game_id)
    # print(df.columns)
    df = df.infer_objects()
    stats_df = df.groupby(
        [
            "season",
            "sport_id",
            "game_datetime",
            "game_id",
            "team_id",
            "team_name"
        ],
        as_index=False,
    ).agg(
        {
            "sets_played": "sum",
            "kills": "sum",
            "errors": "sum",
            "total_attacks": "sum",
            # "hit%": "sum",
            "assists": "sum",
            "aces": "sum",
            "serve_errors": "sum",
            "digs": "sum",
            "return_attacks": "sum",
            "return_errors": "sum",
            "solo_blocks": "sum",
            "assisted_blocks": "sum",
            "block_errors": "sum",
            "total_blocks": "sum",
            "points": "sum",
            "BHE": "sum",
            "DBL_DBL": "sum",
            "TRP_DBL": "sum",
        }
    )
    stats_df["hit%"] = (
        (stats_df["kills"] - stats_df["errors"]) / stats_df["total_attacks"]
    )
    return stats_df


def get_volleyball_raw_pbp(game_id: int) -> pd.DataFrame:
    """
    Given a valid game ID,
    this function will attempt to get the raw play-by-play (PBP)
    data for that game.

    Parameters
    ----------
    `game_id` (int, mandatory):
        Required argument.
        Specifies the game you want play-by-play data (PBP) from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.volleyball import get_volleyball_raw_pbp

    ########################################
    #          Women's volleyball          #
    ########################################

    # Get the play-by-play data of the
    # 2024 NCAA D1 Women's Volleyball National Championship game.
    print(
        "Get the play-by-play data of the "
        + "2024 NCAA D1 Women's volleyball National Championship game"
    )
    df = get_volleyball_raw_pbp(6080706)
    print(df)

    # Get the play-by-play data of a September 14th, 2024
    # game between the UNC Asheville Bulldogs and the Iona Gaels.
    print(
        "Get the play-by-play data of a September 14th, 2024 "
        + "game between the UNC Asheville Bulldogs "
        + "and the Iona Gaels"
    )
    df = get_volleyball_raw_pbp(5670752)
    print(df)

    # Get the play-by-play data of a September 16th, 2023
    # game between the Saginaw Valley Cardinals
    # and the Lake Superior St. Lakes.
    print(
        "Get the play-by-play data of a September 16th, 2023 "
        + "game between the Saginaw Valley Cardinals "
        + "and the Lake Superior St. Lakes."
    )
    df = get_volleyball_raw_pbp(3243563)
    print(df)

    # Get the play-by-play data of a October 15th, 2022
    # game between the Macalester Scots
    # and the St. Scholastica Saints (D3).
    print(
        "Get the play-by-play data of a October 15th, 2022 "
        + "game between the Macalester Scots and "
        + "the St. Scholastica Saints (D3)."
    )
    df = get_volleyball_raw_pbp(2307684)
    print(df)

    # Get the play-by-play data of a October 24th, 2021
    # game between the Howard Bison and the UMES Hawks.
    print(
        "Get the play-by-play data of a October 24th, 2021 "
        + "game between the Howard Bison and the UMES Hawks."
    )
    df = get_volleyball_raw_pbp(2113627)
    print(df)

    # Get the play-by-play data of a March 5th, 2021
    # game between the Notre Dame (OH) Falcons
    # and the Alderson Broaddus Battlers.
    print(
        "Get the play-by-play data of a March 5th, 2021 "
        + "game between the Notre Dame (OH) Falcons "
        + "and the Alderson Broaddus Battlers."
    )
    df = get_volleyball_raw_pbp(2005442)
    print(df)

    # Get the play-by-play data of a November 14th, 2019
    # game between the Wittenberg Tigers
    # and the Muskingum Fighting Muskies (D3).
    print(
        "Get the play-by-play data of a November 14th, 2019 "
        + "game between the Wittenberg Tigers and "
        + "the Muskingum Fighting Muskies (D3)."
    )
    df = get_volleyball_raw_pbp(1815514)
    print(df)

    ########################################
    #          Men's volleyball            #
    ########################################

    # Get the play-by-play data of the
    # 2024 NCAA D1 Men's Volleyball National Championship game.
    print(
        "Get the play-by-play data of the "
        + "2024 NCAA D1 Men's volleyball National Championship game"
    )
    df = get_volleyball_raw_pbp(5282845)
    print(df)

    # Get the play-by-play data of a January 14th, 2025
    # game between the Kean Cougars and the Arcadia Knights.
    print(
        "Get the play-by-play data of a January 14th, 2025 "
        + "game between the UNC Asheville Bulldogs "
        + "and the Iona Gaels"
    )
    df = get_volleyball_raw_pbp(6081598)
    print(df)

    # Get the play-by-play data of a January 13th, 2024
    # game between the Purdue Fort Wayne Mastodons and the NJIT Highlanders.
    print(
        "Get the play-by-play data of a September 14th, 2024 "
        + "game between the Purdue Fort Wayne Mastodons "
        + "and the NJIT Highlanders."
    )
    df = get_volleyball_raw_pbp(4473231)
    print(df)

    # Get the play-by-play data of a January 21st, 2023
    # game between the Baruch Bearcats and the Widener Pride.
    print(
        "Get the play-by-play data of a January 21st, 2023 "
        + "game between the Baruch Bearcats and the Widener Pride."
    )
    df = get_volleyball_raw_pbp(2355323)
    print(df)

    # Get the play-by-play data of a February 24th, 2022
    # game between the Ball St. Cardinals and the Lindenwood Lions.
    print(
        "Get the play-by-play data of a February 24th, 2022 "
        + "game between the Ball St. Cardinals and the Lindenwood Lions."
    )
    df = get_volleyball_raw_pbp(2162239)
    print(df)

    # Get the play-by-play data of a March 7th, 2021
    # game between the Adrian Bulldogs and the Baldwin Wallace Yellow Jackets.
    print(
        "Get the play-by-play data of a March 7th, 2021 "
        + "game between the Adrian Bulldogs "
        + "and the Baldwin Wallace Yellow Jackets."
    )
    df = get_volleyball_raw_pbp(1998844)
    print(df)

    # Get the play-by-play data of a March 1th, 2020
    # game between the USC Trojans and the CUI Golden Eagles.
    print(
        "Get the play-by-play data of a March 1th, 2020 "
        + "game between the USC Trojans and the CUI Golden Eagles."
    )
    df = get_volleyball_raw_pbp(1820058)
    print(df)

    # Get the play-by-play data of an April 4th, 2019
    # game between the Lesly Lynx and the Pine Manor Gators (D3).
    print(
        "Get the play-by-play data of an April 4th, 2019 "
        + "game between the Lesly Lynx and the Pine Manor Gators (D3)."
    )
    df = get_volleyball_raw_pbp(1723131)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a play-by-play (PBP) data in a given game.

    """
    load_from_cache = True
    # is_overtime = False

    sport_id = ""
    season = 0
    away_score = 0
    home_score = 0

    home_sets_won = 0
    away_sets_won = 0

    home_set_1_score = 0
    away_set_1_score = 0

    home_set_2_score = 0
    away_set_2_score = 0

    home_set_3_score = 0
    away_set_3_score = 0

    home_set_4_score = 0
    away_set_4_score = 0

    home_set_5_score = 0
    away_set_5_score = 0

    home_cumulative_score = 0
    away_cumulative_score = 0

    MVB_teams_df = load_volleyball_teams(get_mens_data=True)
    MVB_team_ids_arr = MVB_teams_df["team_id"].to_list()

    WVB_teams_df = load_volleyball_teams(get_mens_data=False)
    WVB_team_ids_arr = WVB_teams_df["team_id"].to_list()

    pbp_df = pd.DataFrame()
    pbp_df_arr = []
    temp_df = pd.DataFrame()

    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    stat_columns = [
        "season",
        "game_id",
        "sport_id",
        "game_datetime",
        "set_num",
        "event_num",
        "event_team",
        "event_text",
        "is_scoring_play",
        "home_set_score",
        "away_set_score",
        "is_extra_points",
        "home_cumulative_score",
        "away_cumulative_score",
        "home_sets_won",
        "away_sets_won",
        "stadium_name",
        "attendance",
        "away_team_id",
        "away_team_name",
        "home_team_id",
        "home_team_name",
        "home_set_1_score",
        "away_set_1_score",
        "home_set_2_score",
        "away_set_2_score",
        "home_set_3_score",
        "away_set_3_score",
        "home_set_4_score",
        "away_set_4_score",
        "home_set_5_score",
        "away_set_5_score",
    ]

    url = f"https://stats.ncaa.org/contests/{game_id}/play_by_play"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/raw_pbp/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/raw_pbp/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_MVB/raw_pbp/"
        + f"{game_id}_raw_pbp.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_MVB/raw_pbp/"
            + f"{game_id}_raw_pbp.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/volleyball_MVB/raw_pbp/"
                + f"{game_id}_raw_pbp.csv"
            )
        )
        load_from_cache = True
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/raw_pbp/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/raw_pbp/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_WVB/raw_pbp/"
        + f"{game_id}_raw_pbp.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_WVB/raw_pbp/"
            + f"{game_id}_raw_pbp.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/volleyball_WVB/raw_pbp/"
                + f"{game_id}_raw_pbp.csv"
            )
        )
        load_from_cache = True
    else:
        logging.info("Could not find a WVB player game stats file")

    now = datetime.today()

    age = now - file_mod_datetime

    if age.days >= 35:
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")

    info_table = soup.find(
        "td",
        {
            "style": "padding: 0px 30px 0px 30px",
            "class": "d-none d-md-table-cell"
        }
    ).find(
        "table",
        {"style": "border-collapse: collapse"}
    )

    info_table_rows = info_table.find_all("tr")

    game_date_str = info_table_rows[3].find("td").text
    if "TBA" in game_date_str:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y TBA')
    elif "tba" in game_date_str:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y tba')
    elif "TBD" in game_date_str:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y TBD')
    elif "tbd" in game_date_str:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y tbd')
    elif (
        "tbd" not in game_date_str.lower() and
        ":" not in game_date_str.lower()
    ):
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y')
    else:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y %I:%M %p')
    game_datetime = game_datetime.astimezone(timezone("US/Eastern"))
    game_date_str = game_datetime.isoformat()
    # del game_datetime

    stadium_str = info_table_rows[4].find("td").text

    attendance_str = info_table_rows[5].find("td").text
    attendance_int = re.findall(
        r"([0-9\,]+)",
        attendance_str
    )[0]
    attendance_int = attendance_int.replace(",", "")
    attendance_int = int(attendance_int)

    del attendance_str
    team_cards = soup.find_all(
        "td",
        {
            "valign": "center",
            "class": "grey_text d-none d-sm-table-cell"
        }
    )

    away_url = team_cards[0].find_all("a")
    away_url = away_url[0]
    home_url = team_cards[1].find_all("a")
    home_url = home_url[0]

    away_team_name = away_url.text
    home_team_name = home_url.text

    away_team_id = away_url.get("href")
    home_team_id = home_url.get("href")

    away_team_id = away_team_id.replace("/teams", "")
    away_team_id = away_team_id.replace("/team", "")
    away_team_id = away_team_id.replace("/", "")
    away_team_id = int(away_team_id)

    home_team_id = home_team_id.replace("/teams", "")
    home_team_id = home_team_id.replace("/team", "")
    home_team_id = home_team_id.replace("/", "")
    home_team_id = int(home_team_id)

    if home_team_id in MVB_team_ids_arr:
        sport_id = "MVB"
        temp_df = MVB_teams_df[MVB_teams_df["team_id"] == home_team_id]
        season = temp_df["season"].iloc[0]
        del temp_df
    elif home_team_id in WVB_team_ids_arr:
        sport_id = "WVB"
        temp_df = WVB_teams_df[WVB_teams_df["team_id"] == home_team_id]
        season = temp_df["season"].iloc[0]
        del temp_df
    # This should never be the case,
    # but if something goes very horribly wrong,
    # double check the away team ID to
    # the MVB and WVB team ID list.
    elif away_team_id in MVB_team_ids_arr:
        sport_id = "MVB"
        temp_df = MVB_teams_df[MVB_teams_df["team_id"] == away_team_id]
        season = temp_df["season"].iloc[0]
        del temp_df
    elif away_team_id in WVB_team_ids_arr:
        sport_id = "WVB"
        temp_df = WVB_teams_df[WVB_teams_df["team_id"] == home_team_id]
        season = temp_df["season"].iloc[0]
        del temp_df
    # If we get to this, we are in a code red situation.
    # "SHUT IT DOWN" - Gordon Ramsay
    else:
        raise ValueError(
            "Could not identify if this is a " +
            "MVB or WVB game based on team IDs. "
        )

    section_cards = soup.find_all(
        "div",
        {"class": "row justify-content-md-center w-100"}
    )

    if len(section_cards) == 0:
        logging.warning(
            f"Could not find any plays for game ID `{game_id}`. " +
            "Returning empty DataFrame."
        )
        df = pd.DataFrame(columns=stat_columns)
        return df

    # play_id = 0
    for card in section_cards:
        is_extra_points = False
        event_text = ""

        set_num_str = card.find(
            "div",
            {"class": "card-header"}
        ).text
        set_num = re.findall(
            r"([0-9]+)",
            set_num_str
        )

        set_num = int(set_num[0])

        table_body = card.find("table").find("tbody").find_all("tr")

        # pbp rows
        for row in table_body:
            is_scoring_play = True
            t_cells = row.find_all("td")
            t_cells = [x.text.strip() for x in t_cells]
            game_time_str = t_cells[0]

            if len(t_cells[0]) > 0:
                event_team = away_team_id
                event_text = t_cells[0]
            elif len(t_cells[2]) > 0:
                event_team = home_team_id
                event_text = t_cells[2]

            if "+" in event_text:
                temp = event_text.split("\n")
                if len(temp) >= 2:
                    event_text = temp[1]
                else:
                    raise Exception(
                        "Unhandled situation " +
                        f"when parsing a scoring play: `{temp}`"
                    )
                # print()
            else:
                event_text = event_text.replace("\n", "")

            event_text = event_text.replace("  ", " ")
            event_text = event_text.strip()

            if len(t_cells) == 3:
                try:
                    away_score, home_score = t_cells[1].split("-")

                    away_score = int(away_score)
                    home_score = int(home_score)
                    is_scoring_play = True
                except ValueError:
                    logging.info(
                        "Could not extract a score " +
                        f"from the following play `{event_text}`"
                    )
                    is_scoring_play = False
                except Exception as e:
                    logging.warning(
                        f"An unhandled exception has occurred: `{e}`"
                    )
                    raise e
                    # scoring_play = False
            elif len(t_cells) > 3:
                raise SyntaxError(
                    f"Unhandled PBP row format in game ID `{game_id}`"
                )

            if set_num <= 4 and home_score == 24 and away_score == 24:
                is_extra_points = True
            elif set_num == 5 and home_score == 14 and away_score == 14:
                is_extra_points = True

            temp_home_cumulative_score = home_cumulative_score + home_score
            temp_away_cumulative_score = away_cumulative_score + away_score

            temp_df = pd.DataFrame(
                {
                    # "season": season,
                    # "game_id": game_id,
                    # "sport_id": sport_id,
                    # "away_team_id": away_team_id,
                    # "away_team_name": away_team_name,
                    # "home_team_id": home_team_id,
                    # "home_team_name": home_team_name,
                    "game_time_str": game_time_str,
                    "set_num": set_num,
                    "away_set_score": away_score,
                    "home_set_score": home_score,
                    "event_team": event_team,
                    "event_text": event_text,
                    "is_scoring_play": is_scoring_play,
                    "is_extra_points": is_extra_points,
                    "home_cumulative_score": temp_home_cumulative_score,
                    "away_cumulative_score": temp_away_cumulative_score,
                    "home_sets_won": home_sets_won,
                    "away_sets_won": away_sets_won,
                },
                index=[0],
            )
            pbp_df_arr.append(temp_df)

        if set_num == 1:
            home_set_1_score = home_score
            away_set_1_score = away_score
            home_cumulative_score = home_set_1_score
            away_cumulative_score = away_set_1_score
        elif set_num == 2:
            home_set_2_score = home_score
            away_set_2_score = away_score
            home_cumulative_score += home_set_2_score
            away_cumulative_score += away_set_2_score
        elif set_num == 3:
            home_set_3_score = home_score
            away_set_3_score = away_score
            home_cumulative_score += home_set_3_score
            away_cumulative_score += away_set_3_score
        elif set_num == 4:
            home_set_4_score = home_score
            away_set_4_score = away_score
            home_cumulative_score += home_set_4_score
            away_cumulative_score += away_set_4_score
        elif set_num == 5:
            home_set_5_score = home_score
            away_set_5_score = away_score
            home_cumulative_score += home_set_4_score
            away_cumulative_score += away_set_4_score

        if temp_away_cumulative_score > home_cumulative_score:
            away_sets_won += 1
        elif temp_away_cumulative_score < home_cumulative_score:
            home_sets_won += 1

        # End of set play
        temp_df = pd.DataFrame(
            {
                # "season": season,
                # "game_id": game_id,
                # "sport_id": sport_id,
                # "away_team_id": away_team_id,
                # "away_team_name": away_team_name,
                # "home_team_id": home_team_id,
                # "home_team_name": home_team_name,
                "game_time_str": game_time_str,
                "set_num": set_num,
                "away_set_score": away_score,
                "home_set_score": home_score,
                "event_team": event_team,
                "event_text": f"END SET {set_num}",
                "is_scoring_play": is_scoring_play,
                "is_extra_points": is_extra_points,
                "home_cumulative_score": temp_home_cumulative_score,
                "away_cumulative_score": temp_away_cumulative_score,
                "home_sets_won": home_sets_won,
                "away_sets_won": away_sets_won,
            },
            index=[0],
        )
        pbp_df_arr.append(temp_df)

    # End of game play
    temp_df = pd.DataFrame(
        {
            # "season": season,
            # "game_id": game_id,
            # "sport_id": sport_id,
            # "away_team_id": away_team_id,
            # "away_team_name": away_team_name,
            # "home_team_id": home_team_id,
            # "home_team_name": home_team_name,
            "game_time_str": game_time_str,
            "set_num": set_num,
            "away_set_score": away_score,
            "home_set_score": home_score,
            "event_team": event_team,
            "event_text": "END MATCH",
            "is_scoring_play": is_scoring_play,
            "is_extra_points": is_extra_points,
            "home_cumulative_score": temp_home_cumulative_score,
            "away_cumulative_score": temp_away_cumulative_score,
            "home_sets_won": home_sets_won,
            "away_sets_won": away_sets_won,
        },
        index=[0],
    )
    pbp_df_arr.append(temp_df)
    pbp_df = pd.concat(pbp_df_arr, ignore_index=True)
    pbp_df["event_num"] = pbp_df.index + 1
    pbp_df["game_datetime"] = game_date_str
    pbp_df["season"] = season
    pbp_df["game_id"] = game_id
    pbp_df["sport_id"] = sport_id
    pbp_df["stadium_name"] = stadium_str
    pbp_df["attendance"] = attendance_int
    pbp_df["away_team_id"] = away_team_id
    pbp_df["away_team_name"] = away_team_name
    pbp_df["home_team_id"] = home_team_id
    pbp_df["home_team_name"] = home_team_name

    pbp_df["home_set_1_score"] = home_set_1_score
    pbp_df["away_set_1_score"] = away_set_1_score

    pbp_df["home_set_2_score"] = home_set_2_score
    pbp_df["away_set_2_score"] = away_set_2_score

    pbp_df["home_set_3_score"] = home_set_3_score
    pbp_df["away_set_3_score"] = away_set_3_score

    pbp_df["home_set_4_score"] = home_set_4_score
    pbp_df["away_set_4_score"] = away_set_4_score

    pbp_df["home_set_5_score"] = home_set_5_score
    pbp_df["away_set_5_score"] = away_set_5_score

    # print(pbp_df.columns)
    pbp_df = pbp_df.reindex(columns=stat_columns)
    pbp_df = pbp_df.infer_objects()

    if sport_id == "MVB":
        pbp_df.to_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_MVB/raw_pbp/"
            + f"{game_id}_raw_pbp.csv",
            index=False
        )
    elif sport_id == "WVB":
        pbp_df.to_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_WVB/raw_pbp/"
            + f"{game_id}_raw_pbp.csv",
            index=False
        )
    else:
        raise ValueError(
            f"Improper Sport ID: `{sport_id}`"
        )

    return pbp_df


def get_parsed_volleyball_pbp(game_id: int) -> pd.DataFrame:
    """
    Given a valid game ID,
    this function will attempt to parse play-by-play (PBP)
    data for that game.

    Parameters
    ----------
    `game_id` (int, mandatory):
        Required argument.
        Specifies the game you want play-by-play data (PBP) from.

    Usage
    ----------
    ```python
    ```

    Returns
    ----------
    A pandas `DataFrame` object with a play-by-play (PBP) data in a given game.

    """
    home_team_id = 0
    away_team_id = 0
    sport_id = ""

    home_roster_df = pd.DataFrame()
    away_roster_df = pd.DataFrame()

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/parsed_pbp/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_MVB/parsed_pbp/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_MVB/parsed_pbp/"
        + f"{game_id}_parsed_pbp.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_MVB/parsed_pbp/"
            + f"{game_id}_parsed_pbp.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/volleyball_MVB/parsed_pbp/"
                + f"{game_id}_parsed_pbp.csv"
            )
        )
        load_from_cache = True
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/")

    if exists(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/parsed_pbp/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/volleyball_WVB/parsed_pbp/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/volleyball_WVB/parsed_pbp/"
        + f"{game_id}_parsed_pbp.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/volleyball_WVB/parsed_pbp/"
            + f"{game_id}_parsed_pbp.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/volleyball_WVB/parsed_pbp/"
                + f"{game_id}_parsed_pbp.csv"
            )
        )
        load_from_cache = True
    else:
        logging.info("Could not find a WVB player game stats file")

    now = datetime.today()

    age = now - file_mod_datetime

    if age.days > 1:
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    raw_df = get_volleyball_raw_pbp(game_id=game_id)

    sport_id = raw_df["sport_id"].iloc[0]
    home_team_id = raw_df["home_team_id"].iloc[0]
    away_team_id = raw_df["away_team_id"].iloc[0]

    pbp_df = _volleyball_pbp_helper(raw_df=raw_df)

    home_roster_df = get_volleyball_team_roster(team_id=home_team_id)
    home_roster_df["Name"] = home_roster_df["Name"].str.lower()

    away_roster_df = get_volleyball_team_roster(team_id=away_team_id)
    away_roster_df["Name"] = away_roster_df["Name"].str.lower()

    home_players_arr = dict(
        zip(
            home_roster_df["Name"], home_roster_df["player_id"]
        )
    )
    away_players_arr = dict(
        zip(
            away_roster_df["Name"], away_roster_df["player_id"]
        )
    )
    players_arr = home_players_arr | away_players_arr
    name_cols = [
        "substitution_player_1_name",
        "substitution_player_2_name",
        "substitution_player_3_name",
        "substitution_player_4_name",
        "serve_player_name",
        "reception_player_name",
        "set_player_name",
        "set_error_player_name",
        "attack_player_name",
        "dig_player_name",
        "kill_player_name",
        "block_player_1_name",
        "block_player_2_name",
        "ball_handling_error_player_name",
        "dig_error_player_name",
    ]
    id_cols = [
        "substitution_player_1_id",
        "substitution_player_2_id",
        "substitution_player_3_id",
        "substitution_player_4_id",
        "serve_player_id",
        "reception_player_id",
        "set_player_id",
        "set_error_player_id",
        "attack_player_id",
        "dig_player_id",
        "kill_player_id",
        "block_player_1_id",
        "block_player_2_id",
        "ball_handling_error_player_id",
        "dig_error_player_id",
    ]

    for i in range(0, len(id_cols)):
        name_column = name_cols[i]
        id_column = id_cols[i]
        pbp_df[name_column] = pbp_df[name_column].str.replace("3a", "")
        pbp_df[name_column] = pbp_df[name_column].str.replace(".", "")
        pbp_df[id_column] = pbp_df[name_column].str.lower()
        pbp_df.loc[pbp_df[id_column].notnull(), id_column] = pbp_df[
            id_column
        ].map(_name_smother)
        pbp_df[id_column] = pbp_df[id_column].map(players_arr)

    pbp_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/volleyball_{sport_id}/parsed_pbp/"
        + f"{game_id}_parsed_pbp.csv",
        index=False
    )
    return pbp_df
