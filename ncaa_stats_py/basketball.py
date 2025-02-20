# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: `basketball.py`
# Purpose: Houses functions that allows one to access NCAA basketball data
# Creation Date: 2024-09-20 08:15 PM EDT
# Update History:
# - 2024-09-20 08:15 PM EDT
# - 2024-11-01 12:10 AM EDT
# - 2024-11-25 07:45 PM EDT
# - 2025-01-04 03:00 PM EDT
# - 2025-01-18 02:40 PM EDT
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

from ncaa_stats_py.utls import (
    _format_folder_str,
    _get_minute_formatted_time_from_seconds,
    _get_schools,
    _get_webpage,
)


def get_basketball_teams(
    season: int,
    level: str | int,
    get_wbb_data: bool = False
) -> pd.DataFrame:
    """
    Retrieves a list of basketball teams from the NCAA.

    Parameters
    ----------
    `season` (int, mandatory):
        Required argument.
        Specifies the season you want NCAA basketball team information from.

    `level` (int, mandatory):
        Required argument.
        Specifies the level/division you want
        NCAA basketball team information from.
        This can either be an integer (1-3) or a string ("I"-"III").

    `get_wbb_data` (bool, optional):
        Optional argument.
        If you want women's basketball data instead of men's basketball data,
        set this to `True`.

    Usage
    ----------
    ```python

    from ncaa_stats_py.basketball import get_basketball_teams

    ########################################
    #          Men's Basketball            #
    ########################################

    # Get all D1 men's basketball teams for the 2024 season.
    print("Get all D1 men's basketball teams for the 2024 season.")
    df = get_basketball_teams(2024, 1)
    print(df)

    # Get all D2 men's basketball teams for the 2023 season.
    print("Get all D2 men's basketball teams for the 2023 season.")
    df = get_basketball_teams(2023, 2)
    print(df)

    # Get all D3 men's basketball teams for the 2022 season.
    print("Get all D3 men's basketball teams for the 2022 season.")
    df = get_basketball_teams(2022, 3)
    print(df)

    # Get all D1 men's basketball teams for the 2021 season.
    print("Get all D1 men's basketball teams for the 2021 season.")
    df = get_basketball_teams(2021, "I")
    print(df)

    # Get all D2 men's basketball teams for the 2020 season.
    print("Get all D2 men's basketball teams for the 2020 season.")
    df = get_basketball_teams(2020, "II")
    print(df)

    # Get all D3 men's basketball teams for the 2019 season.
    print("Get all D3 men's basketball teams for the 2019 season.")
    df = get_basketball_teams(2019, "III")
    print(df)

    ########################################
    #          Women's Basketball          #
    ########################################

    # Get all D1 women's basketball teams for the 2024 season.
    print(
        "Get all D1 women's basketball teams for the 2024 season."
    )
    df = get_basketball_teams(2024, 1)
    print(df)

    # Get all D2 women's basketball teams for the 2023 season.
    print(
        "Get all D2 women's basketball teams for the 2023 season."
    )
    df = get_basketball_teams(2023, 2)
    print(df)

    # Get all D3 women's basketball teams for the 2022 season.
    print(
        "Get all D3 women's basketball teams for the 2022 season."
    )
    df = get_basketball_teams(2022, 3)
    print(df)

    # Get all D1 women's basketball teams for the 2021 season.
    print(
        "Get all D1 women's basketball teams for the 2021 season."
    )
    df = get_basketball_teams(2021, "I")
    print(df)

    # Get all D2 women's basketball teams for the 2020 season.
    print(
        "Get all D2 women's basketball teams for the 2020 season."
    )
    df = get_basketball_teams(2020, "II")
    print(df)

    # Get all D3 women's basketball teams for the 2019 season.
    print(
        "Get all D3 women's basketball teams for the 2019 season."
    )
    df = get_basketball_teams(2019, "III")
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a list of college basketball teams
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

    if get_wbb_data is True:
        sport_id = "WBB"
        stat_sequence = 169
    else:
        sport_id = "MBB"
        stat_sequence = 168

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

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/teams/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}//teams/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/teams/"
        + f"{season}_{formatted_level}_teams.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/teams/"
            + f"{season}_{formatted_level}_teams.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/teams/"
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
    url = (
        "https://stats.ncaa.org/rankings/change_sport_year_div?"
        + f"academic_year={season}.0&division={ncaa_level}.0" +
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
            if "final " in rp.text.lower():
                rp_value = rp.get("value")
                found_value = True
                break
            else:
                rp_value = rp.get("value")
                found_value = True
                break

    url = (
        "https://stats.ncaa.org/rankings/institution_trends?"
        + f"academic_year={season}.0&division={ncaa_level}.0&"
        + f"ranking_period={rp_value}&sport_code={sport_id}"
        + f"&sport_code={sport_id}"
    )

    best_method = True
    if (
        (season < 2015 and sport_id == "MBB")
    ):
        url = (
            "https://stats.ncaa.org/rankings/national_ranking?"
            + f"academic_year={season}.0&division={ncaa_level}.0&"
            + f"ranking_period={rp_value}&sport_code={sport_id}"
            + f"&stat_seq={stat_sequence}"
        )
        response = _get_webpage(url=url)
        best_method = False
    elif season < 2013:
        url = (
            "https://stats.ncaa.org/rankings/national_ranking?"
            + f"academic_year={season}.0&division={ncaa_level}.0&"
            + f"ranking_period={rp_value}&sport_code={sport_id}"
            + f"&stat_seq={stat_sequence}"
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
                + f"academic_year={season}.0&division={ncaa_level}.0&"
                + f"ranking_period={rp_value}&sport_code={sport_id}"
                + f"&stat_seq={stat_sequence}"
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
        f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/teams/"
        + f"{season}_{formatted_level}_teams.csv",
        index=False,
    )

    return teams_df


def load_basketball_teams(
    start_year: int = 2011,
    get_wbb_data: bool = False
) -> pd.DataFrame:
    """
    Compiles a list of known NCAA basketball teams in NCAA basketball history.

    Parameters
    ----------
    `start_year` (int, optional):
        Optional argument.
        Specifies the first season you want
        NCAA basketball team information from.

    `get_wbb_data` (bool, optional):
        Optional argument.
        If you want women's basketball data instead of men's basketball data,
        set this to `True`.

    Usage
    ----------
    ```python

    from ncaa_stats_py.basketball import load_basketball_teams

    # WARNING: Running this script "as-is" for the first time may
    #          take some time.
    #          The *N*th time you run this script will be faster.

    # Load in every women's basketball team
    # from 2011 to present day.
    print(
        "Load in every women's basketball team " +
        "from 2011 to present day."
    )
    df = load_basketball_teams(get_wbb_data=True)
    print(df)

    # Load in every men's basketball team
    # from 2011 to present day.
    print(
        "Load in every men's basketball team " +
        "from 2011 to present day."
    )
    df = load_basketball_teams()
    print(df)

    # Load in every men's basketball team
    # from 2020 to present day.
    print(
        "Load in every men's basketball team " +
        "from 2020 to present day."
    )
    df = load_basketball_teams(start_year=2020)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a list of
    all known college basketball teams.

    """
    # start_year = 2008

    # if get_wbb_data is True:
    #     sport_id = "WBB"
    # else:
    #     sport_id = "MBB"

    teams_df = pd.DataFrame()
    teams_df_arr = []
    temp_df = pd.DataFrame()

    now = datetime.now()
    ncaa_divisions = ["I", "II", "III"]
    if now.month > 5:
        ncaa_seasons = [x for x in range(start_year, (now.year + 2))]
    else:
        ncaa_seasons = [x for x in range(start_year, (now.year + 1))]

    logging.info(
        "Loading in all NCAA basketball teams. "
        + "If this is the first time you're seeing this message, "
        + "it may take some time (3-10 minutes) for this to load."
    )
    for s in ncaa_seasons:
        logging.info(f"Loading in basketball teams for the {s} season.")
        for d in ncaa_divisions:
            try:
                temp_df = get_basketball_teams(season=s, level=d)
                teams_df_arr.append(temp_df)
                del temp_df
            except Exception as e:
                logging.warning(
                    "Unhandled exception when trying to " +
                    f"get the teams. Full exception: `{e}`"
                )


    teams_df = pd.concat(teams_df_arr, ignore_index=True)
    teams_df = teams_df.infer_objects()
    return teams_df


def get_basketball_team_schedule(team_id: int) -> pd.DataFrame:
    """
    Retrieves a team schedule, from a valid NCAA basketball team ID.

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

    from ncaa_stats_py.basketball import get_basketball_team_schedule

    ########################################
    #          Men's Basketball            #
    ########################################

    # Get the team schedule for the
    # 2024 Wright St. MBB team (D1, ID: 561255).
    print(
        "Get the team schedule for the " +
        "2024 Wright St. MBB team (D1, ID: 561255)."
    )
    df = get_basketball_team_schedule(561255)
    print(df)

    # Get the team schedule for the
    # 2023 Caldwell MBB team (D2, ID: 542813).
    print(
        "Get the team schedule for the " +
        "2023 Caldwell MBB team (D2, ID: 542813)."
    )
    df = get_basketball_team_schedule(542813)
    print(df)

    # Get the team schedule for the
    # 2022 SUNY Maritime MBB team (D3, ID: 528097).
    print(
        "Get the team schedule for the " +
        "2022 SUNY Maritime MBB team (D3, ID: 528097)."
    )
    df = get_basketball_team_schedule(528097)
    print(df)

    ########################################
    #          Women's Basketball          #
    ########################################

    # Get the team schedule for the
    # 2021 Wake Forest WBB team (D1, ID: 506339).
    print(
        "Get the team schedule for the " +
        "2021 Wake Forest WBB team (D1, ID: 506339)."
    )
    df = get_basketball_team_schedule(506339)
    print(df)

    # Get the team schedule for the
    # 2020 Trevecca Nazarene WBB team (D2, ID: 484527).
    print(
        "Get the team schedule for the " +
        "2020 Trevecca Nazarene WBB team (D2, ID: 484527)."
    )
    df = get_basketball_team_schedule(484527)
    print(df)

    # Get the team schedule for the
    # 2019 Simpson WBB team (D3, ID: 452452).
    print(
        "Get the team schedule for the " +
        "2019 Simpson WBB team (D3, ID: 452452)."
    )
    df = get_basketball_team_schedule(452452)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with an NCAA basketball team's schedule.

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
        team_df = load_basketball_teams()
        team_df = team_df[team_df["team_id"] == team_id]
        season = team_df["season"].iloc[0]
        ncaa_division = team_df["ncaa_division"].iloc[0]
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        sport_id = "MBB"
    except Exception:
        team_df = load_basketball_teams(get_wbb_data=True)
        team_df = team_df[team_df["team_id"] == team_id]
        season = team_df["season"].iloc[0]
        ncaa_division = team_df["ncaa_division"].iloc[0]
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        sport_id = "WBB"
    # team_conference_name = team_df["team_conference_name"].iloc[0]
    # school_name = team_df["school_name"].iloc[0]
    # school_id = int(team_df["school_id"].iloc[0])

    del team_df

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/team_schedule/"
    ):
        pass
    else:
        mkdir(
            f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/team_schedule/"
        )

    if exists(
        f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/team_schedule/"
        + f"{team_id}_team_schedule.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/team_schedule/"
            + f"{team_id}_team_schedule.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/"
                + f"basketball_{sport_id}/team_schedule/"
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
    # For NCAA basketball, the season always starts in the fall semester,
    # and ends in the spring semester.
    # Thus, if `season_name` = "2011-12", this is the "2012" basketball season,
    # because 2012 would encompass the fall and spring semesters
    # for NCAA member institutions.
    # season = f"{season_name[0:2]}{season_name[-2:]}"
    # season = int(season)
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

        game_date = datetime.strptime(game_date, "%m/%d/%Y").date()

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
                        "home_team_score": score_1,
                        "away_team_score": score_2,
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
                            "home_team_score": score_1,
                            "away_team_score": score_2,
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
                            "home_team_score": score_2,
                            "away_team_score": score_1,
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
                        "home_team_score": score_2,
                        "away_team_score": score_1,
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
        + f"basketball_{sport_id}/team_schedule/"
        + f"{team_id}_team_schedule.csv",
        index=False,
    )

    return games_df


def get_basketball_day_schedule(
    game_date: str | date | datetime,
    level: str | int = "I",
    get_wbb_data: bool = False
):
    """
    Given a date and NCAA level, this function retrieves basketball every game
    for that date.

    Parameters
    ----------
    `game_date` (int, mandatory):
        Required argument.
        Specifies the date you want a basketball schedule from.
        For best results, pass a string formatted as "YYYY-MM-DD".

    `level` (int, mandatory):
        Required argument.
        Specifies the level/division you want a
        NCAA basketball schedule from.
        This can either be an integer (1-3) or a string ("I"-"III").

    `get_wbb_data` (bool, optional):
        Optional argument.
        If you want women's basketball data instead of men's basketball data,
        set this to `True`.

    Usage
    ----------
    ```python

    from ncaa_stats_py.basketball import get_basketball_day_schedule


    # Get all DI games that will be played on April 22th, 2025.
    print("Get all games that will be played on April 22th, 2025.")
    df = get_basketball_day_schedule("2025-04-22", level=1)
    print(df)

    # Get all division II games that were played on February 14th, 2025.
    print("Get all division II games that were played on February 14th, 2025.")
    df = get_basketball_day_schedule("2025-02-14", level="I")
    print(df)

    # Get all DI games that were played on December 10th, 2024.
    print("Get all games that were played on December 10th, 2024.")
    df = get_basketball_day_schedule("2024-12-10", level="I")
    print(df)

    # Get all DI games (if any) that were played on December 12th, 2024.
    print("Get all DI games (if any) that were played on December 12th, 2024.")
    df = get_basketball_day_schedule("2024-12-12")
    print(df)

    # Get all DII games played on January 14th, 2024.
    print("Get all DI games played on January 14th, 2024.")
    df = get_basketball_day_schedule("2024-01-14")
    print(df)

    # Get all division III games played on December 16th, 2023.
    print("Get all division III games played on December 16th, 2023.")
    df = get_basketball_day_schedule("2023-12-16")
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with all basketball games played on that day,
    for that NCAA division/level.

    """

    season = 0
    sport_id = "MBB"

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

    if get_wbb_data is True:
        sport_id = "WBB"
    elif get_wbb_data is False:
        sport_id = "MBB"
    else:
        raise ValueError(
            f"Unhandled value for `get_wbb_data`: `{get_wbb_data}`"
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

        away_points_scored = td_arr[-1].text
        away_points_scored = away_points_scored.replace("\n", "")
        away_points_scored = away_points_scored.replace("\xa0", "")
        if len(away_points_scored) > 0:
            away_points_scored = int(away_points_scored)
        else:
            away_points_scored = 0

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

        home_points_scored = td_arr[-1].text
        home_points_scored = home_points_scored.replace("\n", "")
        home_points_scored = home_points_scored.replace("\xa0", "")
        if len(home_points_scored) > 0:
            home_points_scored = int(home_points_scored)
        else:
            home_points_scored = 0

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
                "home_points_scored": home_points_scored,
                "away_points_scored": away_points_scored,
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


def get_full_basketball_schedule(
    season: int,
    level: str | int = "I",
    get_wbb_data: bool = False
) -> pd.DataFrame:
    """
    Retrieves a full basketball schedule,
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

    `get_wbb_data` (bool, optional):
        Optional argument.
        If you want women's basketball data instead of men's basketball data,
        set this to `True`.

    Usage
    ----------
    ```python

    from ncaa_stats_py.basketball import get_full_basketball_schedule

    # Get the entire 2024 schedule for the 2024 D1 basketball season.
    print("Get the entire 2024 schedule for the 2024 D1 basketball season.")
    df = get_full_basketball_schedule(season=2024, level="I")
    print(df)

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
    df = get_full_basketball_schedule(season=2024, level=1)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with an NCAA basketball
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

    if get_wbb_data is True:
        sport_id = "WBB"
    else:
        sport_id = "MBB"

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

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/" +
        f"basketball_{sport_id}/full_schedule/"
    ):
        pass
    else:
        mkdir(
            f"{home_dir}/.ncaa_stats_py/" +
            f"basketball_{sport_id}/full_schedule/"
        )

    if exists(
        f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/full_schedule/"
        + f"{season}_{formatted_level}_full_schedule.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/full_schedule/"
            + f"{season}_{formatted_level}_full_schedule.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/" +
                f"basketball_{sport_id}/full_schedule/"
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

    teams_df = load_basketball_teams()
    teams_df = teams_df[
        (teams_df["season"] == season) &
        (teams_df["ncaa_division"] == ncaa_level)
    ]
    team_ids_arr = teams_df["team_id"].to_numpy()

    for team_id in tqdm(team_ids_arr):
        temp_df = get_basketball_team_schedule(team_id=team_id)
        schedule_df_arr.append(temp_df)

    schedule_df = pd.concat(schedule_df_arr, ignore_index=True)
    schedule_df = schedule_df.drop_duplicates(subset="game_id", keep="first")
    schedule_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/"
        + f"basketball_{sport_id}/full_schedule/"
        + f"{season}_{formatted_level}_full_schedule.csv",
        index=False,
    )
    return schedule_df


def get_basketball_team_roster(team_id: int) -> pd.DataFrame:
    """
    Retrieves a basketball team's roster from a given team ID.

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

    from ncaa_stats_py.basketball import get_basketball_team_roster

    ########################################
    #          Men's Basketball            #
    ########################################

    # Get the basketball roster for the
    # 2024 Alabama St. MBB team (D1, ID: 560655).
    print(
        "Get the basketball roster for the " +
        "2024 Alabama St. MBB team (D1, ID: 560655)."
    )
    df = get_basketball_team_roster(560655)
    print(df)

    # Get the basketball roster for the
    # 2023 Roberts Wesleyan MBB team (D2, ID: 542994).
    print(
        "Get the basketball roster for the " +
        "2023 Roberts Wesleyan MBB team (D2, ID: 542994)."
    )
    df = get_basketball_team_roster(542994)
    print(df)

    # Get the basketball roster for the
    # 2022 Pacific Lutheran MBB team (D3, ID: 528255).
    print(
        "Get the basketball roster for the " +
        "2022 Pacific Lutheran MBB team (D3, ID: 528255)."
    )
    df = get_basketball_team_roster(528255)
    print(df)

    ########################################
    #          Women's Basketball          #
    ########################################

    # Get the basketball roster for the
    # 2021 Michigan St. WBB team (D1, ID: 506069).
    print(
        "Get the basketball roster for the " +
        "2021 Michigan St. WBB team (D1, ID: 506069)."
    )
    df = get_basketball_team_roster(506069)
    print(df)

    # Get the basketball roster for the
    # 2020 Shippensburg WBB team (D2, ID: 484864).
    print(
        "Get the basketball roster for the " +
        "2020 Shippensburg WBB team (D2, ID: 484864)."
    )
    df = get_basketball_team_roster(484864)
    print(df)

    # Get the basketball roster for the
    # 2019 Maranatha Baptist team (D3, ID: 452546).
    print(
        "Get the basketball roster for the " +
        "2019 Maranatha Baptist team (D3, ID: 452546)."
    )
    df = get_basketball_team_roster(452546)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with
    an NCAA basketball team's roster for that season.
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
        team_df = load_basketball_teams()
        team_df = team_df[team_df["team_id"] == team_id]

        season = team_df["season"].iloc[0]
        ncaa_division = team_df["ncaa_division"].iloc[0]
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        team_conference_name = team_df["team_conference_name"].iloc[0]
        school_name = team_df["school_name"].iloc[0]
        school_id = int(team_df["school_id"].iloc[0])
        sport_id = "MBB"
    except Exception:
        team_df = load_basketball_teams(get_wbb_data=True)
        team_df = team_df[team_df["team_id"] == team_id]

        season = team_df["season"].iloc[0]
        ncaa_division = team_df["ncaa_division"].iloc[0]
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        team_conference_name = team_df["team_conference_name"].iloc[0]
        school_name = team_df["school_name"].iloc[0]
        school_id = int(team_df["school_id"].iloc[0])
        school_id = int(team_df["school_id"].iloc[0])
        sport_id = "WBB"

    del team_df

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/rosters/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/rosters/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/rosters/" +
        f"{team_id}_roster.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/rosters/" +
            f"{team_id}_roster.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/rosters/" +
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
    # For NCAA basketball, the season always starts in the spring semester,
    # and ends in the fall semester.
    # Thus, if `season_name` = "2011-12", this is the "2012" basketball season,
    # because 2012 would encompass the spring and fall semesters
    # for NCAA member institutions.
    season = f"{season_name[0:2]}{season_name[-2:]}"
    season = int(season)

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
            "Hometown": "player_hometown",
            "High School": "player_high_school",
        },
        inplace=True
    )

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
        f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/rosters/" +
        f"{team_id}_roster.csv",
        index=False,
    )
    return roster_df


def get_basketball_player_season_stats(
    team_id: int,
) -> pd.DataFrame:
    """
    Given a team ID, this function retrieves and parses
    the season stats for all of the players in a given basketball team.

    Parameters
    ----------
    `team_id` (int, mandatory):
        Required argument.
        Specifies the team you want basketball stats from.
        This is separate from a school ID, which identifies the institution.
        A team ID should be unique to a school, and a season.

    Usage
    ----------
    ```python

    from ncaa_stats_py.basketball import get_basketball_player_season_stats

    ########################################
    #          Men's Basketball            #
    ########################################

    # Get the season stats for the
    # 2024 Illinois MBB team (D1, ID: 560955).
    print(
        "Get the season stats for the " +
        "2024 Illinois MBB team (D1, ID: 560955)."
    )
    df = get_basketball_player_season_stats(560955)
    print(df)

    # Get the season stats for the
    # 2023 Chico St. MBB team (D2, ID: 542605).
    print(
        "Get the season stats for the " +
        "2023 Chico St. MBB team (D2, ID: 542605)."
    )
    df = get_basketball_player_season_stats(542605)
    print(df)

    # Get the season stats for the
    # 2022 Maine Maritime MBB team (D3, ID: 528070).
    print(
        "Get the season stats for the " +
        "2022 Maine Maritime MBB team (D3, ID: 528070)."
    )
    df = get_basketball_player_season_stats(528070)
    print(df)

    ########################################
    #          Women's Basketball          #
    ########################################

    # Get the season stats for the
    # 2021 Louisville WBB team (D1, ID: 506050).
    print(
        "Get the season stats for the " +
        "2021 Louisville WBB team (D1, ID: 506050)."
    )
    df = get_basketball_player_season_stats(506050)
    print(df)

    # Get the season stats for the
    # 2020 Paine WBB team (D2, ID: 484830).
    print(
        "Get the season stats for the " +
        "2020 Paine WBB team (D2, ID: 484830)."
    )
    df = get_basketball_player_season_stats(484830)
    print(df)

    # Get the season stats for the
    # 2019 Pomona-Pitzer team (D3, ID: 452413).
    print(
        "Get the season stats for the " +
        "2019 Pomona-Pitzer team (D3, ID: 452413)."
    )
    df = get_basketball_player_season_stats(452413)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with the season batting stats for
    all players with a given NCAA basketball team.
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
        "MP_str",
        "MP_minutes",
        "MP_seconds",
        "MP_total_seconds",
        "FGM",
        "FGA",
        "FG%",
        "eFG%",
        "TSA",
        "TS%",
        "2PM",
        "2PA",
        "2FG%",
        "3PM",
        "3PA",
        "3FG%",
        "FT",
        "FTA",
        "FT%",
        "PTS",
        "ORB",
        "DRB",
        "TRB",
        "Avg",
        "AST",
        "TOV",
        "TOV%",
        "STL",
        "BLK",
        "PF",
        "DBL_DBL",
        "TRP_DBL",
        "DQ",
        "TF",
    ]

    # if get_wbb_data is True:
    #     sport_id = "WBB"
    # else:
    #     sport_id = "MBB"

    try:
        team_df = load_basketball_teams()

        team_df = team_df[team_df["team_id"] == team_id]

        season = team_df["season"].iloc[0]
        ncaa_division = int(team_df["ncaa_division"].iloc[0])
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        team_conference_name = team_df["team_conference_name"].iloc[0]
        school_name = team_df["school_name"].iloc[0]
        school_id = int(team_df["school_id"].iloc[0])
        sport_id = "MBB"
    except Exception:
        team_df = load_basketball_teams(get_wbb_data=True)

        team_df = team_df[team_df["team_id"] == team_id]

        season = team_df["season"].iloc[0]
        ncaa_division = int(team_df["ncaa_division"].iloc[0])
        ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
        team_conference_name = team_df["team_conference_name"].iloc[0]
        school_name = team_df["school_name"].iloc[0]
        school_id = int(team_df["school_id"].iloc[0])
        sport_id = "WBB"

    del team_df

    # stat_id = _get_stat_id(
    #     sport="basketball",
    #     season=season,
    #     stat_type="batting"
    # )

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    url = f"https://stats.ncaa.org/teams/{team_id}/season_to_date_stats"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/" +
        f"basketball_{sport_id}/player_season_stats/"
    ):
        pass
    else:
        mkdir(
            f"{home_dir}/.ncaa_stats_py/" +
            f"basketball_{sport_id}/player_season_stats/"
        )

    if exists(
        f"{home_dir}/.ncaa_stats_py/" +
        f"basketball_{sport_id}/player_season_stats/"
        + f"{season:00d}_{school_id:00d}_player_season_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/" +
            f"basketball_{sport_id}/player_season_stats/"
            + f"{season:00d}_{school_id:00d}_player_season_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/" +
                f"basketball_{sport_id}/player_season_stats/"
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
    # try:
    #     school_name = soup.find(
    #         "div", {"class": "card"}
    #     ).find("img").get("alt")
    # except Exception:
    #     school_name = soup.find("div", {"class": "card"}).find("a").text
    #     school_name = school_name.rsplit(" ", maxsplit=1)[0]

    season_name = (
        soup.find("select", {"id": "year_list"})
        .find("option", {"selected": "selected"})
        .text
    )
    # For NCAA basketball, the season always starts in the fall semester,
    # and ends in the spring semester.
    # Thus, if `season_name` = "2011-12", this is the "2012" basketball season,
    # because 2012 would encompass the fall and spring semesters
    # for NCAA member institutions.
    season = f"{season_name[0:2]}{season_name[-2:]}"
    season = int(season)

    # stat_categories_arr = soup.find(
    #     "ul", {"class": "nav nav-tabs padding-nav"}
    # ).find_all("a")

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

        temp_df = pd.DataFrame(
            data=[t_cells],
            columns=table_headers,
            # index=[0]
        )

        player_id = t.find("a").get("href")

        # temp_df["player_url"] = f"https://stats.ncaa.org{player_id}"
        player_id = player_id.replace("/players", "").replace("/", "")

        # stat_id = -1
        # if "year_stat_category_id" in player_id:
        #     stat_id = player_id
        #     stat_id = stat_id.rsplit("?")[-1]
        #     stat_id = stat_id.replace("?", "").replace(
        #         "year_stat_category_id=", ""
        #     )
        #     stat_id = int(stat_id)

        #     player_id = player_id.split("?")[0]

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
            "B/T": "player_bats_throws",
            "3FG": "3PM",
            "3FGA": "3PA",
            "ORebs": "ORB",
            "DRebs": "DRB",
            "Tot Reb": "TRB",
            "TO": "TOV",
            "Dbl Dbl": "DBL_DBL",
            "Trpl Dbl": "TRP_DBL",
            "Fouls": "PF",
            'Tech Fouls': "TF",
            'Effective FG Pct.': "eFG%",
            "MP": "MP_str",
            "Min": "MP_str",
            "Off Reb": "ORB",
            "Def Reb": "DRB",
            "ST": "STL",
            "BLKS": "BLK"
        },
        inplace=True,
    )
    stats_df = stats_df.infer_objects().fillna(0)
    stats_df = stats_df.astype(
        {
            "GP": "uint16",
            "GS": "uint16",
            "FGM": "uint16",
            "FGA": "uint16",
            "3PM": "uint16",
            "3PA": "uint16",
            "FT": "uint16",
            "FTA": "uint16",
            "PTS": "uint16",
            "ORB": "uint16",
            "DRB": "uint16",
            "TRB": "uint16",
            "AST": "uint16",
            "TOV": "uint16",
            "STL": "uint16",
            "BLK": "uint16",
            "PF": "uint16",
            "DBL_DBL": "uint16",
            "TRP_DBL": "uint16",
            "school_id": "uint32",
        }
    )

    # This is a separate function call because these stats
    # *don't* exist in every season.
    if "DQ" not in stats_df.columns:
        stats_df["DQ"] = None

    if "TF" not in stats_df.columns:
        stats_df["TF"] = None

    stats_df = stats_df.astype(
        {
            "DQ": "uint16",
            "TF": "uint16",
        },
        errors="ignore"
    )

    stats_df[["MP_minutes", "MP_seconds"]] = stats_df["MP_str"].str.split(
        ":", expand=True
    )
    stats_df[["MP_minutes", "MP_seconds"]] = stats_df[[
        "MP_minutes", "MP_seconds"
    ]].astype("uint64")
    stats_df["MP_total_seconds"] = (
        stats_df["MP_seconds"] + (stats_df["MP_minutes"] * 60)
    )

    stats_df["FG%"] = (stats_df["FGM"] / stats_df["FGA"])
    stats_df["FG%"] = stats_df["FG%"].round(4)

    stats_df["3P%"] = (stats_df["3PM"] / stats_df["3PA"])
    stats_df["3P%"] = stats_df["3P%"].round(4)

    stats_df["FT%"] = (stats_df["FT"] / stats_df["FTA"])
    stats_df["FT%"] = stats_df["FT%"].round(4)

    stats_df["2PM"] = (stats_df["FGM"] - stats_df["3PM"])
    stats_df["2PA"] = (stats_df["FGA"] - stats_df["3PA"])
    stats_df["2P%"] = (stats_df["2PM"] / stats_df["2PA"])
    stats_df["2P%"] = stats_df["2P%"].round(4)

    stats_df["eFG%"] = (
        (
            stats_df["FGM"] +
            (stats_df["3PM"] * 0.5)
        ) /
        stats_df["FGA"]
    )
    stats_df["eFG%"] = stats_df["eFG%"].round(4)

    stats_df["TSA"] = (
        stats_df["FGA"] + (stats_df["FTA"] * 0.44)
    )
    stats_df["TS%"] = stats_df["PTS"] / (2 * stats_df["TSA"])
    stats_df["TS%"] = stats_df["TS%"].round(4)

    stats_df["TOV%"] = (
        stats_df["TOV"] /
        (
            stats_df["FGA"] +
            (stats_df["FTA"] * 0.44) +
            stats_df["TOV"]
        )
    )
    stats_df["TOV%"] = stats_df["TOV%"].round(4)
    # In many seasons, there is an ["Avg"] column
    # that would otherwise completely screw up
    # any attempts to use the final DataFrame,
    # because it would be a duplicate column
    # that pandas wouldn't complain about
    # until it's too late.

    duplicate_cols = stats_df.columns[stats_df.columns.duplicated()]
    stats_df.drop(columns=duplicate_cols, inplace=True)
    # stats_df = stats_df.T.drop_duplicates().T
    stats_df = stats_df.reindex(columns=stat_columns)
    # print(stats_df.columns)
    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/" +
        f"basketball_{sport_id}/player_season_stats/" +
        f"{season:00d}_{school_id:00d}_player_season_stats.csv",
        index=False,
    )

    return stats_df


def get_basketball_player_game_stats(
    player_id: int,
    season: int
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

    from ncaa_stats_py.basketball import (
        get_basketball_player_game_stats
    )

    # Get the batting stats of Jacob Berry in 2022 (LSU).
    print(
        "Get the batting stats of Jacob Berry in 2022 (LSU)."
    )
    df = get_basketball_player_game_stats(player_id=7579336, season=2022)
    print(df)

    # Get the batting stats of Alec Burleson in 2019 (ECU).
    print(
        "Get the batting stats of Alec Burleson in 2019 (ECU)."
    )
    df = get_basketball_player_game_stats(player_id=6015715, season=2019)
    print(df)

    # Get the batting stats of Hunter Bishop in 2018 (Arizona St.).
    print(
        "Get the batting stats of Hunter Bishop in 2018 (Arizona St.)."
    )
    df = get_basketball_player_game_stats(player_id=6014052, season=2019)
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
        "game_id",
        "game_num",
        "player_id",
        "date",
        "opponent",
        "Result",
        "team_score",
        "opponent_score",
        "MP_str",
        "MP_minutes",
        "MP_seconds",
        "MP_total_seconds",
        "GP",
        "GS",
        "FGM",
        "FGA",
        "FG%",
        "eFG%",
        "2PM",
        "2PA",
        "2P%",
        "3PM",
        "3PA",
        "3P%",
        "FT",
        "FTA",
        "FT%",
        "ORB",
        "DRB",
        "TRB",
        "AST",
        "TOV",
        "TOV%",
        "STL",
        "BLK",
        "PF",
        "DQ",
        "TF",
        "TSA",
        "TS%",
        "PTS",
        "DBL_DBL",
        "TRP_DBL",
    ]
    load_from_cache = True
    stats_df = pd.DataFrame()
    stats_df_arr = []
    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    # stat_id = _get_stat_id(
    #     sport="basketball",
    #     season=season,
    #     stat_type="batting"
    # )
    url = f"https://stats.ncaa.org/players/{player_id}"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_MBB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_MBB/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_MBB/player_game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_MBB/player_game_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/basketball_MBB/player_game_stats/"
        + f"{season}_{player_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_MBB/player_game_stats/"
            + f"{season}_{player_id}_player_game_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/basketball_MBB/"
                + "player_game_stats/"
                + f"{season}_{player_id}_player_game_stats.csv"
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

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_WBB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_WBB/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_WBB/player_game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_WBB/player_game_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/basketball_WBB/player_game_stats/"
        + f"{season}_{player_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_WBB/player_game_stats/"
            + f"{season}_{player_id}_player_game_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/basketball_WBB/"
                + "player_game_stats/"
                + f"{season}_{player_id}_player_game_stats.csv"
            )
        )
        games_df = games_df.infer_objects()
        load_from_cache = True
    else:
        logging.info("Could not find a WBB player game stats file")

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days > 1 and
        (season - 1) >= now.year
    ):
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    # team_df = load_basketball_teams()

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
        if "MBB" in url_str.upper():
            sport_id = "MBB"
        elif "WBB" in url_str.upper():
            sport_id = "WBB"

    if sport_id is None or len(sport_id) == 0:
        # This should **never** be the case IRL,
        # but in case something weird happened and
        # we can't make a determination of if this is a
        # MBB player or a WBB player, and we somehow haven't
        # crashed by this point, set the sport ID to
        # "MBB" by default so we don't have other weirdness.
        logging.error(
            f"Could not determine if player ID {player_id} " +
            "is a MBB or a WBB player. " +
            "Because this cannot be determined, " +
            "we will make the automatic assumption that this is a MBB player."
        )
        sport_id = "MBB"

    table_data = soup.find_all(
        "table", {"class": "small_font dataTable table-bordered"}
    )[1]

    temp_table_headers = table_data.find("thead").find("tr").find_all("th")
    table_headers = [x.text for x in temp_table_headers]

    del temp_table_headers

    temp_t_rows = table_data.find("tbody")
    temp_t_rows = temp_t_rows.find_all("tr")

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

        temp_df["team_score"] = tm_score
        temp_df["opponent_score"] = opp_score

        del tm_score
        del opp_score

        g_id = t.find_all("td")[2].find("a").get("href")

        g_id = g_id.replace("/contests", "")
        g_id = g_id.replace("/box_score", "")
        g_id = g_id.replace("/", "")

        g_id = int(g_id)
        temp_df["game_id"] = g_id

        del g_id
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
    stats_df["season"] = season
    # In many seasons, there is an ["Avg"] column
    # that would otherwise completely screw up
    # any attempts to use the final DataFrame,
    # because it would be a duplicate column
    # that pandas wouldn't complain about
    # until it's too late.

    duplicate_cols = stats_df.columns[stats_df.columns.duplicated()]
    stats_df.drop(columns=duplicate_cols, inplace=True)

    stats_df.rename(
        columns={
            "#": "player_jersey_number",
            "Player": "player_full_name",
            "Yr": "player_class",
            "Pos": "player_position",
            "Ht": "player_height",
            "B/T": "player_bats_throws",
            "3FG": "3PM",
            "3FGA": "3PA",
            "ORebs": "ORB",
            "DRebs": "DRB",
            "Tot Reb": "TRB",
            "TO": "TOV",
            "Dbl Dbl": "DBL_DBL",
            "Trpl Dbl": "TRP_DBL",
            "Fouls": "PF",
            'Tech Fouls': "TF",
            'Effective FG Pct.': "eFG%",
            "MP": "MP_str",
            "Min": "MP_str",
            "Off Reb": "ORB",
            "Def Reb": "DRB",
            "ST": "STL",
            "3FG%": "3P%",
            "BLKS": "BLK"
        },
        inplace=True,
    )

    # This is a separate function call because these stats
    # *don't* exist in every season.
    if "GS" not in stats_df.columns:
        stats_df["GS"] = None

    if "DQ" not in stats_df.columns:
        stats_df["DQ"] = None

    if "TF" not in stats_df.columns:
        stats_df["TF"] = None

    if "DBL_DBL" not in stats_df.columns:
        stats_df["DBL_DBL"] = None

    if "TRP_DBL" not in stats_df.columns:
        stats_df["TRP_DBL"] = None

    stats_df = stats_df.astype(
        {
            "DQ": "uint16",
            "TF": "uint16",
        },
        errors="ignore"
    )

    stats_df = stats_df.infer_objects().fillna(0)
    stats_df = stats_df.astype(
        {
            "GP": "uint16",
            "GS": "uint16",
            "FGM": "uint16",
            "FGA": "uint16",
            "3PM": "uint16",
            "3PA": "uint16",
            "FT": "uint16",
            "FTA": "uint16",
            "PTS": "uint16",
            "ORB": "uint16",
            "DRB": "uint16",
            "TRB": "uint16",
            "AST": "uint16",
            "TOV": "uint16",
            "STL": "uint16",
            "BLK": "uint16",
            "PF": "uint16",
            "DBL_DBL": "uint16",
            "TRP_DBL": "uint16",
            # "school_id": "uint32",
        }
    )

    stats_df[["MP_minutes", "MP_seconds"]] = stats_df["MP_str"].str.split(
        ":", expand=True
    )
    stats_df[["MP_minutes", "MP_seconds"]] = stats_df[[
        "MP_minutes", "MP_seconds"
    ]].fillna(0)
    stats_df[["MP_minutes", "MP_seconds"]] = stats_df[[
        "MP_minutes", "MP_seconds"
    ]].astype("uint16")
    stats_df["MP_total_seconds"] = (
        stats_df["MP_seconds"] + (stats_df["MP_minutes"] * 60)
    )

    stats_df["FG%"] = (stats_df["FGM"] / stats_df["FGA"])
    stats_df["FG%"] = stats_df["FG%"].round(4)

    stats_df["3P%"] = (stats_df["3PM"] / stats_df["3PA"])
    stats_df["3P%"] = stats_df["3P%"].round(4)

    stats_df["FT%"] = (stats_df["FT"] / stats_df["FTA"])
    stats_df["FT%"] = stats_df["FT%"].round(4)

    stats_df["2PM"] = (stats_df["FGM"] - stats_df["3PM"])
    stats_df["2PA"] = (stats_df["FGA"] - stats_df["3PA"])
    stats_df["2P%"] = (stats_df["2PM"] / stats_df["2PA"])
    stats_df["2P%"] = stats_df["2P%"].round(4)

    stats_df["eFG%"] = (
        (
            stats_df["FGM"] +
            (stats_df["3PM"] * 0.5)
        ) /
        stats_df["FGA"]
    )
    stats_df["eFG%"] = stats_df["eFG%"].round(4)

    stats_df["TSA"] = (
        stats_df["FGA"] + (stats_df["FTA"] * 0.44)
    )
    stats_df["TS%"] = stats_df["PTS"] / (2 * stats_df["TSA"])
    stats_df["TS%"] = stats_df["TS%"].round(4)

    stats_df["TOV%"] = (
        stats_df["TOV"] /
        (
            stats_df["FGA"] +
            (stats_df["FTA"] * 0.44) +
            stats_df["TOV"]
        )
    )
    stats_df["TOV%"] = stats_df["TOV%"].round(4)
    stats_df = stats_df.reindex(
        columns=stat_columns
    )
    # print(stats_df.columns)
    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/"
        + "player_game_stats/"
        + f"{season}_{player_id}_player_game_stats.csv",
        index=False,
    )
    return stats_df


def get_basketball_game_player_stats(game_id: int) -> pd.DataFrame:
    """
    Given a valid game ID,
    this function will attempt to get all player game stats, if possible.

    NOTE: Due to an issue with [stats.ncaa.org](stats.ncaa.org),
    full player game stats may not be loaded in through this function.

    This is a known issue, however you should be able to get position
    data and starters information through this function

    Parameters
    ----------
    `game_id` (int, mandatory):
        Required argument.
        Specifies the game you want player game stats from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.basketball import get_basketball_game_player_stats

    ########################################
    #          Men's Basketball            #
    ########################################

    # Get the game stats of the
    # 2024 NCAA D1 Men's Basketball National Championship game.
    print(
        "Get the game stats of the "
        + "2024 NCAA D1 Men's Basketball National Championship game."
    )
    df = get_basketball_game_player_stats(5254137)
    print(df)

    # Get the game stats of a March Madness game on March 29th, 2024
    # between Duke and the Houston Cougars.
    print(
        "Get the game stats of a March Madness game on March 29th, 2024 "
        + "between Duke and the Houston Cougars."
    )
    df = get_basketball_game_player_stats(5254126)
    print(df)

    # Get the game stats of a St. Patrick's Day
    # game between the Duquesne Dukes and VCU Rams (D1).
    print(
        "Get the game stats of a St. Patrick's Day "
        + "game between the Duquesne Dukes and VCU Rams (D1)."
    )
    df = get_basketball_game_player_stats(5252318)
    print(df)

    # Get the game stats of a December 17th, 2023
    # game between the Barry Buccaneers and Findlay Oilers (D2).
    print(
        "Get the game stats of a December 17th, 2023 "
        + "game between the Barry Buccaneers and Findlay Oilers (D2)."
    )
    df = get_basketball_game_player_stats(3960610)
    print(df)

    # Get the game stats of a Valentine's Day
    # game between the Kalamazoo Hornets and the Trine Thunder (D2).
    print(
        "Get the game stats of a Valentine's Day "
        + "game between the Kalamazoo Hornets and the Trine Thunder (D2)."
    )
    df = get_basketball_game_player_stats(3967963)
    print(df)


    ########################################
    #          Women's Basketball          #
    ########################################

    # Get the game stats of the
    # 2024 NCAA D1 Women's Basketball National Championship game.
    print(
        "Get the game stats of the "
        + "2024 NCAA D1 Women's Basketball National Championship game"
    )
    df = get_basketball_game_player_stats(5254137)
    print(df)

    # Get the game stats of a March 3rd, 2024
    # game between Duke and the North Carolina Tar Heels.
    print(
        "Get the game stats of a March 3rd, 2024 "
        + "game between Duke and the North Carolina Tar Heels"
    )
    df = get_basketball_game_player_stats(3984600)
    print(df)

    # Get the game stats of a Thanksgiving Day
    # game between the Sacred Heart Pioneers and the P.R.-Mayaguez Janes (D2).
    print(
        "Get the game stats of a Thanksgiving Day "
        + "game between the Sacred Heart Pioneers and "
        + "the P.R.-Mayaguez Janes (D2)."
    )
    df = get_basketball_game_player_stats(3972687)
    print(df)

    # Get the game stats of a January 21st, 2024
    # game between the Puget Sound Loggers
    # and the Whitworth Pirates (D3).
    print(
        "Get the game stats of a January 21st, 2024 "
        + "game between the Puget Sound Loggers and "
        + "the Whitworth Pirates (D3)."
    )
    df = get_basketball_game_player_stats(3979051)
    print(df)
    ```

    Returns
    ----------
    A pandas `DataFrame` object with player game stats in a given game.

    """
    load_from_cache = True

    sport_id = ""
    season = 0

    mbb_teams_df = load_basketball_teams(get_wbb_data=False)
    mbb_team_ids_arr = mbb_teams_df["team_id"].to_list()

    wbb_teams_df = load_basketball_teams(get_wbb_data=True)
    wbb_team_ids_arr = wbb_teams_df["team_id"].to_list()

    stats_df = pd.DataFrame()
    stats_df_arr = []

    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    stat_columns = [
        "season",
        "game_id",
        "team_id",
        "team_name",
        "player_id",
        "player_num",
        "player_full_name",
        "player_position",
        "GP",
        "GS",
        "MP_str",
        "MP_minutes",
        "MP_seconds",
        "MP_total_seconds",
        "FGM",
        "FGA",
        "FG%",
        "3PM",
        "3PA",
        "3P%",
        "2PM",
        "2PA",
        "2P%",
        "eFG%",
        "FT",
        "FTA",
        "FT%",
        "TSA",
        "TS%",
        "ORB",
        "DRB",
        "TRB",
        "AST",
        "STL",
        "BLK",
        "TOV",
        "TOV%",
        "PF",
        "TF",
        "PTS",
        "DQ",
        "DBL_DBL",
        "TRP_DBL",
    ]

    url = f"https://stats.ncaa.org/contests/{game_id}/individual_stats"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_MBB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_MBB/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_MBB/game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_MBB/game_stats/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_MBB/game_stats/player/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_MBB/game_stats/player/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/basketball_MBB/game_stats/player/"
        + f"{game_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_MBB/game_stats/player/"
            + f"{game_id}_player_game_stats.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/basketball_MBB/game_stats/player/"
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

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_WBB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_WBB/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_WBB/game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_WBB/game_stats/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_WBB/game_stats/player/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_WBB/game_stats/player/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/basketball_WBB/game_stats/player/"
        + f"{game_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_WBB/game_stats/player/"
            + f"{game_id}_player_game_stats.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/basketball_WBB/game_stats/player/"
                + f"{game_id}_player_game_stats.csv"
            )
        )
        load_from_cache = True
    else:
        logging.info("Could not find a WBB player game stats file")

    now = datetime.today()

    age = now - file_mod_datetime

    if age.days >= 35:
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")

    # table_data = soup.find_all(
    #     "table",
    #     {"class": "small_font dataTable table-bordered"}
    # )[1]
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
            game_started = 0

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
            if "\xa0" in p_name:
                game_started = 0

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
            temp_df["GS"] = game_started

            spec_stats_df_arr.append(temp_df)
            del temp_df

        spec_stats_df = pd.concat(
            spec_stats_df_arr,
            ignore_index=True
        )

        if team_id in mbb_team_ids_arr:
            sport_id = "MBB"
            df = mbb_teams_df[mbb_teams_df["team_id"] == team_id]
            season = df["season"].iloc[0]
        elif team_id in wbb_team_ids_arr:
            sport_id = "WBB"
            df = wbb_teams_df[wbb_teams_df["team_id"] == team_id]
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
            "MP": "MP_str",
            "3FG": "3PM",
            "3FGA": "3PA",
            "ORebs": "ORB",
            "DRebs": "DRB",
            "TotReb": "TRB",
            "TO": "TOV",
            "TechFouls": "TF",
            "Fouls": "PF"
        },
        inplace=True,
    )

    if "GS" not in stats_df.columns:
        stats_df["GS"] = None

    if "DQ" not in stats_df.columns:
        stats_df["DQ"] = None

    if "TF" not in stats_df.columns:
        stats_df["TF"] = None

    if "DBL_DBL" not in stats_df.columns:
        stats_df["DBL_DBL"] = None

    if "TRP_DBL" not in stats_df.columns:
        stats_df["TRP_DBL"] = None

    stats_df = stats_df.astype(
        {
            "DQ": "uint16",
            "TF": "uint16",
        },
        errors="ignore"
    )

    stats_df = stats_df.infer_objects().fillna(0)
    stats_df = stats_df.astype(
        {
            "GP": "uint16",
            "GS": "uint16",
            "FGM": "uint16",
            "FGA": "uint16",
            "3PM": "uint16",
            "3PA": "uint16",
            "FT": "uint16",
            "FTA": "uint16",
            "PTS": "uint16",
            "ORB": "uint16",
            "DRB": "uint16",
            "TRB": "uint16",
            "AST": "uint16",
            "TOV": "uint16",
            "STL": "uint16",
            "BLK": "uint16",
            "PF": "uint16",
            "DBL_DBL": "uint16",
            "TRP_DBL": "uint16",
            # "school_id": "uint32",
        }
    )

    stats_df[["MP_minutes", "MP_seconds"]] = stats_df["MP_str"].str.split(
        ":", expand=True
    )
    stats_df[["MP_minutes", "MP_seconds"]] = stats_df[[
        "MP_minutes", "MP_seconds"
    ]].fillna(0)
    stats_df[["MP_minutes", "MP_seconds"]] = stats_df[[
        "MP_minutes", "MP_seconds"
    ]].astype("uint16")
    stats_df["MP_total_seconds"] = (
        stats_df["MP_seconds"] + (stats_df["MP_minutes"] * 60)
    )

    stats_df["FG%"] = (stats_df["FGM"] / stats_df["FGA"])
    stats_df["FG%"] = stats_df["FG%"].round(4)

    stats_df["3P%"] = (stats_df["3PM"] / stats_df["3PA"])
    stats_df["3P%"] = stats_df["3P%"].round(4)

    stats_df["FT%"] = (stats_df["FT"] / stats_df["FTA"])
    stats_df["FT%"] = stats_df["FT%"].round(4)

    stats_df["2PM"] = (stats_df["FGM"] - stats_df["3PM"])
    stats_df["2PA"] = (stats_df["FGA"] - stats_df["3PA"])
    stats_df["2P%"] = (stats_df["2PM"] / stats_df["2PA"])
    stats_df["2P%"] = stats_df["2P%"].round(4)

    stats_df["eFG%"] = (
        (
            stats_df["FGM"] +
            (stats_df["3PM"] * 0.5)
        ) /
        stats_df["FGA"]
    )
    stats_df["eFG%"] = stats_df["eFG%"].round(4)

    stats_df["TSA"] = (
        stats_df["FGA"] + (stats_df["FTA"] * 0.44)
    )
    stats_df["TS%"] = stats_df["PTS"] / (2 * stats_df["TSA"])
    stats_df["TS%"] = stats_df["TS%"].round(4)

    stats_df["TOV%"] = (
        stats_df["TOV"] /
        (
            stats_df["FGA"] +
            (stats_df["FTA"] * 0.44) +
            stats_df["TOV"]
        )
    )
    stats_df["TOV%"] = stats_df["TOV%"].round(4)

    double_double_stats = ["PTS", "TRB", "AST", "BLK", "STL"]
    stats_df["DBL_DBL"] = (stats_df[double_double_stats] >= 10).sum(1) >= 2
    stats_df["TRP_DBL"] = (stats_df[double_double_stats] >= 10).sum(1) >= 3

    stats_df = stats_df.astype(
        {
            "DBL_DBL": "uint16",
            "TRP_DBL": "uint16",
        },
        errors="ignore"
    )
    stats_df = stats_df.reindex(
        columns=stat_columns
    )
    stats_df["game_id"] = game_id
    # print(stats_df.columns)
    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/basketball_{sport_id}/game_stats/player/"
        + f"{game_id}_player_game_stats.csv",
        index=False
    )
    return stats_df


def get_basketball_game_team_stats(game_id: int) -> pd.DataFrame:
    """
    Given a valid game ID,
    this function will attempt to get all team game stats, if possible.

    NOTE: Due to an issue with [stats.ncaa.org](stats.ncaa.org),
    full team game stats may not be loaded in through this function.

    This is a known issue, however you should be able to get position
    data and starters information through this function

    Parameters
    ----------
    `game_id` (int, mandatory):
        Required argument.
        Specifies the game you want team game stats from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.basketball import get_basketball_game_team_stats

    ########################################
    #          Men's Basketball            #
    ########################################

    # Get the game stats of the
    # 2024 NCAA D1 Men's Basketball National Championship game.
    print(
        "Get the game stats of the "
        + "2024 NCAA D1 Men's Basketball National Championship game."
    )
    df = get_basketball_game_team_stats(5254137)
    print(df)

    # Get the game stats of a March Madness game on March 29th, 2024
    # between Duke and the Houston Cougars.
    print(
        "Get the game stats of a March Madness game on March 29th, 2024 "
        + "between Duke and the Houston Cougars."
    )
    df = get_basketball_game_team_stats(5254126)
    print(df)

    # Get the game stats of a St. Patrick's Day
    # game between the Duquesne Dukes and VCU Rams (D1).
    print(
        "Get the game stats of a St. Patrick's Day "
        + "game between the Duquesne Dukes and VCU Rams (D1)."
    )
    df = get_basketball_game_team_stats(5252318)
    print(df)

    # Get the game stats of a December 17th, 2023
    # game between the Barry Buccaneers and Findlay Oilers (D2).
    print(
        "Get the game stats of a December 17th, 2023 "
        + "game between the Barry Buccaneers and Findlay Oilers (D2)."
    )
    df = get_basketball_game_team_stats(3960610)
    print(df)

    # Get the game stats of a Valentine's Day
    # game between the Kalamazoo Hornets and the Trine Thunder (D2).
    print(
        "Get the game stats of a Valentine's Day "
        + "game between the Kalamazoo Hornets and the Trine Thunder (D2)."
    )
    df = get_basketball_game_team_stats(3967963)
    print(df)


    ########################################
    #          Women's Basketball          #
    ########################################

    # Get the game stats of the
    # 2024 NCAA D1 Women's Basketball National Championship game.
    print(
        "Get the game stats of the "
        + "2024 NCAA D1 Women's Basketball National Championship game"
    )
    df = get_basketball_game_team_stats(5254137)
    print(df)

    # Get the game stats of a March 3rd, 2024
    # game between Duke and the North Carolina Tar Heels.
    print(
        "Get the game stats of a March 3rd, 2024 "
        + "game between Duke and the North Carolina Tar Heels"
    )
    df = get_basketball_game_team_stats(3984600)
    print(df)

    # Get the game stats of a Thanksgiving Day
    # game between the Sacred Heart Pioneers and the P.R.-Mayaguez Janes (D2).
    print(
        "Get the game stats of a Thanksgiving Day "
        + "game between the Sacred Heart Pioneers and "
        + "the P.R.-Mayaguez Janes (D2)."
    )
    df = get_basketball_game_team_stats(3972687)
    print(df)

    # Get the game stats of a January 21st, 2024
    # game between the Puget Sound Loggers
    # and the Whitworth Pirates (D3).
    print(
        "Get the game stats of a January 21st, 2024 "
        + "game between the Puget Sound Loggers and "
        + "the Whitworth Pirates (D3)."
    )
    df = get_basketball_game_team_stats(3979051)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with team game stats in a given game.

    """
    df = get_basketball_game_player_stats(game_id=game_id)
    # print(df.columns)
    df = df.infer_objects()
    stats_df = df.groupby(
        ["season", "game_id", "team_id", "team_name"],
        as_index=False
    ).agg(
        {
            # "MP_minutes": "sum",
            # "MP_seconds": "sum",
            "MP_total_seconds": "sum",
            "FGM": "sum",
            "FGA": "sum",
            "3PM": "sum",
            "3PA": "sum",
            "2PM": "sum",
            "2PA": "sum",
            "FT": "sum",
            "FTA": "sum",
            "ORB": "sum",
            "DRB": "sum",
            "TRB": "sum",
            "AST": "sum",
            "STL": "sum",
            "BLK": "sum",
            "TOV": "sum",
            "PF": "sum",
            "TF": "sum",
            "PTS": "sum",
            "DQ": "sum",
            "DBL_DBL": "sum",
            "TRP_DBL": "sum",
        }
    )
    stats_df["MP_str"] = stats_df["MP_total_seconds"].map(
        _get_minute_formatted_time_from_seconds
    )

    stats_df["FG%"] = (stats_df["FGM"] / stats_df["FGA"])
    stats_df["FG%"] = stats_df["FG%"].round(4)

    stats_df["3P%"] = (stats_df["3PM"] / stats_df["3PA"])
    stats_df["3P%"] = stats_df["3P%"].round(4)

    stats_df["FT%"] = (stats_df["FT"] / stats_df["FTA"])
    stats_df["FT%"] = stats_df["FT%"].round(4)

    stats_df["2PM"] = (stats_df["FGM"] - stats_df["3PM"])
    stats_df["2PA"] = (stats_df["FGA"] - stats_df["3PA"])
    stats_df["2P%"] = (stats_df["2PM"] / stats_df["2PA"])
    stats_df["2P%"] = stats_df["2P%"].round(4)

    stats_df["eFG%"] = (
        (
            stats_df["FGM"] +
            (stats_df["3PM"] * 0.5)
        ) /
        stats_df["FGA"]
    )
    stats_df["eFG%"] = stats_df["eFG%"].round(4)

    stats_df["TSA"] = (
        stats_df["FGA"] + (stats_df["FTA"] * 0.44)
    )
    stats_df["TS%"] = stats_df["PTS"] / (2 * stats_df["TSA"])
    stats_df["TS%"] = stats_df["TS%"].round(4)

    stats_df["TOV%"] = (
        stats_df["TOV"] /
        (
            stats_df["FGA"] +
            (stats_df["FTA"] * 0.44) +
            stats_df["TOV"]
        )
    )
    stats_df["TOV%"] = stats_df["TOV%"].round(4)

    return stats_df


def get_basketball_raw_pbp(game_id: int) -> pd.DataFrame:
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

    from ncaa_stats_py.basketball import get_basketball_raw_pbp

    ########################################
    #          Men's Basketball            #
    ########################################

    # Get the play-by-play data of the
    # 2024 NCAA D1 Men's Basketball National Championship game.
    print(
        "Get the play-by-play data of the "
        + "2024 NCAA D1 Men's Basketball National Championship game."
    )
    df = get_basketball_raw_pbp(5254137)
    print(df)

    # Get the play-by-play data of a March Madness game on March 29th, 2024
    # between Duke and the Houston Cougars.
    print(
        "Get the play-by-play data "
        + "of a March Madness game on March 29th, 2024 "
        + "between Duke and the Houston Cougars."
    )
    df = get_basketball_raw_pbp(5254126)
    print(df)

    # Get the play-by-play data of a February 28th
    # game between the Winthrop Eagles and High Point Panthers.
    print(
        "Get the play-by-play data of a February 28th "
        + "game between the Winthrop Eagles and High Point Panthers."
    )
    df = get_basketball_raw_pbp(3969302)
    print(df)

    # Get the play-by-play data of a December 19th, 2022
    # game between the San Francisco St. Gators and
    # the Cal St. Monterey Bay Otters (D2).
    print(
        "Get the play-by-play data of a December 19th, 2022 "
        + "game between the San Francisco St. Gators and " +
        "the Cal St. Monterey Bay Otters (D2)."
    )
    df = get_basketball_raw_pbp(2341500)
    print(df)

    # Get the play-by-play data of a January 3rd, 2022
    # game between the Hamline Pipers and the St. Olaf Oles (D3).
    print(
        "Get the play-by-play data of a January 3rd, 2022 "
        + "game between the Hamline Pipers and the St. Olaf Oles (D3)."
    )
    df = get_basketball_raw_pbp(3967963)
    print(df)


    ########################################
    #          Women's Basketball          #
    ########################################

    # Get the play-by-play data of the
    # 2024 NCAA D1 Women's Basketball National Championship game.
    print(
        "Get the play-by-play data of the "
        + "2024 NCAA D1 Women's Basketball National Championship game."
    )
    df = get_basketball_raw_pbp(5254137)
    print(df)

    # Get the play-by-play data of a March 12th, 2021
    # game between the La Salle Explorers and the Dayton Flyers.
    print(
        "Get the play-by-play data of a March 12th, 2021 "
        + "game between the La Salle Explorers and the Dayton Flyers."
    )
    df = get_basketball_raw_pbp(2055636)
    print(df)

    # Get the play-by-play data of a February 6th, 2020
    # game between Purdue Northwest and the Michigan Tech Huskies (D2).
    print(
        "Get the play-by-play data of a Thanksgiving Day "
        + "game between the Sacred Heart Pioneers and "
        + "the P.R.-Mayaguez Janes (D2)."
    )
    df = get_basketball_raw_pbp(1793405)
    print(df)

    # Get the play-by-play data of a January 5th, 2019
    # game between the Puget Sound Loggers
    # and the Whitworth Pirates (D3).
    print(
        "Get the play-by-play data of a January 5th, 2019 "
        + "game between the Simpson Storm and "
        + "the Dubuque Spartans (D3)."
    )
    df = get_basketball_raw_pbp(1625974)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a play-by-play (PBP) data in a given game.

    """
    load_from_cache = True
    is_overtime = False

    sport_id = ""
    season = 0
    away_score = 0
    home_score = 0

    mbb_teams_df = load_basketball_teams(get_wbb_data=False)
    mbb_team_ids_arr = mbb_teams_df["team_id"].to_list()

    wbb_teams_df = load_basketball_teams(get_wbb_data=True)
    wbb_team_ids_arr = wbb_teams_df["team_id"].to_list()

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
        "half_num",
        "event_num",
        "game_time_str",
        "game_time_seconds",
        "game_time_milliseconds",
        "event_team",
        "event_text",
        "is_overtime",
        "stadium_name",
        "attendance",
        "away_team_id",
        "away_team_name",
        "home_team_id",
        "home_team_name",
    ]

    url = f"https://stats.ncaa.org/contests/{game_id}/play_by_play"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_MBB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_MBB/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_MBB/raw_pbp/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_MBB/raw_pbp/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/basketball_MBB/raw_pbp/"
        + f"{game_id}_raw_pbp.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_MBB/raw_pbp/"
            + f"{game_id}_raw_pbp.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/basketball_MBB/raw_pbp/"
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

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_WBB/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_WBB/")

    if exists(f"{home_dir}/.ncaa_stats_py/basketball_WBB/raw_pbp/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/basketball_WBB/raw_pbp/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/basketball_WBB/raw_pbp/"
        + f"{game_id}_raw_pbp.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_WBB/raw_pbp/"
            + f"{game_id}_raw_pbp.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/basketball_WBB/raw_pbp/"
                + f"{game_id}_raw_pbp.csv"
            )
        )
        load_from_cache = True
    else:
        logging.info("Could not find a WBB player game stats file")

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
        game_date_str = game_date_str.replace(" ", "")
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y')
    else:
        game_datetime = datetime.strptime(
            game_date_str,
            '%m/%d/%Y %I:%M %p'
        )
    game_datetime = game_datetime.astimezone(timezone("US/Eastern"))
    game_date_str = game_datetime.isoformat()
    del game_datetime

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

    if home_team_id in mbb_team_ids_arr:
        sport_id = "MBB"
        temp_df = mbb_teams_df[mbb_teams_df["team_id"] == home_team_id]
        season = temp_df["season"].iloc[0]
        del temp_df
    elif home_team_id in wbb_team_ids_arr:
        sport_id = "WBB"
        temp_df = wbb_teams_df[wbb_teams_df["team_id"] == home_team_id]
        season = temp_df["season"].iloc[0]
        del temp_df
    # This should never be the case,
    # but if something goes very horribly wrong,
    # double check the away team ID to
    # the MBB and WBB team ID list.
    elif away_team_id in mbb_team_ids_arr:
        sport_id = "MBB"
        temp_df = mbb_teams_df[mbb_teams_df["team_id"] == away_team_id]
        season = temp_df["season"].iloc[0]
        del temp_df
    elif away_team_id in wbb_team_ids_arr:
        sport_id = "WBB"
        temp_df = wbb_teams_df[wbb_teams_df["team_id"] == home_team_id]
        season = temp_df["season"].iloc[0]
        del temp_df
    # If we get to this, we are in a code red situation.
    # "SHUT IT DOWN" - Gordon Ramsay
    else:
        raise ValueError(
            "Could not identify if this is a " +
            "MBB or WBB game based on team IDs. "
        )

    section_cards = soup.find_all(
        "div",
        {"class": "row justify-content-md-center w-100"}
    )

    for card in section_cards:
        # top_bot = ""
        event_text = ""
        half_str = card.find(
            "div",
            {"class": "card-header"}
        ).text
        half_num = re.findall(
            r"([0-9]+)",
            half_str
        )

        half_num = int(half_num[0])
        if "ot" in half_str.lower():
            is_overtime = True
            half_num += 2
        table_body = card.find("table").find("tbody").find_all("tr")

        for row in table_body:
            t_cells = row.find_all("td")
            t_cells = [x.text.strip() for x in t_cells]
            game_time_str = t_cells[0]

            if len(t_cells[1]) > 0:
                event_team = away_team_id
                event_text = t_cells[1]
            elif len(t_cells[3]) > 0:
                event_team = home_team_id
                event_text = t_cells[3]

            if t_cells[1].lower() == "game start":
                pass
            elif t_cells[1].lower() == "jumpball startperiod":
                pass
            elif t_cells[1].lower() == "period start":
                pass
            elif t_cells[1].lower() == "period end confirmed;":
                pass
            elif t_cells[1].lower() == "period end confirmed":
                pass
            elif t_cells[1].lower() == "game end confirmed;":
                pass
            elif t_cells[1].lower() == "game end confirmed":
                pass
            elif t_cells[1].lower() == "timeout commercial":
                pass
            else:
                away_score, home_score = t_cells[2].split("-")

            away_score = int(away_score)
            home_score = int(home_score)
            if len(game_time_str.split(":")) == 3:
                temp_time_minutes, temp_time_seconds, game_time_ms = \
                    game_time_str.split(":")
            elif len(game_time_str.split(":")) == 2:
                temp_time_minutes, temp_time_seconds = \
                    game_time_str.split(":")
                game_time_ms = 0

            temp_time_minutes = int(temp_time_minutes)
            temp_time_seconds = int(temp_time_seconds)
            game_time_ms = int(game_time_ms)
            game_time_seconds = temp_time_seconds + (temp_time_minutes * 60)

            if half_num == 1:
                half_seconds_remaining = game_time_seconds
                half_ms_remaining = game_time_ms

                game_time_seconds += 1200
            else:
                half_seconds_remaining = game_time_seconds
                half_ms_remaining = game_time_ms

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
                    "half_seconds_remaining": half_seconds_remaining,
                    "half_milliseconds_remaining": half_ms_remaining,
                    "game_seconds_remaining": game_time_seconds,
                    "game_milliseconds_remaining": game_time_ms,
                    "half_num": half_num,
                    "event_team": event_team,
                    "event_text": event_text,
                    "is_overtime": is_overtime
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

    pbp_df = pbp_df.reindex(columns=stat_columns)
    pbp_df = pbp_df.infer_objects()

    if sport_id == "MBB":
        pbp_df.to_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_MBB/raw_pbp/"
            + f"{game_id}_raw_pbp.csv",
            index=False
        )
    elif sport_id == "WBB":
        pbp_df.to_csv(
            f"{home_dir}/.ncaa_stats_py/basketball_WBB/raw_pbp/"
            + f"{game_id}_raw_pbp.csv",
            index=False
        )
    else:
        raise ValueError(
            f"Improper Sport ID: `{sport_id}`"
        )

    return pbp_df


def get_basketball_game_starters(game_id: int) -> list:
    """
    Given a valid game ID, this function will attempt to
    get the starting lineup out of the raw play-by-play data
    from the game.

    NOTE #1: The layout of the list will be as follows:

    > | Index |   **Away players**   |
    > | :---: | :------------------: |
    > |   0   | Away team starter #1 |
    > |   1   | Away team starter #2 |
    > |   2   | Away team starter #3 |
    > |   3   | Away team starter #4 |
    > |   4   | Away team starter #5 |

    > | Index |   **Home players**   |
    > | :---: | :------------------: |
    > |   5   | Home team starter #1 |
    > |   6   | Home team starter #2 |
    > |   7   | Home team starter #3 |
    > |   8   | Home team starter #4 |
    > |   9   | Home team starter #5 |

    NOTE #2: Starters are listed in order of when they first sub out.
    Do not assume that starter #5 for a team is a center,
    or that starter #1 is a PG!

    Returns
    ----------
    A list of starters from a specific basketball game ID.

    """
    starters_list = []
    pbp_df = get_basketball_raw_pbp(game_id=game_id)
    away_team_id = pbp_df["away_team_id"].iloc[0]
    home_team_id = pbp_df["home_team_id"].iloc[0]
    # pointer_int = 0

    for team_id in [away_team_id, home_team_id]:
        temp_starters_list = []

        temp_df = pbp_df[pbp_df["event_team"] == team_id]

        play_text_list = temp_df["event_text"].to_list()

        for play_txt in play_text_list:
            if len(temp_starters_list) == 5:
                break
            elif "substitution out" in play_txt:
                player_txt = play_txt.split(",")[0]
                if play_txt in temp_starters_list:
                    pass
                elif player_txt.lower() == "team":
                    pass
                elif (player_txt is None) or (len(player_txt) == 0):
                    raise ValueError(
                        "Player cannot be NULL."
                    )
                else:
                    temp_starters_list.append(player_txt)

        if len(temp_starters_list) < 5:
            raise ValueError(
                f"Could not find all 5 starters for team ID {team_id} " +
                f"in game ID {game_id}"
            )
        for txt in temp_starters_list:
            starters_list.append(txt)
    return starters_list


def get_basketball_game_shot_locations(game_id: int) -> pd.DataFrame:
    """ """
    raise NotImplementedError(
        "It's not implemented yet."
    )
