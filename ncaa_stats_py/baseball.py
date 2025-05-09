# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: `baseball.py`
# Purpose: Houses functions that allows one to access NCAA Baseball data
# Creation Date: 2024-06-25 07:40 PM EDT
# Update History:
# - 2024-06-25 07:40 PM EDT
# - 2024-09-19 04:30 PM EDT
# - 2024-09-23 10:30 PM EDT
# - 2024-11-01 12:10 AM EDT
# - 2024-11-25 07:45 PM EDT
# - 2024-12-17 10:30 AM EDT
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
    _get_schools,
    _get_stat_id,
    _get_webpage,
)


def get_baseball_teams(season: int, level: str | int) -> pd.DataFrame:
    """
    Retrieves a list of baseball teams from the NCAA.

    Parameters
    ----------
    `season` (int, mandatory):
        Required argument.
        Specifies the season you want NCAA baseball team information from.

    `level` (int, mandatory):
        Required argument.
        Specifies the level/division you want
        NCAA baseball team information from.
        This can either be an integer (1-3) or a string ("I"-"III").

    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import get_baseball_teams

    # Get all D1 baseball teams for the 2024 season.
    print("Get all D1 baseball teams for the 2024 season.")
    df = get_baseball_teams(2024, 1)
    print(df)

    # Get all D2 baseball teams for the 2023 season.
    print("Get all D2 baseball teams for the 2023 season.")
    df = get_baseball_teams(2023, 2)
    print(df)

    # Get all D3 baseball teams for the 2022 season.
    print("Get all D3 baseball teams for the 2022 season.")
    df = get_baseball_teams(2022, 3)
    print(df)

    # Get all D1 baseball teams for the 2021 season.
    print("Get all D1 baseball teams for the 2021 season.")
    df = get_baseball_teams(2021, "I")
    print(df)

    # Get all D2 baseball teams for the 2020 season.
    print("Get all D2 baseball teams for the 2020 season.")
    df = get_baseball_teams(2020, "II")
    print(df)

    # Get all D3 baseball teams for the 2019 season.
    print("Get all D3 baseball teams for the 2019 season.")
    df = get_baseball_teams(2019, "III")
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a list of college baseball teams
    in that season and NCAA level.
    """
    # def is_comment(elem):
    #     return isinstance(elem, Comment)

    load_from_cache = True
    sport_id = "MBA"
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)
    teams_df = pd.DataFrame()
    teams_df_arr = []
    temp_df = pd.DataFrame()
    formatted_level = ""
    ncaa_level = 0

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

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/teams/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/teams/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/teams/"
        + f"{season}_{formatted_level}_teams.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/teams/"
            + f"{season}_{formatted_level}_teams.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/teams/"
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
        season >= now.year and
        now.month <= 7
    ):
        load_from_cache = False
    elif (
        age.days >= 14 and
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
        + f"academic_year={season}.0&division={ncaa_level}.0&sport_code=MBA"
    )

    response = _get_webpage(url=url)

    soup = BeautifulSoup(response.text, features="lxml")
    ranking_periods = soup.find("select", {"name": "rp", "id": "rp"})
    ranking_periods = ranking_periods.find_all("option")

    rp_value = 0
    found_value = False

    while found_value is False:
        for rp in ranking_periods:
            rp_value = rp.get("value")
            found_value = True
            break

    url = (
        "https://stats.ncaa.org/rankings/institution_trends?"
        + f"academic_year={season}.0&division={ncaa_level}.0&"
        + f"ranking_period={rp_value}&sport_code={sport_id}"
        + "&stat_seq=484.0"
    )

    best_method = True

    if season < 2013:
        url = (
            "https://stats.ncaa.org/rankings/national_ranking?"
            + f"academic_year={season}.0&division={ncaa_level}.0&"
            + f"ranking_period={rp_value}&sport_code={sport_id}"
            + "&stat_seq=484.0"
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
                + "&stat_seq=484.0"
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
        f"{home_dir}/.ncaa_stats_py/baseball/teams/"
        + f"{season}_{formatted_level}_teams.csv",
        index=False,
    )

    return teams_df


def load_baseball_teams(start_year: int = 2008) -> pd.DataFrame:
    """
    Compiles a list of known NCAA baseball teams in NCAA baseball history.

    Parameters
    ----------
    `start_year` (int, optional):
        Optional argument.
        Specifies the first season you want
        NCAA baseball team information from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import load_baseball_teams

    # Compile a list of known baseball teams
    # in NCAA baseball history.
    print(
        "Compile a list of known baseball teams " +
        "in NCAA baseball history."
    )
    df = load_baseball_teams()
    print(df)
    ```

    Returns
    ----------
    A pandas `DataFrame` object with a list of
    all known college baseball teams.

    """
    # start_year = 2008

    teams_df = pd.DataFrame()
    teams_df_arr = []
    temp_df = pd.DataFrame()

    now = datetime.now()
    ncaa_divisions = ["I", "II", "III"]
    ncaa_seasons = [x for x in range(start_year, (now.year + 1))]

    logging.info(
        "Loading in all NCAA baseball teams. "
        + "If this is the first time you're seeing this message, "
        + "it may take some time (3-10 minutes) for this to load."
    )
    for s in ncaa_seasons:
        logging.info(f"Loading in baseball teams for the {s} season.")
        for d in ncaa_divisions:
            try:
                temp_df = get_baseball_teams(season=s, level=d)
                teams_df_arr.append(temp_df)
                del temp_df
            except Exception as e:
                logging.warning(
                    "Unhandled exception when trying to " +
                    f"get the teams. Full exception: `{e}`"
                )

    teams_df = pd.concat(teams_df_arr, ignore_index=True)
    return teams_df


def get_baseball_team_schedule(team_id: int) -> pd.DataFrame:
    """
    Retrieves a team schedule, from a valid NCAA baseball team ID.

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

    from ncaa_stats_py.baseball import get_baseball_team_schedule

    # Get the team schedule for the
    # 2024 Texas Tech baseball team (D1, ID: 573916).
    print(
        "Get the team schedule for the " +
        "2024 Texas Tech baseball team (ID: 573916)."
    )
    df = get_baseball_team_schedule(573916)
    print(df)

    # Get the team schedule for the
    # 2023 Emporia St. baseball team (D2, ID: 548561).
    print(
        "Get the team schedule for the " +
        "2023 Emporia St. baseball team (D2, ID: 548561)."
    )
    df = get_baseball_team_schedule(548561)
    print(df)

    # Get the team schedule for the
    # 2022 Pfeiffer baseball team (D3, ID: 526836).
    print(
        "Get the team schedule for the " +
        "2022 Pfeiffer baseball team (D3, ID: 526836)."
    )
    df = get_baseball_team_schedule(526836)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with an NCAA baseball team's schedule.

    """
    sport_id = "MBA"
    schools_df = _get_schools()
    games_df = pd.DataFrame()
    games_df_arr = []
    season = 0
    temp_df = pd.DataFrame()
    load_from_cache = True

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    url = f"https://stats.ncaa.org/teams/{team_id}"

    team_df = load_baseball_teams()

    team_df = team_df[team_df["team_id"] == team_id]

    season = team_df["season"].iloc[0]
    ncaa_division = team_df["ncaa_division"].iloc[0]
    ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
    # team_conference_name = team_df["team_conference_name"].iloc[0]
    # school_name = team_df["school_name"].iloc[0]
    # school_id = int(team_df["school_id"].iloc[0])

    del team_df

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/team_schedule/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/team_schedule/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/team_schedule/"
        + f"{team_id}_team_schedule.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/team_schedule/"
            + f"{team_id}_team_schedule.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/team_schedule/"
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
        season >= now.year and
        now.month <= 7
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
    # For NCAA Baseball, the season always starts in the spring semester,
    # and ends in the fall semester.
    # Thus, if `season_name` = "2011-12", this is the "2012" baseball season,
    # because 2012 would encompass the spring and fall semesters
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
        innings = 9
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
                    score_2, innings = score_2.split("(")
                    innings = int(innings)

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
                        "innings": innings,
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
                            "innings": innings,
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
                            "innings": innings,
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
                        "innings": innings,
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
    games_df["sport_id"] = sport_id

    # games_df["game_url"] = games_df["game_url"].str.replace("/box_score", "")
    games_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/baseball/team_schedule/"
        + f"{team_id}_team_schedule.csv",
        index=False,
    )

    return games_df


def get_baseball_day_schedule(
    game_date: str | date | datetime,
    level: str | int = "I",
):
    """
    Given a date and NCAA level, this function retrieves baseball every game
    for that date.

    Parameters
    ----------
    `game_date` (int, mandatory):
        Required argument.
        Specifies the date you want a baseball schedule from.
        For best results, pass a string formatted as "YYYY-MM-DD".

    `level` (int, mandatory):
        Required argument.
        Specifies the level/division you want a
        NCAA baseball schedule from.
        This can either be an integer (1-3) or a string ("I"-"III").

    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import get_baseball_day_schedule

    # Get all DI games that will be played on April 14th, 2025.
    print("Get all games that will be played on April 14th, 2025.")
    df = get_baseball_day_schedule("2025-04-14", level=1)
    print(df)

    # Get all DI games that were played on February 14th, 2025.
    print("Get all games that were played on February 14th, 2025.")
    df = get_baseball_day_schedule("2025-02-14", level="I")
    print(df)

    # Get all division II games that were played on February 14th, 2025.
    print("Get all division II games that were played on February 14th, 2025.")
    df = get_baseball_day_schedule("2025-02-14", level="II")
    print(df)

    # Get all DI games (if any) that were played on June 24th, 2024.
    print("Get all DI games (if any) that were played on June 24th, 2024.")
    df = get_baseball_day_schedule("2024-06-24")
    print(df)

    # Get all DI games played on March 3rd, 2024.
    print("Get all DI games played on March 3rd, 2024.")
    df = get_baseball_day_schedule("2024-03-03")
    print(df)

    # Get all division III games played on April 4th, 2024.
    print("Get all division III games played on April 4th, 2024.")
    df = get_baseball_day_schedule("2024-04-04")
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with all baseball games played on that day,
    for that NCAA division/level.

    """

    season = 0
    sport_id = "MBA"

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

    season = game_datetime.year
    game_month = game_datetime.month
    game_day = game_datetime.day
    game_year = game_datetime.year

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

        if "canceled" in td_arr[5].text.lower():
            continue
        elif "ppd" in td_arr[5].text.lower():
            continue
        elif "canceled" in td_arr[-1].text.lower():
            continue
        elif "ppd" in td_arr[-1].text.lower():
            continue

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

        away_runs_scored = td_arr[-3].text
        away_runs_scored = away_runs_scored.replace("\n", "")
        away_runs_scored = away_runs_scored.replace("\xa0", "")
        if len(away_runs_scored) > 0:
            away_runs_scored = int(away_runs_scored)
        else:
            away_runs_scored = 0

        away_hits = td_arr[-2].text
        away_hits = away_hits.replace("\n", "")
        away_hits = away_hits.replace("\xa0", "")
        if len(away_hits) > 0:
            away_hits = int(away_hits)
        else:
            away_hits = 0

        away_errors = td_arr[-1].text
        away_errors = away_errors.replace("\n", "")
        away_errors = away_errors.replace("\xa0", "")
        if len(away_errors) > 0:
            away_errors = int(away_errors)
        else:
            away_errors = 0

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

        home_runs_scored = td_arr[-3].text
        home_runs_scored = home_runs_scored.replace("\n", "")
        if len(home_runs_scored) > 0:
            home_runs_scored = int(home_runs_scored)
        else:
            home_runs_scored = 0

        home_hits = td_arr[-2].text
        home_hits = home_hits.replace("\n", "")
        home_hits = home_hits.replace("\xa0", "")
        if len(home_hits) > 0:
            home_hits = int(home_hits)
        else:
            home_hits = 0

        home_errors = td_arr[-1].text
        home_errors = home_errors.replace("\n", "")
        home_errors = home_errors.replace("\xa0", "")
        if len(home_errors) > 0:
            home_errors = int(home_errors)
        else:
            home_errors = 0

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
                "home_runs_scored": home_runs_scored,
                "home_hits": home_hits,
                "home_errors": home_errors,
                "away_runs_scored": away_runs_scored,
                "away_hits": away_hits,
                "away_errors": away_errors,
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


def get_full_baseball_schedule(
    season: int,
    level: str | int = "I"
) -> pd.DataFrame:
    """
    Retrieves a full baseball schedule,
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
    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import get_full_baseball_schedule

    # Get the entire 2024 schedule for the 2024 D1 baseball season.
    print("Get the entire 2024 schedule for the 2024 D1 baseball season.")
    df = get_full_baseball_schedule(season=2024, level="I")
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
    df = get_full_baseball_schedule(season=2024, level=1)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with an NCAA baseball
    schedule for a specific season and level.
    """
    load_from_cache = True
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)
    schedule_df = pd.DataFrame()
    schedule_df_arr = []
    temp_df = pd.DataFrame()
    formatted_level = ""
    ncaa_level = 0

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

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/full_schedule/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/full_schedule/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/full_schedule/"
        + f"{season}_{formatted_level}_full_schedule.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/full_schedule/"
            + f"{season}_{formatted_level}_full_schedule.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/full_schedule/"
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
        season >= now.year and
        now.month <= 7
    ):
        load_from_cache = False

    if load_from_cache is True:
        return teams_df

    teams_df = load_baseball_teams()
    teams_df = teams_df[
        (teams_df["season"] == season) &
        (teams_df["ncaa_division"] == ncaa_level)
    ]
    team_ids_arr = teams_df["team_id"].to_numpy()

    for team_id in tqdm(team_ids_arr):
        temp_df = get_baseball_team_schedule(team_id=team_id)
        schedule_df_arr.append(temp_df)

    schedule_df = pd.concat(schedule_df_arr, ignore_index=True)
    schedule_df = schedule_df.drop_duplicates(subset="game_id", keep="first")
    schedule_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/baseball/full_schedule/"
        + f"{season}_{formatted_level}_full_schedule.csv",
        index=False
    )
    return schedule_df


def get_baseball_team_roster(team_id: int) -> pd.DataFrame:
    """
    Retrieves a baseball team's roster from a given team ID.

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

    from ncaa_stats_py.baseball import get_baseball_team_roster

    # Get the roster of the 2024 Stetson baseball team (D1, ID: 574223).
    print(
        "Get the roster of the 2024 Stetson baseball team (D1, ID: 574223)."
    )
    df = get_baseball_team_roster(team_id=574223)
    print(df)

    # Get the roster of the 2023 Findlay baseball team (D2, ID: 548310).
    print(
        "Get the roster of the 2023 Findlay baseball team (D2, ID: 548310)."
    )
    df = get_baseball_team_roster(team_id=548310)
    print(df)


    # Get the roster of the 2022 Mary Baldwin baseball team (D3, ID: 534084).
    print(
        "Get the roster of the 2022 Mary Baldwin " +
        "baseball team (D3, ID: 534084)."
    )
    df = get_baseball_team_roster(team_id=534084)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with
    an NCAA baseball team's roster for that season.
    """
    sport_id = "MBA"
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
        "player_batting_hand",
        "player_throwing_hand",
        "player_hometown",
        "player_high_school",
        "player_G",
        "player_GS",
        "player_url",
    ]

    team_df = load_baseball_teams()

    team_df = team_df[team_df["team_id"] == team_id]

    season = team_df["season"].iloc[0]
    ncaa_division = team_df["ncaa_division"].iloc[0]
    ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
    team_conference_name = team_df["team_conference_name"].iloc[0]
    school_name = team_df["school_name"].iloc[0]
    school_id = int(team_df["school_id"].iloc[0])

    del team_df

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/rosters/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/rosters/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/rosters/" +
        f"{team_id}_roster.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/rosters/" +
            f"{team_id}_roster.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/rosters/" +
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
        season >= now.year and
        now.month <= 7
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
    # For NCAA Baseball, the season always starts in the spring semester,
    # and ends in the fall semester.
    # Thus, if `season_name` = "2011-12", this is the "2012" baseball season,
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
        f"{home_dir}/.ncaa_stats_py/baseball/rosters/" +
        f"{team_id}_roster.csv",
        index=False,
    )
    return roster_df


def get_baseball_player_season_batting_stats(
    team_id: int,
) -> pd.DataFrame:
    """
    Given a team ID, this function retrieves and parses
    the season batting stats for all of the players in a given baseball team.

    Parameters
    ----------
    `team_id` (int, mandatory):
        Required argument.
        Specifies the team you want batting stats from.
        This is separate from a school ID, which identifies the institution.
        A team ID should be unique to a school, and a season.

    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import get_baseball_player_season_batting_stats

    # Get the season batting stats of
    # the 2024 Stetson baseball team (D1, ID: 574077).
    print(
        "Get the season batting stats of the " +
        "2024 Gonzaga baseball team (D1, ID: 574077)."
    )
    df = get_baseball_player_season_batting_stats(team_id=574077)
    print(df)

    # Get the season batting stats of
    # the 2023 Lock Haven baseball team (D2, ID: 548493).
    print(
        "Get the season batting stats of the " +
        "2023 Findlay baseball team (D2, ID: 548493)."
    )
    df = get_baseball_player_season_batting_stats(team_id=548493)
    print(df)

    # Get the season batting stats of
    # the 2022 Moravian baseball team (D3, ID: 526838).
    print(
        "Get the season batting stats of the " +
        "2022 Findlay baseball team (D2, ID: 526838)."
    )
    df = get_baseball_player_season_batting_stats(team_id=526838)
    print(df)
    ```

    Returns
    ----------
    A pandas `DataFrame` object with the season batting stats for
    all players with a given NCAA baseball team.
    """
    sport_id = "MBA"
    load_from_cache = True
    stats_df = pd.DataFrame()
    stats_df_arr = []
    temp_df = pd.DataFrame()

    stat_columns = [
        "season",
        "season_name",
        "sport_id",
        "school_id",
        "school_name",
        "ncaa_division",
        "ncaa_division_formatted",
        "team_conference_name",
        "player_id",
        "player_jersey_number",
        "player_full_name",
        "player_last_name",
        "player_first_name",
        "player_class",
        "player_position",
        "player_height",
        "player_bats_throws",
        "stat_id",
        "batting_GP",
        "batting_GS",
        "batting_AVG",
        "batting_OBP",
        "batting_SLG",
        "batting_R",
        "batting_AB",
        "batting_H",
        "batting_2B",
        "batting_3B",
        "batting_TB",
        "batting_HR",
        "batting_RBI",
        "batting_BB",
        "batting_HBP",
        "batting_SF",
        "batting_SH",
        "batting_SO",
        "OPP DP",
        "batting_CS",
        "batting_PK",
        "batting_SB",
        "batting_IBB",
        "batting_GDP",
        "RBI2out",
    ]

    team_df = load_baseball_teams()

    team_df = team_df[team_df["team_id"] == team_id]

    season = team_df["season"].iloc[0]
    ncaa_division = team_df["ncaa_division"].iloc[0]
    ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
    team_conference_name = team_df["team_conference_name"].iloc[0]
    school_name = team_df["school_name"].iloc[0]
    school_id = int(team_df["school_id"].iloc[0])

    del team_df

    stat_id = _get_stat_id(
        sport="baseball",
        season=season,
        stat_type="batting"
    )

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    url = (
        f"https://stats.ncaa.org/teams/{team_id}/season_to_date_stats?"
        + f"year_stat_category_id={stat_id}"
    )

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/batting/"
    ):
        pass
    else:
        mkdir(
            f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/batting/"
        )

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/batting/"
        + f"{season:00d}_{school_id:00d}_player_batting_season_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/batting/"
            + f"{season:00d}_{school_id:00d}_player_batting_season_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/"
                + "player_season_stats/batting/"
                + f"{season:00d}_{school_id:00d}" +
                "_player_batting_season_stats.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days > 1 and
        season >= now.year and
        now.month <= 7
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
    # For NCAA Baseball, the season always starts in the spring semester,
    # and ends in the fall semester.
    # Thus, if `season_name` = "2011-12", this is the "2012" baseball season,
    # because 2012 would encompass the spring and fall semesters
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
        if "year_stat_category_id" in player_id:
            stat_id = player_id
            stat_id = stat_id.rsplit("?")[-1]
            stat_id = stat_id.replace("?", "").replace(
                "year_stat_category_id=", ""
            )
            stat_id = int(stat_id)

            player_id = player_id.split("?")[0]

        player_id = int(player_id)

        temp_df["player_id"] = player_id
        temp_df["player_last_name"] = p_last.strip()
        temp_df["player_first_name"] = p_first.strip()

        stats_df_arr.append(temp_df)
        del temp_df

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    stats_df = stats_df.replace("", None)

    stats_df["stat_id"] = stat_id
    stats_df["season"] = season
    stats_df["season_name"] = season_name
    stats_df["school_id"] = school_id
    stats_df["school_name"] = school_name
    stats_df["ncaa_division"] = ncaa_division
    stats_df["ncaa_division_formatted"] = ncaa_division_formatted
    stats_df["team_conference_name"] = team_conference_name
    stats_df["sport_id"] = sport_id

    stats_df = stats_df.infer_objects()

    stats_df.rename(
        columns={
            "#": "player_jersey_number",
            "Player": "player_full_name",
            "Yr": "player_class",
            "Pos": "player_position",
            "Ht": "player_height",
            "B/T": "player_bats_throws",
            "GP": "batting_GP",
            "GS": "batting_GS",
            "BA": "batting_AVG",
            "OBPct": "batting_OBP",
            "SlgPct": "batting_SLG",
            "R": "batting_R",
            "AB": "batting_AB",
            "H": "batting_H",
            "2B": "batting_2B",
            "3B": "batting_3B",
            "TB": "batting_TB",
            "HR": "batting_HR",
            "RBI": "batting_RBI",
            "BB": "batting_BB",
            "HBP": "batting_HBP",
            "SF": "batting_SF",
            "SH": "batting_SH",
            "K": "batting_SO",
            "GDP": "batting_GDP",
            "CS": "batting_CS",
            "Picked": "batting_PK",
            "SB": "batting_SB",
            "IBB": "batting_IBB",
        },
        inplace=True,
    )

    for i in stats_df.columns:
        if i in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )

    stats_df = stats_df.reindex(
        columns=stat_columns
    )

    stats_df = stats_df.infer_objects()
    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/baseball/"
        + "player_season_stats/batting/"
        + f"{school_id:00d}_player_batting_season_stats.csv",
        index=False,
    )

    return stats_df


def get_baseball_player_season_pitching_stats(
    team_id: int,
) -> pd.DataFrame:
    """
    Given a team ID, this function retrieves and parses
    the season pitching stats for all of the players in a given baseball team.

    Parameters
    ----------
    `team_id` (int, mandatory):
        Required argument.
        Specifies the team you want pitching stats from.
        This is separate from a school ID, which identifies the institution.
        A team ID should be unique to a school, and a season.

    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import (
        get_baseball_player_season_pitching_stats
    )

    # Get the season pitching stats of
    # the 2024 Minnesota baseball team (D1, ID: 574129).
    print(
        "Get the season pitching stats of the " +
        "2024 Minnesota baseball team (D1, ID: 574129)."
    )
    df = get_baseball_player_season_pitching_stats(team_id=574129)
    print(df)
    1
    # Get the season pitching stats of
    # the 2023 Missouri S&T baseball team (D2, ID: 548504).
    print(
        "Get the season pitching stats of the " +
        "2023 Missouri S&T baseball team (D2, ID: 548504)."
    )
    df = get_baseball_player_season_pitching_stats(team_id=548504)
    print(df)

    # Get the season pitching stats of
    # the 2022 Rowan baseball team (D3, ID: 527161).
    print(
        "Get the season pitching stats of the " +
        "2022 Rowan baseball team (D2, ID: 527161)."
    )
    df = get_baseball_player_season_pitching_stats(team_id=527161)
    print(df)
    ```

    Returns
    ----------
    A pandas `DataFrame` object with the season pitching stats for
    all players with a given NCAA baseball team.
    """
    sport_id = "MBA"
    load_from_cache = True
    stats_df = pd.DataFrame()
    stats_df_arr = []
    temp_df = pd.DataFrame()

    stat_columns = [
        "season",
        "season_name",
        "sport_id",
        "school_id",
        "school_name",
        "ncaa_division",
        "ncaa_division_formatted",
        "team_conference_name",
        "player_id",
        "player_jersey_number",
        "player_full_name",
        "player_last_name",
        "player_first_name",
        "player_class",
        "player_position",
        "player_height",
        "player_bats_throws",
        "stat_id",

        "pitching_G",
        "pitching_APP",
        "pitching_GS",
        "pitching_ERA",
        "pitching_IP",
        "pitching_CG",
        "pitching_H",
        "pitching_R",
        "pitching_ER",
        "pitching_BB",
        "pitching_SO",
        "pitching_SHO",
        "pitching_BF",
        "pitching_AB",
        "pitching_2B",
        "pitching_3B",
        "pitching_BK",
        "pitching_HR",
        "pitching_WP",
        "pitching_HBP",
        "pitching_IBB",
        "pitching_IR",
        "pitching_IRS",
        "pitching_SH",
        "pitching_SFA",
        "pitching_PI",
        "pitching_GO",
        "pitching_FO",
        "pitching_W",
        "pitching_L",
        "pitching_SV",
        "pitching_KL",
        "pitching_PK",
    ]

    team_df = load_baseball_teams()

    team_df = team_df[team_df["team_id"] == team_id]

    season = team_df["season"].iloc[0]
    ncaa_division = team_df["ncaa_division"].iloc[0]
    ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
    team_conference_name = team_df["team_conference_name"].iloc[0]
    school_name = team_df["school_name"].iloc[0]
    school_id = int(team_df["school_id"].iloc[0])

    del team_df

    stat_id = _get_stat_id(
        sport="baseball",
        season=season,
        stat_type="pitching"
    )

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    url = (
        f"https://stats.ncaa.org/teams/{team_id}/season_to_date_stats?"
        + f"year_stat_category_id={stat_id}"
    )

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/pitching/"
    ):
        pass
    else:
        mkdir(
            f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/pitching/"
        )

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/pitching/"
        + f"{season:00d}_{school_id:00d}_player_pitching_season_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/pitching/"
            + f"{season:00d}_{school_id:00d}_player_pitching_season_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/"
                + "player_season_stats/pitching/"
                + f"{season:00d}_{school_id:00d}" +
                "_player_pitching_season_stats.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days > 1 and
        season >= now.year and
        now.month <= 7
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
    # For NCAA Baseball, the season always starts in the spring semester,
    # and ends in the fall semester.
    # Thus, if `season_name` = "2011-12", this is the "2012" baseball season,
    # because 2012 would encompass the spring and fall semesters
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
        if "year_stat_category_id" in player_id:
            stat_id = player_id
            stat_id = stat_id.rsplit("?")[-1]
            stat_id = stat_id.replace("?", "").replace(
                "year_stat_category_id=", ""
            )
            stat_id = int(stat_id)

            player_id = player_id.split("?")[0]

        player_id = int(player_id)

        temp_df["player_id"] = player_id
        temp_df["player_last_name"] = p_last.strip()
        temp_df["player_first_name"] = p_first.strip()

        stats_df_arr.append(temp_df)
        del temp_df

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    stats_df = stats_df.replace("", None)

    stats_df["stat_id"] = stat_id
    stats_df["season"] = season
    stats_df["season_name"] = season_name
    stats_df["school_id"] = school_id
    stats_df["school_name"] = school_name
    stats_df["ncaa_division"] = ncaa_division
    stats_df["ncaa_division_formatted"] = ncaa_division_formatted
    stats_df["team_conference_name"] = team_conference_name
    stats_df["sport_id"] = sport_id

    stats_df = stats_df.infer_objects()

    # Rename columns
    stats_df.rename(
        columns={
            "#": "player_jersey_number",
            "Player": "player_full_name",
            "Yr": "player_class",
            "Pos": "player_position",
            "Ht": "player_height",
            "B/T": "player_bats_throws",
            "App": "pitching_APP",
            "G": "pitching_G",
            "GS": "pitching_GS",
            "ERA": "pitching_ERA",
            "IP": "pitching_IP",
            "CG": "pitching_CG",
            "H": "pitching_H",
            "R": "pitching_R",
            "ER": "pitching_ER",
            "BB": "pitching_BB",
            "SO": "pitching_SO",
            "SHO": "pitching_SHO",
            "BF": "pitching_BF",
            "P-OAB": "pitching_AB",
            "2B-A": "pitching_2B",
            "3B-A": "pitching_3B",
            "HR-A": "pitching_HR",
            "Bk": "pitching_BK",
            "WP": "pitching_WP",
            "HB": "pitching_HBP",
            "Inh Run": "pitching_IR",
            "Inh Run Score": "pitching_IRS",
            "SHA": "pitching_SH",
            "SFA": "pitching_SFA",
            "Pitches": "pitching_PI",
            "GO": "pitching_GO",
            "FO": "pitching_FO",
            "W": "pitching_W",
            "L": "pitching_L",
            "SV": "pitching_SV",
            "KL": "pitching_KL",
            "IBB": "pitching_IBB",
            "pickoffs": "pitching_PK",
        },
        inplace=True,
    )
    # stats_df[["player_height_ft", "player_height_in"]] = stats_df[
    #     "player_height"
    # ].str.split('-', expand=True)

    # stats_df["player_height"] = stats_df["player_height_ft"] + "\' " +\
    #     stats_df["player_height_in"] + "\""

    # stats_df.drop(
    #     columns=["player_height_ft", "player_height_in"],
    #     inplace=True
    # )
    stats_df = stats_df.infer_objects()

    # print(stats_df.columns)

    for i in stats_df.columns:
        if i in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )

    stats_df = stats_df.reindex(
        columns=stat_columns
    )

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/baseball/"
        + "player_season_stats/pitching/"
        + f"{school_id:00d}_player_pitching_season_stats.csv",
        index=False,
    )

    return stats_df


def get_baseball_player_season_fielding_stats(
    team_id: int,
) -> pd.DataFrame:
    """
    Given a team ID, this function retrieves and parses
    the season fielding stats for all of the players in a given baseball team.

    Parameters
    ----------
    `team_id` (int, mandatory):
        Required argument.
        Specifies the team you want fielding stats from.
        This is separate from a school ID, which identifies the institution.
        A team ID should be unique to a school, and a season.

    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import (
        get_baseball_player_season_fielding_stats
    )

    # Get the season fielding stats of
    # the 2024 South Florida (USF) baseball team (D1, ID: 574210).
    print(
        "Get the season fielding stats of the " +
        "2024 South Florida (USF) baseball team (D1, ID: 574210)."
    )
    df = get_baseball_player_season_fielding_stats(team_id=574210)
    print(df)

    # Get the season fielding stats of
    # the 2023 Wingate baseball team (D2, ID: 548345).
    print(
        "Get the season fielding stats of the " +
        "2023 Wingate baseball team (D2, ID: 548345)."
    )
    df = get_baseball_player_season_fielding_stats(team_id=548345)
    print(df)

    # Get the season fielding stats of
    # the 2022 Texas-Dallas baseball team (D3, ID: 527042).
    print(
        "Get the season fielding stats of the " +
        "2022 Texas-Dallas baseball team (D2, ID: 527042)."
    )
    df = get_baseball_player_season_fielding_stats(team_id=527042)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with the season fielding stats for
    all players with a given NCAA baseball team.
    """
    sport_id = "MBA"
    load_from_cache = True
    stats_df = pd.DataFrame()
    stats_df_arr = []
    temp_df = pd.DataFrame()

    stat_columns = [
        "season",
        "season_name",
        "sport_id",
        "school_id",
        "school_name",
        "ncaa_division",
        "ncaa_division_formatted",
        "team_conference_name",
        "player_id",
        "player_jersey_number",
        "player_full_name",
        "player_last_name",
        "player_first_name",
        "player_class",
        "player_position",
        "player_height",
        "player_bats_throws",
        "stat_id",

        "fielding_G",
        "fielding_GS",
        "fielding_PO",
        "fielding_A",
        "fielding_TC",
        "fielding_E",
        "fielding_FLD%",
        "fielding_CI",
        "fielding_PB",
        "fielding_SBA",
        "fielding_CS",
        "fielding_DP",
        "fielding_TP",
        "fielding_SB%",
    ]

    team_df = load_baseball_teams()

    team_df = team_df[team_df["team_id"] == team_id]

    season = team_df["season"].iloc[0]
    ncaa_division = team_df["ncaa_division"].iloc[0]
    ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
    team_conference_name = team_df["team_conference_name"].iloc[0]
    school_name = team_df["school_name"].iloc[0]
    school_id = int(team_df["school_id"].iloc[0])

    del team_df

    stat_id = _get_stat_id(
        sport="baseball",
        season=season,
        stat_type="fielding"
    )

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    url = (
        f"https://stats.ncaa.org/teams/{team_id}/season_to_date_stats?"
        + f"year_stat_category_id={stat_id}"
    )

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/fielding/"
    ):
        pass
    else:
        mkdir(
            f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/fielding/"
        )

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/fielding/"
        + f"{season:00d}_{school_id:00d}_player_fielding_season_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/player_season_stats/fielding/"
            + f"{season:00d}_{school_id:00d}_player_fielding_season_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/"
                + "player_season_stats/fielding/"
                + f"{season:00d}_{school_id:00d}" +
                "_player_fielding_season_stats.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days > 1 and
        season >= now.year and
        now.month <= 7
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
    # For NCAA Baseball, the season always starts in the spring semester,
    # and ends in the fall semester.
    # Thus, if `season_name` = "2011-12", this is the "2012" baseball season,
    # because 2012 would encompass the spring and fall semesters
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
        if "year_stat_category_id" in player_id:
            stat_id = player_id
            stat_id = stat_id.rsplit("?")[-1]
            stat_id = stat_id.replace("?", "").replace(
                "year_stat_category_id=", ""
            )
            stat_id = int(stat_id)

            player_id = player_id.split("?")[0]

        player_id = int(player_id)

        temp_df["player_id"] = player_id
        temp_df["player_last_name"] = p_last.strip()
        temp_df["player_first_name"] = p_first.strip()

        stats_df_arr.append(temp_df)
        del temp_df

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    stats_df = stats_df.replace("", None)

    stats_df["stat_id"] = stat_id
    stats_df["season"] = season
    stats_df["season_name"] = season_name
    stats_df["school_id"] = school_id
    stats_df["school_name"] = school_name
    stats_df["ncaa_division"] = ncaa_division
    stats_df["ncaa_division_formatted"] = ncaa_division_formatted
    stats_df["team_conference_name"] = team_conference_name
    stats_df["sport_id"] = sport_id

    stats_df = stats_df.infer_objects()

    stats_df.rename(
        columns={
            "#": "player_jersey_number",
            "Player": "player_full_name",
            "Yr": "player_class",
            "Pos": "player_position",
            "Ht": "player_height",
            "B/T": "player_bats_throws",
            "GP": "fielding_G",
            "GS": "fielding_GS",
            "PO": "fielding_PO",
            "A": "fielding_A",
            "TC": "fielding_TC",
            "E": "fielding_E",
            "FldPct": "fielding_FLD%",
            "CI": "fielding_CI",
            "PB": "fielding_PB",
            "SBA": "fielding_SBA",
            "CSB": "fielding_CS",
            "IDP": "fielding_DP",
            "TP": "fielding_TP",
            "SBAPct": "fielding_SB%",
        },
        inplace=True,
    )
    # stats_df[["player_height_ft", "player_height_in"]] = stats_df[
    #     "player_height"
    # ].str.split('-', expand=True)

    # stats_df["player_height"] = stats_df["player_height_ft"] + "\' " +\
    #     stats_df["player_height_in"] + "\""

    # stats_df.drop(
    #     columns=["player_height_ft", "player_height_in"],
    #     inplace=True
    # )
    stats_df = stats_df.infer_objects()

    # print(stats_df.columns)

    for i in stats_df.columns:
        if i in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )

    stats_df = stats_df.reindex(
        columns=stat_columns
    )

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/baseball/"
        + "player_season_stats/fielding/"
        + f"{school_id:00d}_player_fielding_season_stats.csv",
        index=False,
    )

    return stats_df


def get_baseball_player_game_batting_stats(
    player_id: int,
    season: int
) -> pd.DataFrame:
    """
    Given a valid player ID and season,
    this function retrieves the batting stats for this player at a game level.

    Parameters
    ----------
    `player_id` (int, mandatory):
        Required argument.
        Specifies the player you want batting stats from.

    `season` (int, mandatory):
        Required argument.
        Specifies the season you want batting stats from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import (
        get_baseball_player_game_batting_stats
    )

    # Get the batting stats of Jacob Berry in 2022 (LSU).
    print(
        "Get the batting stats of Jacob Berry in 2022 (LSU)."
    )
    df = get_baseball_player_game_batting_stats(player_id=7579336, season=2022)
    print(df)

    # Get the batting stats of Alec Burleson in 2019 (ECU).
    print(
        "Get the batting stats of Alec Burleson in 2019 (ECU)."
    )
    df = get_baseball_player_game_batting_stats(player_id=6015715, season=2019)
    print(df)

    # Get the batting stats of Hunter Bishop in 2018 (Arizona St.).
    print(
        "Get the batting stats of Hunter Bishop in 2018 (Arizona St.)."
    )
    df = get_baseball_player_game_batting_stats(player_id=6014052, season=2019)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a player's batting game logs
    in a given season.
    """
    sport_id = "MBA"
    load_from_cache = True
    stats_df = pd.DataFrame()
    stats_df_arr = []
    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    stat_columns = [
        "season",
        "sport_id",
        "date",
        "game_id",
        "game_num",
        "game_innings",
        "opponent_team_id",
        "opponent",
        "team_score",
        "opponent_score",
        "result",
        "player_id",

        "batting_G",
        "batting_R",
        "batting_AB",
        "batting_H",
        "batting_2B",
        "batting_3B",
        "batting_TB",
        "batting_HR",
        "batting_RBI",
        "batting_BB",
        "batting_HBP",
        "batting_SF",
        "batting_SH",
        "batting_SO",
        "batting_OPP_DP",
        "batting_CS",
        "batting_PK",
        "batting_SB",
        "batting_IBB",
        "batting_GDP",
        "batting_RBI2out",
    ]
    stat_id = _get_stat_id(
        sport="baseball",
        season=season,
        stat_type="batting"
    )
    url = (
        f"https://stats.ncaa.org/players/{player_id}?"
        + f"year_stat_category_id={stat_id}"
    )

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/batting/"
    ):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/batting/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/batting/"
        + f"{season}_{player_id}_player_game_batting_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/batting/"
            + f"{season}_{player_id}_player_game_batting_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/"
                + "player_game_stats/batting/"
                + f"{season}_{player_id}_player_game_batting_stats.csv"
            )
        )
        games_df = games_df.infer_objects()
        load_from_cache = True
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days > 1 and
        season >= now.year and
        now.month <= 7
    ):
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")

    table_data = soup.find_all(
        "table", {"class": "small_font dataTable table-bordered"}
    )[1]

    temp_table_headers = table_data.find("thead").find("tr").find_all("th")
    table_headers = [x.text for x in temp_table_headers]

    del temp_table_headers

    temp_t_rows = table_data.find("tbody")
    temp_t_rows = temp_t_rows.find_all("tr")

    # Parse the table
    for t in temp_t_rows:
        game_num = 1
        innings = 9
        row_id = t.get("id")
        opp_team_name = ""

        if "contest" not in row_id:
            continue
        del row_id

        t_cells = t.find_all("td")
        t_cells = [x.text.strip() for x in t_cells]

        temp_df = pd.DataFrame(
            data=[t_cells],
            columns=table_headers,
            # index=[0]
        )

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
        tm_score, opp_score = result_str.split("-")

        tm_score = int(tm_score)
        if "(" in opp_score:
            opp_score = opp_score.replace(")", "")
            opp_score, innings = opp_score.split("(")
            temp_df["game_innings"] = innings

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
        temp_df["game_innings"] = innings

        if len(opp_team_name) > 0:
            temp_df["opponent"] = opp_team_name
        del opp_team_name

        stats_df_arr.append(temp_df)
        del temp_df

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    stats_df = stats_df.replace("/", "", regex=True)
    stats_df = stats_df.replace("", np.nan)
    stats_df = stats_df.infer_objects()

    stats_df["player_id"] = player_id
    stats_df["season"] = season
    stats_df["sport_id"] = sport_id

    stats_df.rename(
        columns={
            "Result": "result",
            "GP": "batting_G",
            "R": "batting_R",
            "AB": "batting_AB",
            "H": "batting_H",
            "2B": "batting_2B",
            "3B": "batting_3B",
            "TB": "batting_TB",
            "HR": "batting_HR",
            "RBI": "batting_RBI",
            "BB": "batting_BB",
            "HBP": "batting_HBP",
            "SF": "batting_SF",
            "SH": "batting_SH",
            "K": "batting_SO",
            "OPP DP": "batting_OPP_DP",
            "CS": "batting_CS",
            "Picked": "batting_PK",
            "SB": "batting_SB",
            "DP": "batting_OPP_DP",
            "IBB": "batting_IBB",
            "GDP": "batting_GDP",
            "RBI2out": "batting_RBI2out",
        },
        inplace=True,
    )
    stats_df = stats_df.infer_objects()

    # print(stats_df.columns)

    for i in stats_df.columns:
        if i in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )

    stats_df = stats_df.reindex(
        columns=stat_columns
    )

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/baseball/"
        + "player_game_stats/batting/"
        + f"{season}_{player_id}_player_game_batting_stats.csv",
        index=False,
    )
    return stats_df


def get_baseball_player_game_pitching_stats(
    player_id: int,
    season: int
) -> pd.DataFrame:
    """
    Given a valid player ID and season,
    this function retrieves the pitching stats for this player at a game level.

    Parameters
    ----------
    `player_id` (int, mandatory):
        Required argument.
        Specifies the player you want pitching stats from.

    `season` (int, mandatory):
        Required argument.
        Specifies the season you want pitching stats from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import (
        get_baseball_player_game_pitching_stats
    )

    # Get the pitching stats of Jack Leiter in 2021 (Vanderbilt).
    print(
        "Get the pitching stats of Jack Leiter in 2021 (Vanderbilt)."
    )
    df = get_baseball_player_game_pitching_stats(
        player_id=6611721,
        season=2021
    )
    print(df)

    # Get the pitching stats of Kumar Rocker in 2020 (Vanderbilt).
    print(
        "Get the pitching stats of Kumar Rocker in 2020 (Vanderbilt)."
    )
    df = get_baseball_player_game_pitching_stats(
        player_id=6552352,
        season=2020
    )
    print(df)

    # Get the pitching stats of Garrett Crochet in 2018 (Tennessee).
    print(
        "Get the pitching stats of Garrett Crochet in 2018 (Tennessee)."
    )
    df = get_baseball_player_game_pitching_stats(
        player_id=5641886,
        season=2018
    )
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a player's pitching game logs
    in a given season.

    """
    sport_id = "MBA"
    load_from_cache = True
    stats_df = pd.DataFrame()
    stats_df_arr = []
    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    stat_columns = [
        "season",
        "sport_id",
        "date",
        "game_id",
        "game_num",
        "game_innings",
        "opponent_team_id",
        "opponent",
        "team_score",
        "opponent_score",
        "result",
        "player_id",

        "pitching_G",
        "pitching_APP",
        "pitching_GS",
        "pitching_order_appeared",
        "pitching_W",
        "pitching_L",
        "pitching_SV",
        "pitching_IP_str",
        "pitching_IP",
        "pitching_CG",
        "pitching_H",
        "pitching_R",
        "pitching_ER",
        "pitching_BB",
        "pitching_SO",
        "pitching_SHO",
        "pitching_BF",
        "pitching_AB",
        "pitching_2B",
        "pitching_3B",
        "pitching_BK",
        "pitching_HR",
        "pitching_WP",
        "pitching_HBP",
        "pitching_IBB",
        "pitching_IR",
        "pitching_IRS",
        "pitching_SH",
        "pitching_SF",
        "pitching_PI",
        "pitching_GO",
        "pitching_FO",
        "pitching_KL",
        "pitching_PK",
    ]

    stat_id = _get_stat_id(
        sport="baseball",
        season=season,
        stat_type="pitching"
    )

    url = (
        f"https://stats.ncaa.org/players/{player_id}?"
        + f"year_stat_category_id={stat_id}"
    )

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/pitching/"
    ):
        pass
    else:
        mkdir(
            f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/pitching/"
        )

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/pitching/"
        + f"{player_id}_player_game_pitching_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/pitching/"
            + f"{player_id}_player_game_pitching_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/"
                + "player_game_stats/pitching/"
                + f"{player_id}_player_game_pitching_stats.csv"
            )
        )
        games_df = games_df.infer_objects()
        load_from_cache = True
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days > 1 and
        season >= now.year and
        now.month <= 7
    ):
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    # team_df = load_baseball_teams()

    # del team_df
    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")

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
        innings = 9
        row_id = t.get("id")
        opp_team_name = ""

        if "contest" not in row_id:
            continue
        del row_id

        t_cells = t.find_all("td")
        t_cells = [x.text.strip() for x in t_cells]

        temp_df = pd.DataFrame(
            data=[t_cells],
            columns=table_headers,
            # index=[0]
        )

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
        tm_score, opp_score = result_str.split("-")

        tm_score = int(tm_score)
        if "(" in opp_score:
            opp_score = opp_score.replace(")", "")
            opp_score, innings = opp_score.split("(")
            temp_df["game_innings"] = innings

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
        temp_df["game_innings"] = innings

        if len(opp_team_name) > 0:
            temp_df["opponent"] = opp_team_name
        del opp_team_name

        stats_df_arr.append(temp_df)
        del temp_df

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    stats_df = stats_df.replace("/", "", regex=True)
    stats_df = stats_df.replace("", np.nan)
    stats_df = stats_df.infer_objects()

    if "IP" not in stats_df.columns:
        raise Exception(
            "Pitching stats were not found for this player."
        )

    stats_df["IP"] = stats_df["IP"].astype("str")
    stats_df["IP_str"] = stats_df["IP"]
    stats_df["IP"] = stats_df["IP"].str.replace(".1", ".333")
    stats_df["IP"] = stats_df["IP"].str.replace(".2", ".667")
    stats_df["IP"] = stats_df["IP"].astype("float32")
    stats_df["IP_str"] = stats_df["IP_str"].str.replace("nan", "")
    stats_df["sport_id"] = sport_id

    stats_df["player_id"] = player_id
    stats_df["season"] = season
    stats_df.rename(
        columns={
            "Result": "result",
            "GP": "pitching_G",
            "App": "pitching_APP",
            "GS": "pitching_GS",
            "IP": "pitching_IP",
            "IP_str": "pitching_IP_str",
            "CG": "pitching_CG",
            "P-OAB": "pitching_AB",
            "2B-A": "pitching_2B",
            "3B-A": "pitching_3B",
            "Bk": "pitching_BK",
            "HR-A": "pitching_HR",
            "WP": "pitching_WP",
            "H": "pitching_H",
            "R": "pitching_R",
            "ER": "pitching_ER",
            "BB": "pitching_BB",
            "SO": "pitching_SO",
            "SHO": "pitching_SHO",
            "BF": "pitching_BF",
            "HB": "pitching_HBP",
            "IBB": "pitching_IBB",
            "Inh Run": "pitching_IR",
            "Inh Run Score": "pitching_IRS",
            "SHA": "pitching_SH",
            "SFA": "pitching_SF",
            "Pitches": "pitching_PI",
            "GO": "pitching_GO",
            "FO": "pitching_FO",
            "W": "pitching_W",
            "L": "pitching_L",
            "SV": "pitching_SV",
            "OrdAppeared": "pitching_order_appeared",
            "KL": "pitching_KL",
            "pickoffs": "pitching_PK",
        },
        inplace=True,
    )
    stats_df = stats_df.infer_objects()
    # print(stats_df.columns)

    for i in stats_df.columns:
        if i in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )

    stats_df = stats_df.reindex(
        columns=stat_columns
    )

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/baseball/"
        + "player_game_stats/pitching/"
        + f"{season}_{player_id}_player_game_pitching_stats.csv",
        index=False,
    )

    return stats_df


def get_baseball_player_game_fielding_stats(
    player_id: int,
    season: int
) -> pd.DataFrame:
    """
    Given a valid player ID and season,
    this function retrieves the fielding stats for this player at a game level.

    Parameters
    ----------
    `player_id` (int, mandatory):
        Required argument.
        Specifies the player you want fielding stats from.

    `season` (int, mandatory):
        Required argument.
        Specifies the season you want fielding stats from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import (
        get_baseball_player_game_fielding_stats
    )

    # Get the fielding stats of Hunter Dorraugh in 2024 (San Jose St.).
    print(
        "Get the fielding stats of Hunter Dorraugh in 2024 (San Jose St.)."
    )
    df = get_baseball_player_game_fielding_stats(
        player_id=8271037,
        season=2024
    )
    print(df)

    # Get the fielding stats of Matt Bathauer in 2023 (Adams St., DII).
    print(
        "Get the fielding stats of Matt Bathauer in 2023 (Adams St., DII)."
    )
    df = get_baseball_player_game_fielding_stats(
        player_id=7833458,
        season=2023
    )
    print(df)

    # Get the fielding stats of Paul Hamilton in 2022 (Saint Elizabeth, DIII).
    print(
        "Get the fielding stats of Paul Hamilton in 2022 " +
        "(Saint Elizabeth, DIII)."
    )
    df = get_baseball_player_game_fielding_stats(
        player_id=7581440,
        season=2022
    )
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a player's fielding game logs
    in a given season.

    """
    sport_id = "MBA"
    load_from_cache = True
    stats_df = pd.DataFrame()
    stats_df_arr = []
    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    stat_columns = [
        "season",
        "sport_id",
        "date",
        "game_id",
        "game_num",
        "game_innings",
        "opponent_team_id",
        "opponent",
        "team_score",
        "opponent_score",
        "result",
        "player_id",

        "fielding_G",
        "fielding_PO",
        "fielding_A",
        "fielding_TC",
        "fielding_E",
        "fielding_CI",
        "fielding_PB",
        "fielding_SBA",
        "fielding_CSB",
        "fielding_IDP",
        "fielding_TP",
    ]

    stat_id = _get_stat_id(
        sport="baseball",
        season=season,
        stat_type="fielding"
    )
    url = (
        f"https://stats.ncaa.org/players/{player_id}?"
        + f"year_stat_category_id={stat_id}"
    )

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/fielding/"
    ):
        pass
    else:
        mkdir(
            f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/fielding/"
        )

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/fielding/"
        + f"{player_id}_player_game_fielding_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/player_game_stats/fielding/"
            + f"{player_id}_player_game_fielding_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/"
                + "player_game_stats/fielding/"
                + f"{player_id}_player_game_fielding_stats.csv"
            )
        )
        games_df = games_df.infer_objects()
        load_from_cache = True
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if (
        age.days > 1 and
        season >= now.year and
        now.month <= 7
    ):
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")

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
        innings = 9
        row_id = t.get("id")
        opp_team_name = ""

        if "contest" not in row_id:
            continue
        del row_id

        t_cells = t.find_all("td")
        t_cells = [x.text.strip() for x in t_cells]

        temp_df = pd.DataFrame(
            data=[t_cells],
            columns=table_headers,
            # index=[0]
        )

        g_date = t_cells[0]

        if "(" in g_date:
            g_date, game_num = g_date.split("(")
            g_date = g_date.strip()

            game_num = game_num.replace(")", "")
            game_num = int(game_num)

        try:
            check = t.find_all("td")
            opp_team_id = check[1].find("a").get("href")
        except AttributeError as e:
            logging.info(
                "Could not extract a team ID for this game. " +
                f"Full exception {e}"
            )
        except IndexError:
            logging.info(
                "Couldn't find the opposition team name "
                + "for this row from an image element. "
                + "Attempting a backup method"
            )
            opp_team_name = t_cells[1]
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
        tm_score, opp_score = result_str.split("-")

        tm_score = int(tm_score)
        if "(" in opp_score:
            opp_score = opp_score.replace(")", "")
            opp_score, innings = opp_score.split("(")
            temp_df["game_innings"] = innings

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
        temp_df["game_innings"] = innings

        if len(opp_team_name) > 0:
            temp_df["opponent"] = opp_team_name
        del opp_team_name

        stats_df_arr.append(temp_df)
        del temp_df

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    stats_df = stats_df.replace("/", "", regex=True)
    stats_df = stats_df.replace("", np.nan)
    stats_df = stats_df.infer_objects()
    stats_df["player_id"] = player_id
    stats_df["season"] = season
    stats_df["sport_id"] = sport_id

    stats_df.rename(
        columns={
            "Result": "result",
            "GP": "fielding_G",
            "PO": "fielding_PO",
            "A": "fielding_A",
            "TC": "fielding_TC",
            "E": "fielding_E",
            "CI": "fielding_CI",
            "PB": "fielding_PB",
            "SBA": "fielding_SBA",
            "CS": "fielding_CS",
            "IDP": "fielding_IDP",
            "TP": "fielding_TP",
            "CSB": "fielding_CSB",
        },
        inplace=True,
    )
    stats_df = stats_df.infer_objects()
    print(stats_df.columns)

    for i in stats_df.columns:
        if i in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )

    stats_df = stats_df.reindex(
        columns=stat_columns
    )

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/baseball/"
        + "player_game_stats/fielding/"
        + f"{player_id}_player_game_fielding_stats.csv",
        index=False,
    )

    return stats_df


def get_baseball_game_player_stats(game_id: int) -> pd.DataFrame:
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

    from ncaa_stats_py.baseball import (
        get_baseball_game_player_stats
    )

    # Get the player game stats of the series winning game
    # of the 2024 NCAA D1 Baseball Championship.
    print(
        "Get the player game stats of the series winning game " +
        "of the 2024 NCAA D1 Baseball Championship."
    )
    df = get_baseball_game_player_stats(
        game_id=5336815
    )
    print(df)


    # Get the player game stats of a game that occurred between Ball St.
    # and Lehigh on February 16th, 2024.
    print(
        "Get the player game stats of a game that occurred between Ball St. " +
        "and Lehigh on February 16th, 2024."
    )
    df = get_baseball_game_player_stats(
        game_id=4525569
    )
    print(df)

    # Get the player game stats of a game that occurred between Findlay
    # and Tiffin on April 10th, 2024.
    print(
        "Get the player game stats of a game that occurred between Findlay " +
        "and Tiffin on April 10th, 2024."
    )
    df = get_baseball_game_player_stats(
        game_id=4546074
    )
    print(df)

    # Get the player game stats of a game that occurred between Dean
    # and Anna Maria on March 30th, 2024.
    print(
        "Get the player game stats of a game that occurred between Dean " +
        "and Anna Maria on March 30th, 2024."
    )
    df = get_baseball_game_player_stats(
        game_id=4543103
    )
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a player game stats in a given game.

    """
    sport_id = "MBA"
    season = 0
    load_from_cache = True

    stats_df = pd.DataFrame()
    # stats_df_arr = []

    batting_df = pd.DataFrame()
    batting_df_arr = []

    pitching_df = pd.DataFrame()
    pitching_df_arr = []

    fielding_df = pd.DataFrame()
    fielding_df_arr = []

    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    stat_columns = [
        "season",
        "sport_id",
        "game_datetime",
        "game_id",
        "team_id",
        "player_id",
        "player_jersey_number",
        "player_full_name",
        "player_positions",
        # Batting
        "batting_G",
        "batting_GS",
        "batting_R",
        "batting_AB",
        "batting_H",
        "batting_2B",
        "batting_3B",
        "batting_TB",
        "batting_HR",
        "batting_RBI",
        "batting_BB",
        "batting_HBP",
        "batting_SF",
        "batting_SH",
        "batting_SO",
        "batting_OPP_DP",
        "batting_CS",
        "batting_PK",
        "batting_SB",
        "batting_IBB",
        "batting_KL",
        # Pitching
        "pitching_GP",
        "pitching_GS",
        "pitching_IP",
        "pitching_IP_str",
        "pitching_H",
        "pitching_R",
        "pitching_ER",
        "pitching_BB",
        "pitching_SO",
        "pitching_BF",
        "pitching_2B",
        "pitching_3B",
        "pitching_BK",
        "pitching_HR",
        "pitching_WP",
        "pitching_HBP",
        "pitching_IBB",
        "pitching_IR",
        "pitching_IRS",
        "pitching_SH",
        "pitching_SF",
        "pitching_KL",
        "pitching_TUER",
        "pitching_PK",
        "pitching_order_appeared",
        # Fielding
        "fielding_G",
        "fielding_GS",
        "fielding_PO",
        "fielding_A",
        "fielding_TC",
        "fielding_E",
        "fielding_CI",
        "fielding_PB",
        "fielding_SBA",
        "fielding_CSB",
        "fielding_IDP",
        "fielding_TP",
        "fielding_SBA%",
        # misc
        "stadium_name",
        "attendance",
        "away_team_id",
        "home_team_id",
        "away_team_name",
        "home_team_name",
    ]

    url = f"https://stats.ncaa.org/contests/{game_id}/individual_stats"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/game_stats/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/game_stats/player/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/game_stats/player/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/game_stats/player/"
        + f"{game_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/game_stats/player/"
            + f"{game_id}_player_game_stats.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/game_stats/player/"
                + f"{game_id}_player_game_stats.csv"
            )
        )
        load_from_cache = True
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

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
    season = game_datetime.year
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

    table_boxes = soup.find_all("div", {"class": "card p-0 table-responsive"})

    for box in table_boxes:
        t_header = box.find(
            "div", {"class": "card-header"}
        ).find(
            "div", {"class": "row"}
        )
        t_header_str = t_header.text
        team_id = t_header.find("a").get("href")
        team_id = team_id.replace("/teams", "")
        team_id = team_id.replace(
            "javascript:togglePeriodStats(competitor_",
            ""
        )
        if "_year" in team_id:
            team_id = team_id.split("_year")[0]
        team_id = team_id.replace("/", "")

        try:
            team_id = int(team_id)
        except Exception as e:
            team_id = -1
            logging.warning(
                f"Unhandled exception: `{e}`"
            )

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
            game_started = 1
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
                player_id = team_id * -1

            t_cells = t.find_all("td")
            p_name = t_cells[1].text.replace("\n", "")
            if "\xa0" in p_name:
                game_started = 0
            t_cells = [x.text.strip() for x in t_cells]
            try:
                player_id = int(player_id)
            except Exception:
                logging.info(
                    "Could not find a player ID, skipping this row."
                )
                continue
            temp_df = pd.DataFrame(
                data=[t_cells],
                columns=table_headers
            )
            temp_df["player_id"] = player_id
            temp_df["GP"] = game_played
            temp_df["GS"] = game_started
            spec_stats_df_arr.append(temp_df)

        spec_stats_df = pd.concat(
            spec_stats_df_arr,
            ignore_index=True
        )
        spec_stats_df["team_id"] = team_id
        spec_stats_df = spec_stats_df[
            (spec_stats_df["player_id"] > 0) |
            (spec_stats_df["Name"] == "TEAM")
        ]

        if (
            "batting" in t_header_str.lower() or
            "hitting" in t_header_str.lower()
        ):
            batting_df_arr.append(spec_stats_df)
        elif "pitching" in t_header_str.lower():
            spec_stats_df["pitching_order_appeared"] = spec_stats_df.index + 1
            pitching_df_arr.append(spec_stats_df)
        elif "fielding" in t_header_str.lower():
            fielding_df_arr.append(spec_stats_df)

        del spec_stats_df

    batting_df = pd.concat(batting_df_arr, ignore_index=True)
    batting_df.rename(
        columns={
            "#": "player_jersey_number",
            "Name": "player_full_name",
            "P": "player_positions",
            "GP": "batting_G",
            "GS": "batting_GS",
            "R": "batting_R",
            "AB": "batting_AB",
            "H": "batting_H",
            "2B": "batting_2B",
            "3B": "batting_3B",
            "TB": "batting_TB",
            "HR": "batting_HR",
            "RBI": "batting_RBI",
            "BB": "batting_BB",
            "HBP": "batting_HBP",
            "SF": "batting_SF",
            "SH": "batting_SH",
            "K": "batting_SO",
            "KL": "batting_KL",
            "OPP DP": "batting_OPP_DP",
            "OPPDP": "batting_OPP_DP",
            "CS": "batting_CS",
            "Picked": "batting_PK",
            "SB": "batting_SB",
            "IBB": "batting_IBB",
            "GDP": "batting_GDP",
            "RBI2out": "batting_RBI2out",
        },
        inplace=True,
    )

    pitching_df = pd.concat(pitching_df_arr, ignore_index=True)

    try:
        pitching_df["IP"] = pitching_df["IP"].astype("str")
        pitching_df["IP_str"] = pitching_df["IP"]
        pitching_df["IP"] = pitching_df["IP"].str.replace(".1", ".333")
        pitching_df["IP"] = pitching_df["IP"].str.replace(".2", ".667")
        pitching_df["IP"] = pitching_df["IP"].astype("float32")
        pitching_df["IP_str"] = pitching_df["IP_str"].str.replace("nan", "")
    except Exception:
        logging.warning(
            "Could not locate an innings column " +
            f"for the following game ID {game_id}"
        )
    pitching_df.rename(
        columns={
            "#": "player_jersey_number",
            "Name": "player_full_name",
            "P": "player_positions",
            "IP": "pitching_IP",
            "IP_str": "pitching_IP_str",
            "GP": "pitching_GP",
            "GS": "pitching_GS",
            "CG": "pitching_CG",
            "P-OAB": "pitching_AB",
            "2B-A": "pitching_2B",
            "3B-A": "pitching_3B",
            "Bk": "pitching_BK",
            "HR-A": "pitching_HR",
            "WP": "pitching_WP",
            "H": "pitching_H",
            "R": "pitching_R",
            "ER": "pitching_ER",
            "BB": "pitching_BB",
            "SO": "pitching_SO",
            "SHO": "pitching_SHO",
            "BF": "pitching_BF",
            "HB": "pitching_HBP",
            "IBB": "pitching_IBB",
            "Inh Run": "pitching_IR",
            "InhRun": "pitching_IR",
            "Inh Run Score": "pitching_IRS",
            "InhRunScore": "pitching_IRS",
            "SHA": "pitching_SH",
            "SFA": "pitching_SF",
            "Pitches": "pitching_PI",
            "GO": "pitching_GO",
            "FO": "pitching_FO",
            "TUER": "pitching_TUER",
            "W": "pitching_W",
            "L": "pitching_L",
            "SV": "pitching_SV",
            "OrdAppeared": "pitching_order_appeared",
            "KL": "pitching_KL",
            "pickoffs": "pitching_PK",
        },
        inplace=True,
    )

    fielding_df = pd.concat(fielding_df_arr, ignore_index=True)
    fielding_df.rename(
        columns={
            "#": "player_jersey_number",
            "Name": "player_full_name",
            "P": "player_positions",
            "GP": "fielding_G",
            "GS": "fielding_GS",
            "PO": "fielding_PO",
            "A": "fielding_A",
            "TC": "fielding_TC",
            "E": "fielding_E",
            "CI": "fielding_CI",
            "PB": "fielding_PB",
            "CSB": "fielding_CSB",
            "SBA": "fielding_SBA",
            "CS": "fielding_CS",
            "IDP": "fielding_IDP",
            "TP": "fielding_TP",
            "SBAPct": "fielding_SBA%"
        },
        inplace=True,
    )

    # print(batting_df.columns)
    # print(pitching_df.columns)
    # print(fielding_df.columns)

    stats_df = pd.merge(
        left=batting_df,
        right=pitching_df,
        on=[
            'player_jersey_number',
            'player_full_name',
            'player_positions',
            'player_id',
            "team_id",
        ],
        how="outer"
    )

    stats_df = pd.merge(
        left=stats_df,
        right=fielding_df,
        on=[
            'player_jersey_number',
            'player_full_name',
            'player_positions',
            'player_id',
            "team_id",
        ],
        how="outer"
    )

    stats_df["season"] = season
    stats_df['sport_id'] = sport_id
    stats_df["game_id"] = game_id
    stats_df["game_datetime"] = game_date_str
    stats_df["stadium_name"] = stadium_str
    stats_df["attendance"] = attendance_int
    stats_df["away_team_id"] = away_team_id
    stats_df["home_team_id"] = home_team_id
    stats_df["away_team_name"] = away_team_name
    stats_df["home_team_name"] = home_team_name

    for i in stats_df.columns:
        if i in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )

    if "pitching_IP" in stats_df.columns:
        stats_df = stats_df.reindex(columns=stat_columns)
    # print(stats_df.columns)
    # stats_df = stats_df.infer_objects()
    stats_df = stats_df.infer_objects().fillna(0)
    stats_df = stats_df.astype(
        {
            "game_id": "int64",
            "team_id": "int64",
            "player_id": "int64",
            "player_jersey_number": "string",
            "player_full_name": "string",
            "player_positions": "string",
            "batting_G": "uint16",
            "batting_GS": "uint16",
            "batting_R": "uint16",
            "batting_AB": "uint16",
            "batting_H": "uint16",
            "batting_2B": "uint16",
            "batting_3B": "uint16",
            "batting_TB": "uint16",
            "batting_HR": "uint16",
            "batting_RBI": "uint16",
            "batting_BB": "uint16",
            "batting_HBP": "uint16",
            "batting_SF": "uint16",
            "batting_SH": "uint16",
            "batting_SO": "uint16",
            "batting_OPP_DP": "uint16",
            "batting_CS": "uint16",
            "batting_PK": "uint16",
            "batting_SB": "uint16",
            "batting_IBB": "uint16",
            "batting_KL": "uint16",
            "pitching_GP": "uint16",
            "pitching_GS": "uint16",
            "pitching_IP": "float16",
            "pitching_IP_str": "string",
            "pitching_H": "uint16",
            "pitching_R": "uint16",
            "pitching_ER": "uint16",
            "pitching_BB": "uint16",
            "pitching_SO": "uint16",
            "pitching_BF": "uint16",
            "pitching_2B": "uint16",
            "pitching_3B": "uint16",
            "pitching_BK": "uint16",
            "pitching_HR": "uint16",
            "pitching_WP": "uint16",
            "pitching_HBP": "uint16",
            "pitching_IBB": "uint16",
            "pitching_IR": "uint16",
            "pitching_IRS": "uint16",
            "pitching_SH": "uint16",
            "pitching_SF": "uint16",
            "pitching_KL": "uint16",
            "pitching_TUER": "uint16",
            "pitching_PK": "uint16",
            "pitching_order_appeared": "uint16",
            "fielding_G": "uint16",
            "fielding_GS": "uint16",
            "fielding_PO": "uint16",
            "fielding_A": "uint16",
            "fielding_TC": "uint16",
            "fielding_E": "uint16",
            "fielding_CI": "uint16",
            "fielding_PB": "uint16",
            "fielding_SBA": "uint16",
            "fielding_CSB": "uint16",
            "fielding_IDP": "uint16",
            "fielding_TP": "uint16",
            "fielding_SBA%": "float16",
        }
    )
    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/baseball/game_stats/player/"
        + f"{game_id}_player_game_stats.csv",
        index=False
    )
    return stats_df


def get_baseball_game_team_stats(game_id: int) -> pd.DataFrame:
    """
    Given a valid game ID,
    this function will attempt to get all team game stats, if possible.

    NOTE: Due to an issue with [stats.ncaa.org](stats.ncaa.org),
    full team game stats may not be loaded in through this function.
    This is a known issue.

    Parameters
    ----------
    `game_id` (int, mandatory):
        Required argument.
        Specifies the game you want team game stats from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import (
        get_baseball_game_team_stats
    )

    # Get the team game stats of the series winning game
    # of the 2024 NCAA D1 Baseball Championship.
    print(
        "Get the team game stats of the series winning game " +
        "of the 2024 NCAA D1 Baseball Championship."
    )
    df = get_baseball_game_team_stats(
        game_id=5336815
    )
    print(df)

    # Get the team game stats of a game
    # that occurred between Ball St.
    # and Lehigh on February 16th, 2024.
    print(
        "Get the team game stats of a game " +
        "that occurred between Ball St. " +
        "and Lehigh on February 16th, 2024."
    )
    df = get_baseball_game_team_stats(
        game_id=4525569
    )
    print(df)

    # Get the team game stats of a game
    # that occurred between Findlay
    # and Tiffin on April 10th, 2024.
    print(
        "Get the team game stats of a game " +
        "that occurred between Findlay " +
        "and Tiffin on April 10th, 2024."
    )
    df = get_baseball_game_team_stats(
        game_id=4546074
    )
    print(df)

    # Get the team game stats of a game
    # that occurred between Dean
    # and Anna Maria on March 30th, 2024.
    print(
        "Get the team game stats of a game " +
        "that occurred between Dean " +
        "and Anna Maria on March 30th, 2024."
    )
    df = get_baseball_game_team_stats(
        game_id=4543103
    )
    print(df)
    ```

    Returns
    ----------
    A pandas `DataFrame` object with a team game stats in a given game.

    """
    # home_dir = expanduser("~")
    # home_dir = _format_folder_str(home_dir)
    df = get_baseball_game_player_stats(game_id=game_id)

    final_df = df.groupby(
        ["sport_id", "game_id", "team_id"],
        as_index=False
    ).agg(
        {
            "batting_AB": "sum",
            "batting_R": "sum",
            "batting_H": "sum",
            "batting_2B": "sum",
            "batting_3B": "sum",
            "batting_TB": "sum",
            "batting_HR": "sum",
            "batting_RBI": "sum",
            "batting_BB": "sum",
            "batting_HBP": "sum",
            "batting_SF": "sum",
            "batting_SH": "sum",
            "batting_SO": "sum",
            "batting_OPP_DP": "sum",
            "batting_CS": "sum",
            "batting_PK": "sum",
            "batting_SB": "sum",
            "batting_IBB": "sum",
            "batting_KL": "sum",

            "pitching_IP": "sum",
            "pitching_H": "sum",
            "pitching_R": "sum",
            "pitching_ER": "sum",
            "pitching_BB": "sum",
            "pitching_SO": "sum",
            "pitching_BF": "sum",
            "pitching_2B": "sum",
            "pitching_3B": "sum",
            "pitching_BK": "sum",
            "pitching_HR": "sum",
            "pitching_WP": "sum",
            "pitching_HBP": "sum",
            "pitching_IBB": "sum",
            "pitching_IR": "sum",
            "pitching_IRS": "sum",
            "pitching_SH": "sum",
            "pitching_SF": "sum",
            "pitching_KL": "sum",
            "pitching_TUER": "sum",
            "pitching_PK": "sum",

            "fielding_PO": "sum",
            "fielding_A": "sum",
            "fielding_TC": "sum",
            "fielding_E": "sum",
            "fielding_CI": "sum",
            "fielding_PB": "sum",
            "fielding_SBA": "sum",
            "fielding_CSB": "sum",
            "fielding_IDP": "sum",
            "fielding_TP": "sum",
            # "fielding_SBA%": "sum",
        },
    )
    final_df.loc[
        (final_df["fielding_SBA"] > 0) | (final_df["fielding_CSB"] > 0),
        "fielding_SBA%"
    ] = round(
        final_df["fielding_SBA"] / (
            final_df["fielding_SBA"] + final_df["fielding_CSB"]
        )
    )
    return final_df


def get_raw_baseball_game_pbp(game_id: int):
    """
    Given a valid game ID,
    this function will attempt to get the raw play-by-play (PBP)
    data for that game.

    Long term goal is to get to something like this, but for college baseball:
    https://www.retrosheet.org/datause.html

    Parameters
    ----------
    `game_id` (int, mandatory):
        Required argument.
        Specifies the game you want play-by-play data (PBP) from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.baseball import (
        get_raw_baseball_game_pbp
    )

    # Get the raw play-by-play data of the series winning game
    # of the 2024 NCAA D1 Baseball Championship.
    print(
        "Get the raw play-by-play data " +
        "of the series winning game " +
        "in the 2024 NCAA D1 Baseball Championship."
    )
    df = get_raw_baseball_game_pbp(
        game_id=5336815
    )
    print(df)

    # Get the raw play-by-play data of a game
    # that occurred between Ball St.
    # and Lehigh on February 16th, 2024.
    print(
        "Get the raw play-by-play data of a game " +
        "that occurred between Ball St. " +
        "and Lehigh on February 16th, 2024."
    )
    df = get_raw_baseball_game_pbp(
        game_id=4525569
    )
    print(df)

    # Get the raw play-by-play data of a game
    # that occurred between Findlay
    # and Tiffin on April 10th, 2024.
    print(
        "Get the raw play-by-play data of a game " +
        "that occurred between Findlay " +
        "and Tiffin on April 10th, 2024."
    )
    df = get_raw_baseball_game_pbp(
        game_id=4546074
    )
    print(df)

    # Get the raw play-by-play data of a game
    # that occurred between Dean
    # and Anna Maria on March 30th, 2024.
    print(
        "Get the raw play-by-play data of a game " +
        "that occurred between Dean " +
        "and Anna Maria on March 30th, 2024."
    )
    df = get_raw_baseball_game_pbp(
        game_id=4543103
    )
    print(df)
    ```

    Returns
    ----------
    A pandas `DataFrame` object with a play-by-play (PBP) data in a given game.
    """
    sport_id = "MBA"
    load_from_cache = False

    pbp_df = pd.DataFrame()
    pbp_df_arr = []
    temp_df = pd.DataFrame()

    batting_team = 0

    url = f"https://stats.ncaa.org/contests/{game_id}/play_by_play"

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)
    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/")

    if exists(f"{home_dir}/.ncaa_stats_py/baseball/raw_pbp/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/baseball/raw_pbp/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/baseball/raw_pbp/"
        + f"{game_id}_raw_pbp.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/baseball/raw_pbp/"
            + f"{game_id}_raw_pbp.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/baseball/raw_pbp/"
                + f"{game_id}_raw_pbp.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if age.days > 365:
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
    season = game_datetime.year
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

    section_cards = soup.find_all(
        "div",
        {"class": "row justify-content-md-center w-100"}
    )

    for card in section_cards:
        top_bot = ""
        event_text = ""
        inning_str = card.find(
            "div",
            {"class": "card-header"}
        ).text
        inning_int = re.findall(
            r"([0-9]+)",
            inning_str
        )
        table_body = card.find("table").find("tbody").find_all("tr")

        for row in table_body:
            t_cells = row.find_all("td")
            t_cells = [x.text.strip() for x in t_cells]

            if len(t_cells[0]) > 0:
                top_bot = "top"
                batting_team = away_team_id
                event_text = t_cells[0]
            elif len(t_cells[2]) > 0:
                top_bot = "bot"
                batting_team = home_team_id
                event_text = t_cells[2]

            away_score, home_score = t_cells[1].split("-")

            away_score = int(away_score)
            home_score = int(home_score)

            temp_df = pd.DataFrame(
                {
                    "game_id": game_id,
                    "away_team_id": away_team_id,
                    "away_team_name": away_team_name,
                    "home_team_id": home_team_id,
                    "home_team_name": home_team_name,
                    "batting_team": batting_team,
                    "inning": inning_int,
                    "top_bot": top_bot,
                    "event_text": event_text,
                },
                index=[0],
            )
            pbp_df_arr.append(temp_df)

    pbp_df = pd.concat(pbp_df_arr, ignore_index=True)
    pbp_df["event_num"] = pbp_df.index + 1
    pbp_df["game_datetime"] = game_date_str
    pbp_df["stadium_name"] = stadium_str
    pbp_df["attendance"] = attendance_int
    pbp_df["sport_id"] = sport_id
    pbp_df["season"] = season

    pbp_df = pbp_df.infer_objects()
    pbp_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/baseball/raw_pbp/"
        + f"{game_id}_raw_pbp.csv",
        index=False
    )
    return pbp_df
