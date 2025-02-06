# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: `football.py`
# Purpose: Houses functions that allows one to access NCAA football data
# Creation Date: 2024-09-20 08:15 PM EDT
# Update History:
# - 2024-09-20 08:15 PM EDT
# - 2024-09-20 08:15 PM EDT
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
    _get_seconds_from_time_str,
    _get_stat_id,
    _get_webpage,
)


def get_football_teams(season: int, level: str | int) -> pd.DataFrame:
    """
    Retrieves a list of football teams from the NCAA.

    Parameters
    ----------
    `season` (int, mandatory):
        Required argument.
        Specifies the season you want NCAA football team information from.

    `level` (int, mandatory):
        Required argument.
        Specifies the level/division you want
        NCAA football team information from.
        This can either be an integer (1-3) or a string ("I"-"III").

    Usage
    ----------
    ```python

    from ncaa_stats_py.football import get_football_teams

    # Get all FBS football teams for the 2024 season.
    print("Get all FBS football teams for the 2024 season.")
    df = get_football_teams(2024, "FBS")
    print(df)

    # Get all FCS football teams for the 2024 season.
    print("Get all FCS football teams for the 2024 season.")
    df = get_football_teams(2024, "FCS")
    print(df)

    # Get all D2 football teams for the 2023 season.
    print("Get all D2 football teams for the 2023 season.")
    df = get_football_teams(2023, 2)
    print(df)

    # Get all D3 football teams for the 2022 season.
    print("Get all D3 football teams for the 2022 season.")
    df = get_football_teams(2022, 3)
    print(df)

    # Get all FBS football teams for the 2021 season.
    print("Get all FBS football teams for the 2021 season.")
    df = get_football_teams(2021, "FBS")
    print(df)

    # Get all FCS football teams for the 2021 season.
    print("Get all FCS football teams for the 2021 season.")
    df = get_football_teams(2021, "FCS")
    print(df)

    # Get all D2 football teams for the 2020 season.
    print("Get all D2 football teams for the 2020 season.")
    df = get_football_teams(2020, "II")
    print(df)

    # Get all D3 football teams for the 2019 season.
    print("Get all D3 football teams for the 2019 season.")
    df = get_football_teams(2019, "III")
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a list of college football teams
    in that season and NCAA level.
    """
    stat_sequence = 23
    sport_id = "MFB"
    now = datetime.today()
    # season -= 1

    load_from_cache = True
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)
    teams_df = pd.DataFrame()
    teams_df_arr = []
    temp_df = pd.DataFrame()
    team_ids_df = pd.DataFrame()
    formatted_level = ""
    ncaa_level = 0

    if isinstance(level, int) and level == 1:
        logging.warning(
            "For functions dealing with football, entering `1` or \"I\" " +
            "for `level` will only return FBS (previously D I-A) data. " +
            "If you want FCS (previously D I-AA) data, " +
            "pass through \"FCS\" for `level`."
        )
        formatted_level = "FBS"
        ncaa_level = 11
    elif isinstance(level, int) and level == 11:
        formatted_level = "FBS"
        ncaa_level = 11
    elif isinstance(level, int) and level == 12:
        formatted_level = "FCS"
        ncaa_level = 12
    elif isinstance(level, int) and level == 2:
        formatted_level = "II"
        ncaa_level = 2
    elif isinstance(level, int) and level == 3:
        formatted_level = "III"
        ncaa_level = 3
    elif isinstance(level, str) and (level.lower() == "fbs"):
        ncaa_level = 11
        formatted_level = "FBS"
    elif isinstance(level, str) and (level.lower() == "fcs"):
        ncaa_level = 12
        formatted_level = "FCS"
    elif isinstance(level, str) and (
        level.lower() == "i" or level.lower() == "d1" or level.lower() == "1"
    ):
        logging.warning(
            "For functions dealing with football, entering `1` or \"I\" " +
            "for `level` will only return FBS (previously D I-A) data. " +
            "If you want FCS (previously D I-AA) data, " +
            "pass through \"FCS\" for `level`."
        )
        ncaa_level = 11
        formatted_level = "FBS"
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

    if ncaa_level == 3 and season == 2009:
        return pd.DataFrame()
    elif (season + 1) > (now.year + 1):
        # This is empty, despite D3 football
        # existing at the D3 level since 1981 (kinda)
        return pd.DataFrame()

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/teams/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/teams/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/football/teams/"
        + f"{season}_{formatted_level}_teams.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/football/teams/"
            + f"{season}_{formatted_level}_teams.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/football/teams/"
                + f"{season}_{formatted_level}_teams.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    age = now - file_mod_datetime

    if age.days >= 14 and season >= (now.year - 1):
        load_from_cache = False

    if load_from_cache is True:
        return teams_df

    try:
        team_ids_df.read_csv(
            f"{home_dir}/.ncaa_stats_py/football/fb_team_abvs.csv"
        )
    except Exception:
        team_ids_df = pd.read_csv(
            "https://raw.githubusercontent.com/armstjc/ncaa_stats_py" +
            "/refs/heads/main/fb_team_abvs.csv"
        )
        team_ids_df.to_csv(
            f"{home_dir}/.ncaa_stats_py/football/fb_team_abvs.csv",
            index=False
        )

    team_ids_df.rename(
        columns={
            "NCAA ID": "school_id",
            "NFS Team Code": "nfs_team_code",
            "Club Code": "team_abv_1",
            "Club Code 2": "team_abv_2",
        },
        inplace=True
    )
    team_ids_df = team_ids_df[[
        "school_id",
        "nfs_team_code",
        "team_abv_1",
        "team_abv_2",
    ]]

    if ncaa_level < 4:
        logging.warning(
            f"Either we could not load {season} D{level} schools from cache, "
            + "or it's time to refresh the cached data."
        )
    else:
        logging.warning(
            f"Either we could not load {season} {formatted_level} "
            + "schools from cache, or it's time to refresh the cached data."
        )

    schools_df = _get_schools()
    url = (
        "https://stats.ncaa.org/rankings/change_sport_year_div?"
        + f"academic_year={season+1}.0&division={ncaa_level}.0"
        + f"&sport_code={sport_id}"
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
            if "championship" in rp.text.lower():
                pass
            elif "final" in rp.text.lower():
                rp_value = rp.get("value")
                found_value = True
                break
            # elif "-" in rp.text:
            #     pass
            else:
                rp_value = rp.get("value")
                found_value = True
                break

    url = (
        "https://stats.ncaa.org/rankings/institution_trends?"
        + f"academic_year={season+1}.0&division={ncaa_level}.0&"
        + f"ranking_period={rp_value}&sport_code={sport_id}"
        + f"&stat_seq={stat_sequence}"
    )

    best_method = True
    if season < 2013:
        url = (
            "https://stats.ncaa.org/rankings/national_ranking?"
            + f"academic_year={season+1}.0&division={ncaa_level}.0&"
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
                + f"academic_year={season+1}.0&division={ncaa_level}.0&"
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
    teams_df["school_name"] = teams_df["school_name"].str.replace(
        "Saint Francis (PA)",
        "Saint Francis"
    )
    teams_df["school_name"] = teams_df["school_name"].str.replace(
        "Saint Francis ",
        "Saint Francis"
    )
    teams_df["school_name"] = teams_df["school_name"].str.replace(
        "Tex. A&M-Commerce", "East Texas A&M"
    )
    teams_df = pd.merge(
        left=teams_df,
        right=schools_df,
        on=["school_name"],
        how="left"
    )
    teams_df.loc[teams_df["school_name"] == "Saint Francis", "school_id"] = 600
    teams_df.sort_values(by=["team_id"], inplace=True)

    teams_df = pd.merge(
        teams_df,
        team_ids_df,
        on="school_id",
        how="left"
    )

    teams_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/football/teams/"
        + f"{season}_{formatted_level}_teams.csv",
        index=False,
    )

    return teams_df


def load_football_teams(start_year: int = 2013) -> pd.DataFrame:
    """
    Compiles a list of known NCAA football teams
    in NCAA football history.

    Parameters
    ----------
    `start_year` (int, optional):
        Optional argument.
        Specifies the first season you want
        NCAA football team information from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.football import load_football_teams

    # Compile a list of known football teams
    # in NCAA football history.
    print(
        "Compile a list of known football teams " +
        "in NCAA football history."
    )
    df = load_football_teams()
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a list of
    all known college football teams.

    """
    # start_year = 2008
    #

    teams_df = pd.DataFrame()
    teams_df_arr = []
    temp_df = pd.DataFrame()

    now = datetime.now()
    ncaa_divisions = ["FBS", "FCS", "II", "III"]
    ncaa_seasons = [x for x in range(start_year, (now.year + 1))]

    logging.info(
        "Loading in all NCAA football teams. "
        + "If this is the first time you're seeing this message, "
        + "it may take some time (3-10 minutes) for this to load."
    )
    for s in ncaa_seasons:
        logging.info(f"Loading in football teams for the {s} season.")
        for d in ncaa_divisions:
            temp_df = get_football_teams(season=s, level=d)
            teams_df_arr.append(temp_df)
            del temp_df

    teams_df = pd.concat(teams_df_arr, ignore_index=True)
    return teams_df


def get_football_team_schedule(team_id: int) -> pd.DataFrame:
    """
    Retrieves a team schedule, from a valid NCAA football team ID.

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

    from ncaa_stats_py.football import get_football_team_schedule

    # Get the team schedule for the
    # 2024 Ball St. football team (D1, FBS, ID: 589028).
    print(
        "Get the team schedule for the " +
        "2024 Ball St. football team (D1, FBS, ID: 589028)."
    )
    df = get_football_team_schedule(589028)
    print(df)

    # Get the team schedule for the
    # 2024 Mercer football team (D1, ID: 589126).
    print(
        "Get the team schedule for the " +
        "2024 Mercer football team (D1, ID: 589126)."
    )
    df = get_football_team_schedule(589126)
    print(df)

    # Get the team schedule for the
    # 2023 Tiffin football team (D2, ID: 558009).
    print(
        "Get the team schedule for the " +
        "2023 Tiffin football team (D2, ID: 558009)."
    )
    df = get_football_team_schedule(558009)
    print(df)

    # Get the team schedule for the
    # 2022 Bridgewater St. football team (D3, ID: 545196).
    print(
        "Get the team schedule for the " +
        "2022 Bridgewater St. football team (D3, ID: 545196)."
    )
    df = get_football_team_schedule(545196)
    print(df)

    # Get the team schedule for the
    # 2021 Hawaii football team (D1, ID: 522954).
    print(
        "Get the team schedule for the " +
        "2021 Hawaii football team (D1, ID: 522954)."
    )
    df = get_football_team_schedule(522954)
    print(df)

    # Get the team schedule for the
    # 2021 Idaho football team (D1, ID: 523112).
    print(
        "Get the team schedule for the " +
        "2021 Idaho football team (D1, ID: 523112)."
    )
    df = get_football_team_schedule(523112)
    print(df)

    # Get the team schedule for the
    # 2020 Barton football team (D2, ID: 505390).
    print(
        "Get the team schedule for the " +
        "2020 Barton football team (D2, ID: 505390)."
    )
    df = get_football_team_schedule(505390)
    print(df)

    # Get the team schedule for the
    # 2019 Beloit football team (D3, ID: 478427).
    print(
        "Get the team schedule for the " +
        "2019 Beloit football team (D3, ID: 478427)."
    )
    df = get_football_team_schedule(478427)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with an NCAA football team's schedule.
    """
    sport_id = "MFB"
    schools_df = _get_schools()
    temp_df = pd.DataFrame()
    games_df = pd.DataFrame()
    games_df_arr = []
    season = 0
    load_from_cache = True

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    url = f"https://stats.ncaa.org/teams/{team_id}"

    team_df = load_football_teams()
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

    if exists(f"{home_dir}/.ncaa_stats_py/football/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/team_schedule/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/team_schedule/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/football/team_schedule/"
        + f"{team_id}_team_schedule.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/football/team_schedule/"
            + f"{team_id}_team_schedule.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/"
                + "football/team_schedule/"
                + f"{team_id}_team_schedule.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime
    if age.days >= 1 and season >= now.year and now.month <= 7:
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
    # For NCAA football, the season always starts in the fall semester,
    # and ends in the spring semester.
    # Thus, if `season_name` = "2011-12",
    # this is the "2012" football season,
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
    games_df["sport_id"] = sport_id
    # games_df["game_url"] = games_df["game_url"].str.replace("/box_score", "")
    games_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/"
        + "football/team_schedule/"
        + f"{team_id}_team_schedule.csv",
        index=False,
    )

    return games_df


def get_football_day_schedule(
    game_date: str | date | datetime,
    level: str | int = "FBS",
):
    """
    Given a date and NCAA level,
    this function retrieves football every game for that date.

    Parameters
    ----------
    `game_date` (int, mandatory):
        Required argument.
        Specifies the date you want a football schedule from.
        For best results, pass a string formatted as "YYYY-MM-DD".

    `level` (int, mandatory):
        Required argument.
        Specifies the level/division you want a
        NCAA football schedule from.
        This can either be an integer (1-3) or a string ("I"-"III").

    Usage
    ----------
    ```python

    from ncaa_stats_py.football import get_football_day_schedule


    # Get all DI games that were played on November 24th, 2024.
    print("Get all games that were played on November 24th, 2024.")
    df = get_football_day_schedule("2024-11-24", level=1)
    print(df)

    # Get all division II games that were played on October 14th, 2024.
    print("Get all division II games that were played on October 14th, 2024.")
    df = get_football_day_schedule("2024-10-14", level="I")
    print(df)

    # Get all DI games that were played on September 7th, 2024.
    print("Get all DI games that were played on September 7th, 2024.")
    df = get_football_day_schedule("2024-09-07")
    print(df)

    # Get all DII games that were played on October 24th, 2023.
    print("Get all games DII that were played on October 24th, 2023.")
    df = get_football_day_schedule("2023-10-24", level="II")
    print(df)

    # Get all DII games played on September 14th, 2024.
    print("Get all DII games played on September 14th, 2024.")
    df = get_football_day_schedule("2023-01-14", level="III")
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with all football games played on that day,
    for that NCAA division/level.

    """

    season = 0
    sport_id = "MFB"

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
        logging.warning(
            "For functions dealing with football, entering `1` or \"I\" " +
            "for `level` will only return FBS (previously D I-A) data. " +
            "If you want FCS (previously D I-AA) data, " +
            "pass through \"FCS\" for `level`."
        )
        formatted_level = "FBS"
        ncaa_level = 11
    elif isinstance(level, int) and level == 11:
        formatted_level = "FBS"
        ncaa_level = 11
    elif isinstance(level, int) and level == 12:
        formatted_level = "FCS"
        ncaa_level = 12
    elif isinstance(level, int) and level == 2:
        formatted_level = "II"
        ncaa_level = 2
    elif isinstance(level, int) and level == 3:
        formatted_level = "III"
        ncaa_level = 3
    elif isinstance(level, str) and (level.lower() == "fbs"):
        ncaa_level = 11
        formatted_level = "FBS"
    elif isinstance(level, str) and (level.lower() == "fcs"):
        ncaa_level = 12
        formatted_level = "FCS"
    elif isinstance(level, str) and (
        level.lower() == "i" or level.lower() == "d1" or level.lower() == "1"
    ):
        logging.warning(
            "For functions dealing with football, entering `1` or \"I\" " +
            "for `level` will only return FBS (previously D I-A) data. " +
            "If you want FCS (previously D I-AA) data, " +
            "pass through \"FCS\" for `level`."
        )
        ncaa_level = 11
        formatted_level = "FBS"
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
        f"&academic_year={season+1}&division={ncaa_level}" +
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

        if "Canceled" in td_arr[5].text:
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

        away_goals_scored = td_arr[-1].text
        away_goals_scored = away_goals_scored.replace("\n", "")
        away_goals_scored = away_goals_scored.replace("\xa0", "")
        if "canceled" in away_goals_scored.lower():
            continue
        elif "ppd" in away_goals_scored.lower():
            continue
        if len(away_goals_scored) > 0:
            away_goals_scored = int(away_goals_scored)
        else:
            away_goals_scored = 0

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

        home_goals_scored = td_arr[-1].text
        home_goals_scored = home_goals_scored.replace("\n", "")
        if len(home_goals_scored) > 0:
            home_goals_scored = int(home_goals_scored)
        else:
            home_goals_scored = 0

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
                "home_goals_scored": home_goals_scored,
                "away_goals_scored": away_goals_scored,
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


def get_full_football_schedule(
    season: int,
    level: str | int = "FBS"
) -> pd.DataFrame:
    """
    Retrieves a full football schedule,
    from an NCAA level (`"FBS"`, `"FCS"`, `"II"`, `"III"`).
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

    from ncaa_stats_py.football import get_full_football_schedule

    # Get the entire 2024 schedule for the 2024 DII football season.
    print("Get the entire 2024 schedule for the 2024 DII football season.")
    df = get_full_football_schedule(season=2024, level="II")
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
    df = get_full_football_schedule(season=2024, level=2)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with an NCAA football
    schedule for a specific season and level.
    """

    sport_id = "MFB"
    load_from_cache = True
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)
    schedule_df = pd.DataFrame()
    schedule_df_arr = []
    temp_df = pd.DataFrame()
    formatted_level = ""
    ncaa_level = 0

    if isinstance(level, int) and level == 1:
        logging.warning(
            "For functions dealing with football, entering `1` or \"I\" " +
            "for `level` will only return FBS (previously D I-A) data. " +
            "If you want FCS (previously D I-AA) data, " +
            "pass through \"FCS\" for `level`."
        )
        formatted_level = "FBS"
        ncaa_level = 11
    elif isinstance(level, int) and level == 11:
        formatted_level = "FBS"
        ncaa_level = 11
    elif isinstance(level, int) and level == 12:
        formatted_level = "FCS"
        ncaa_level = 12
    elif isinstance(level, int) and level == 2:
        formatted_level = "II"
        ncaa_level = 2
    elif isinstance(level, int) and level == 3:
        formatted_level = "III"
        ncaa_level = 3
    elif isinstance(level, str) and (level.lower() == "fbs"):
        ncaa_level = 11
        formatted_level = "FBS"
    elif isinstance(level, str) and (level.lower() == "fcs"):
        ncaa_level = 12
        formatted_level = "FCS"
    elif isinstance(level, str) and (
        level.lower() == "i" or level.lower() == "d1" or level.lower() == "1"
    ):
        logging.warning(
            "For functions dealing with football, entering `1` or \"I\" " +
            "for `level` will only return FBS (previously D I-A) data. " +
            "If you want FCS (previously D I-AA) data, " +
            "pass through \"FCS\" for `level`."
        )
        ncaa_level = 11
        formatted_level = "FBS"
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

    if exists(f"{home_dir}/.ncaa_stats_py/football/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/")

    if exists(f"{home_dir}/.ncaa_stats_py/" + "football/full_schedule/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/" + "football/full_schedule/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/football/full_schedule/"
        + f"{season}_{formatted_level}_full_schedule.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/football/full_schedule/"
            + f"{season}_{formatted_level}_full_schedule.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/"
                + "football/full_schedule/"
                + f"{season}_{formatted_level}_full_schedule.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if age.days >= 1 and season >= now.year and now.month <= 7:
        load_from_cache = False

    if load_from_cache is True:
        return teams_df

    teams_df = load_football_teams()
    teams_df = teams_df[
        (teams_df["season"] == season) &
        (teams_df["ncaa_division"] == ncaa_level)
    ]
    team_ids_arr = teams_df["team_id"].to_numpy()

    for team_id in tqdm(team_ids_arr):
        temp_df = get_football_team_schedule(team_id=team_id)
        schedule_df_arr.append(temp_df)

    schedule_df = pd.concat(schedule_df_arr, ignore_index=True)
    schedule_df = schedule_df.drop_duplicates(subset="game_id", keep="first")
    schedule_df["sport_id"] = sport_id
    schedule_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/"
        + "football/full_schedule/"
        + f"{season}_{formatted_level}_full_schedule.csv",
        index=False,
    )
    return schedule_df


def get_football_team_roster(team_id: int) -> pd.DataFrame:
    """
    Retrieves a football team's roster from a given team ID.

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

    from ncaa_stats_py.football import get_football_team_roster

    # Get the roster of the 2024 UTSA football team (D1, FBS, ID: 588997).
    print(
        "Get the roster of the 2024 UTSA " +
        "football team (D1, FBS, ID: 588997)."
    )
    df = get_football_team_roster(team_id=588997)
    print(df)

    # Get the roster of the 2024 Dayton football team (D1, FCS, ID: 589097).
    print(
        "Get the roster of the 2024 Dayton " +
        "football team (D1, FCS, ID: 589097)."
    )
    df = get_football_team_roster(team_id=589097)
    print(df)

    # Get the roster of the 2023 Slippery Rock
    # football team (D2, ID: 557966).
    print(
        "Get the roster of the 2023 Slippery Rock " +
        "football team (D2, ID: 557966)."
    )
    df = get_football_team_roster(team_id=557966)
    print(df)

    # Get the roster of the 2022 Misericordia football team (D3, ID: 545159).
    print(
        "Get the roster of the " +
        "2022 Misericordia football team (D3, ID: 545159)."
    )
    df = get_football_team_roster(team_id=545159)
    print(df)

    # Get the roster of the 2021 Troy football team (D1, FBS, ID: 522801).
    print(
        "Get the roster of the 2021 Troy " +
        "football team (D1, FBS, ID: 522801)."
    )
    df = get_football_team_roster(team_id=522801)
    print(df)

    # Get the roster of the 2021 Portland St. football team
    # (D1, FCS, ID: 523346).
    print(
        "Get the roster of the 2021 Portland St. " +
        "football team (D1, FCS, ID: 523346)."
    )
    df = get_football_team_roster(team_id=523346)
    print(df)

    # Get the roster of the 2020 Lincoln (MO)
    # football team (D2, ID: 499833).
    print(
        "Get the roster of the 2020 Lincoln (MO) " +
        "football team (D2, ID: 499833)."
    )
    df = get_football_team_roster(team_id=499833)
    print(df)

    # Get the roster of the 2019 Wash. & Lee football team (D3, ID: 478577).
    print(
        "Get the roster of the 2019 Wash. & Lee " +
        "football team (D3, ID: 478577)."
    )
    df = get_football_team_roster(team_id=478577)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with
    an NCAA football team's roster for that season.
    """
    sport_id = "MFB"
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
        "player_stat_crew_num",
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

    team_df = load_football_teams()

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

    if exists(f"{home_dir}/.ncaa_stats_py/football/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/rosters/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/rosters/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/football/rosters/" +
        f"{team_id}_roster.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/football/rosters/" +
            f"{team_id}_roster.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/football/rosters/"
                + f"{team_id}_roster.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if age.days >= 14 and season >= now.year and now.month <= 7:
        load_from_cache = False

    if load_from_cache is True:
        return teams_df

    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")
    try:
        school_name = soup.find("div", {"class": "card"}).find("img").get(
            "alt"
        )
    except Exception:
        school_name = soup.find("div", {"class": "card"}).find("a").text
        school_name = school_name.rsplit(" ", maxsplit=1)[0]

    season_name = (
        soup.find("select", {"id": "year_list"})
        .find("option", {"selected": "selected"})
        .text
    )
    # For NCAA football, the season always starts in the spring semester,
    # and ends in the fall semester.
    # Thus, if `season_name` = "2011-12",
    # this is the "2012" football season,
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
            "StatCrew #": "player_stat_crew_num",
            "Name": "player_full_name",
            "Class": "player_class",
            "Position": "player_positions",
            "Height": "player_height_string",
            "Weight": "player_weight",
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
        f"{home_dir}/.ncaa_stats_py/football/rosters/" +
        f"{team_id}_roster.csv",
        index=False,
    )
    return roster_df


def get_football_player_season_stats(
    team_id: int,
) -> pd.DataFrame:
    """
    Given a team ID, this function retrieves and parses
    the season stats for all of the players in a given football team.

    Parameters
    ----------
    `team_id` (int, mandatory):
        Required argument.
        Specifies the team you want stats from.
        This is separate from a school ID, which identifies the institution.
        A team ID should be unique to a school, and a season.

    Usage
    ----------
    ```python

    ```

    Returns
    ----------
    A pandas `DataFrame` object with the season stats for
    all players with a given NCAA football team.
    """
    sport_id = "MFB"
    load_from_cache = True
    gk_df = pd.DataFrame()
    gk_df_arr = []
    players_df = pd.DataFrame()
    players_df_arr = []
    stats_df = pd.DataFrame()
    # stats_df_arr = []
    temp_df = pd.DataFrame()

    schedule_df = get_football_team_schedule(team_id=team_id)

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/player_season_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/player_season_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/football/player_season_stats/"
        + f"{team_id:00d}_player_season_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/football/player_season_stats/"
            + f"{team_id:00d}_player_season_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/football/"
                + "player_season_stats/"
                + f"{team_id:00d}_player_season_stats.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if age.days >= 1:
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    raise NotImplementedError(
        "It's not ready yet!"
    )

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/football/"
        + "player_season_stats/"
        + f"{team_id:00d}_player_season_stats.csv",
        index=False,
    )

    return stats_df


def get_football_player_game_stats(
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


    ```

    Returns
    ----------
    A pandas `DataFrame` object with a player's batting game logs
    in a given season.
    """
    # sport_id = "MFB"

    # stat_columns = []
    load_from_cache = True

    stats_df = pd.DataFrame()
    stats_df_arr = []
    init_df = pd.DataFrame()
    init_df_arr = []
    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    url = f"https://stats.ncaa.org/players/{player_id}"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/player_game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/player_game_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/football/player_game_stats/"
        + f"{season}_{player_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/football/player_game_stats/"
            + f"{season}_{player_id}_player_game_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/football/"
                + "player_game_stats/"
                + f"{season}_{player_id}_player_game_stats.csv"
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
        age.days >= 1 and
        (season - 1) >= now.year
    ):
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    response = _get_webpage(url=url)
    soup = BeautifulSoup(response.text, features="lxml")

    # table_navigation = soup.find("ul", {"class": "nav nav-tabs padding-nav"})
    # table_nav_card = table_navigation.find_all("a")

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

        init_df_arr.append(temp_df)
        del temp_df

    init_df = pd.concat(init_df_arr, ignore_index=True)
    init_df = init_df.replace("/", "", regex=True)
    init_df = init_df.replace("", np.nan)
    init_df = init_df.infer_objects()

    # print(stats_df)
    init_df["GP"] = init_df["GP"].fillna("0")
    init_df = init_df.astype(
        {"GP": "uint8"}
    )
    init_df = init_df[init_df["GP"] == 1]

    game_ids_arr = init_df["game_id"].to_numpy()
    # time.sleep(2)

    print(f"Loading for player game stats for player ID `{player_id}`")
    count = 0
    game_ids_len = len(game_ids_arr)
    for game_id in game_ids_arr:
        count += 1
        print(f"On game {count} of {game_ids_len}")
        df = get_football_game_player_stats(game_id)
        stats_df_arr.append(df)
        del df

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    stats_df = stats_df[stats_df["player_id"] == player_id]

    raise NotImplementedError(
        "It's not ready yet!"
    )

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/football/player_game_stats/"
        + f"{season}_{player_id}_player_game_stats.csv",
        index=False
    )
    return stats_df


def get_football_game_player_stats(game_id: int) -> pd.DataFrame:
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

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a player game stats in a given game.

    """
    sport_id = "MFB"
    load_from_cache = True
    season = 0
    team_df = load_football_teams()

    stats_df = pd.DataFrame()

    players_df = pd.DataFrame()
    players_df_arr = []

    gk_df = pd.DataFrame()
    gk_df_arr = []

    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    stat_columns = [
    ]

    url = f"https://stats.ncaa.org/contests/{game_id}/individual_stats"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/game_stats/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/game_stats/player/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/game_stats/player/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/football/game_stats/player/"
        + f"{game_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/football/game_stats/player/"
            + f"{game_id}_player_game_stats.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/football/game_stats/player/"
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

    raise NotImplementedError(
        "It's not ready yet."
    )

    stats_df["game_id"] = game_id
    stats_df["sport_id"] = sport_id
    for i in stats_df.columns:
        if i in stat_columns:
            pass
        else:
            raise ValueError(f"Unhandled column name {i}")

    stats_df = stats_df.reindex(columns=stat_columns)

    # print(stats_df.columns)

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/football/game_stats/player/"
        + f"{game_id}_player_game_stats.csv",
        index=False
    )
    return stats_df


def get_football_raw_pbp(game_id: int) -> pd.DataFrame:
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

    from ncaa_stats_py.football import get_football_raw_pbp


    # Get the play-by-play data of the
    # 2025 NCAA FBS National Championship game.
    print(
        "Get the play-by-play data of the "
        + "2025 NCAA FBS National Championship game."
    )
    df = get_football_raw_pbp(6081029)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a play-by-play (PBP) data in a given game.

    """
    load_from_cache = True
    is_overtime = False

    sport_id = "MFB"
    season = 0
    away_score = 0
    home_score = 0

    quarter_seconds_remaining = 0
    half_seconds_remaining = 0
    game_seconds_remaining = 0

    pbp_df = pd.DataFrame()
    pbp_df_arr = []

    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    stat_columns = [
        "season",
        "sport_id",
        "game_id",
        "game_time_str",
        "quarter_seconds_remaining",
        "half_seconds_remaining",
        "game_seconds_remaining",
        "quarter_num",
        "event_team",
        "event_text",
        "is_overtime",
        "event_num",
        "game_datetime",
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

    if exists(f"{home_dir}/.ncaa_stats_py/football/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/")

    if exists(f"{home_dir}/.ncaa_stats_py/football/raw_pbp/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/football/raw_pbp/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/football/raw_pbp/"
        + f"{game_id}_raw_pbp.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/football/raw_pbp/"
            + f"{game_id}_raw_pbp.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/football/raw_pbp/"
                + f"{game_id}_raw_pbp.csv"
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
    elif ":" not in game_date_str:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y')
    else:
        game_datetime = datetime.strptime(game_date_str, '%m/%d/%Y %I:%M %p')
    game_datetime = game_datetime.astimezone(timezone("US/Eastern"))
    game_date_str = game_datetime.isoformat()

    if game_datetime.year == 2021 and game_datetime.month < 8:
        season = 2020
    else:
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

    quarter_boxes_arr = soup.find(
        "div", {"style": "width: 50%; margin-left: auto; margin-right: auto;"}
    )
    quarter_boxes_arr = quarter_boxes_arr.find_all(
        "div",
        {
            "class": "drives"
        }
    )

    for i in range(0, len(quarter_boxes_arr)):
        dp_pointer = 0
        quarter_num = i + 1
        q_box = quarter_boxes_arr[i]
        drive_header_box = q_box.find_all(
            "h5", class_=re.compile("scoring_play")
        )
        drive_plays_arr = q_box.find_all(
            "div", class_=re.compile("scoring_play")
        )

        for d in range(0, len(drive_header_box)):
            drive_num = d + 1

            possession_team = None
            defensive_team = None

            drive_team = drive_header_box[d].find(
                "img"
            ).get("alt")

            if drive_team in away_team_name:
                possession_team = away_team_id
                defensive_team = home_team_id
                posteam_type = "away"
            elif drive_team in home_team_name:
                possession_team = home_team_id
                defensive_team = away_team_id
                posteam_type = "home"
            else:
                raise ValueError(
                    f"Unhandled possession team: `{drive_team}`"
                )

            drive_str = drive_header_box[d].find_all(
                "div", {"class": "headerLeft"}
            )[1].text
            drive_str = drive_str.replace("\n", "")
            score_str = drive_header_box[d].find(
                "div", {"class": "headerRight"}
            ).text

            temp_plays_arr = drive_plays_arr[dp_pointer].find_all(
                "div", {"style": "border-bottom: 1px dotted #dcdddf;"}
            )

            if len(temp_plays_arr) == 0:
                while len(temp_plays_arr) == 0:
                    temp_plays_arr = drive_plays_arr[dp_pointer].find_all(
                        "div", {"style": "border-bottom: 1px dotted #dcdddf;"}
                    )
                    dp_pointer += 1

            for p in range(0, len(temp_plays_arr)):
                temp_play = temp_plays_arr[p]
                temp_play = temp_play.find_all("span")

                play_text = temp_play[-1].text
                play_text = play_text.replace("\n", "")
                play_text = play_text.strip()

                time_str = re.findall(
                    r"([0-9\:]+)",
                    play_text
                )

                if len(time_str) == 0:
                    time_str = None
                elif len(time_str) >= 4:
                    pass
                else:
                    time_str = time_str[0]

                if (
                    "start of" in play_text.lower() and
                    "quarter" in play_text.lower()
                ):
                    play_arr = re.findall(
                        r"start of ([a-zA-Z0-9]+) quarter, " +
                        r"clock ([0-9\:]+)\.?",
                        play_text.lower()
                    )
                    quarter_str = play_arr[0][0]
                    if "ot" in quarter_str.lower():
                        quarter_num = 5
                    else:
                        quarter_num = int(quarter_str[0])
                play_d_and_d = temp_play[-2].text
                play_d_and_d = play_d_and_d.replace("\n", "")
                play_d_and_d = play_d_and_d.strip()

                d_and_d_arr = re.findall(
                    r"([0-9]+)[a-zA-Z]+ \& ([0-9]+) at ([a-zA-Z0-9]+)",
                    play_d_and_d
                )
                down = d_and_d_arr[0][0]
                distance = d_and_d_arr[0][1]
                yrdln = d_and_d_arr[0][2]

                temp_df = pd.DataFrame(
                    {
                        
                    },
                    index=[0]
                )

    raise NotImplementedError(
        "It's in progress, but raw PBP data isn't ready yet."
    )
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
    pbp_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/football/raw_pbp/"
        + f"{game_id}_raw_pbp.csv",
        index=False
    )
    return pbp_df
