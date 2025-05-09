# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: `field_hockey.py`
# Purpose: Houses functions that allows one to access NCAA field hockey data
# Creation Date: 2024-09-20 08:15 PM EDT
# Update History:
# - 2024-09-20 08:15 PM EDT
# - 2024-11-08 08:15 PM EST
# - 2024-11-25 07:45 PM EDT
# - 2024-11-25 08:20 PM EDT
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
    _get_seconds_from_time_str,
    _get_stat_id,
    _get_webpage,
)


def get_field_hockey_teams(season: int, level: str | int) -> pd.DataFrame:
    """
    Retrieves a list of field hockey teams from the NCAA.

    Parameters
    ----------
    `season` (int, mandatory):
        Required argument.
        Specifies the season you want NCAA field hockey team information from.

    `level` (int, mandatory):
        Required argument.
        Specifies the level/division you want
        NCAA field hockey team information from.
        This can either be an integer (1-3) or a string ("I"-"III").

    Usage
    ----------
    ```python

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a list of college field hockey teams
    in that season and NCAA level.
    """
    stat_sequence = 450
    sport_id = "WFH"
    now = datetime.today()
    # season -= 1

    load_from_cache = True
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

    if ncaa_level == 3 and season == 2009:
        return pd.DataFrame()
    elif (season + 1) > (now.year + 1):
        # This is empty, despite D3 field hockey
        # existing at the D3 level since 1981 (kinda)
        return pd.DataFrame()

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/")

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/teams/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/teams/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/field_hockey/teams/"
        + f"{season}_{formatted_level}_teams.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/field_hockey/teams/"
            + f"{season}_{formatted_level}_teams.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/field_hockey/teams/"
                + f"{season}_{formatted_level}_teams.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

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
    teams_df = pd.merge(
        left=teams_df,
        right=schools_df,
        on=["school_name"],
        how="left"
    )
    teams_df.sort_values(by=["team_id"], inplace=True)

    teams_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/field_hockey/teams/"
        + f"{season}_{formatted_level}_teams.csv",
        index=False,
    )

    return teams_df


def load_field_hockey_teams(start_year: int = 2009) -> pd.DataFrame:
    """
    Compiles a list of known NCAA field hockey teams
    in NCAA field hockey history.

    Parameters
    ----------
    `start_year` (int, optional):
        Optional argument.
        Specifies the first season you want
        NCAA field hockey team information from.

    Usage
    ----------
    ```python

    from ncaa_stats_py.field_hockey import load_field_hockey_teams

    # Compile a list of known field hockey teams
    # in NCAA field hockey history.
    print(
        "Compile a list of known field hockey teams " +
        "in NCAA field hockey history."
    )
    df = load_field_hockey_teams()
    print(df)
    ```

    Returns
    ----------
    A pandas `DataFrame` object with a list of
    all known college field hockey teams.

    """
    # start_year = 2008
    #

    teams_df = pd.DataFrame()
    teams_df_arr = []
    temp_df = pd.DataFrame()

    now = datetime.now()
    ncaa_divisions = ["I", "II", "III"]
    ncaa_seasons = [x for x in range(start_year, (now.year + 1))]

    logging.info(
        "Loading in all NCAA field hockey teams. "
        + "If this is the first time you're seeing this message, "
        + "it may take some time (3-10 minutes) for this to load."
    )
    for s in ncaa_seasons:
        logging.info(f"Loading in field hockey teams for the {s} season.")
        for d in ncaa_divisions:
            try:
                temp_df = get_field_hockey_teams(season=s, level=d)
                teams_df_arr.append(temp_df)
                del temp_df
            except Exception as e:
                logging.warning(
                    "Unhandled exception when trying to " +
                    f"get the teams. Full exception: `{e}`"
                )

    teams_df = pd.concat(teams_df_arr, ignore_index=True)
    return teams_df


def get_field_hockey_team_schedule(team_id: int) -> pd.DataFrame:
    """
    Retrieves a team schedule, from a valid NCAA field hockey team ID.

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

    from ncaa_stats_py.field_hockey import get_field_hockey_team_schedule

    # Get the team schedule for the
    # 2024 Ohio Bobcats field hockey team (D1, ID: 586476).
    print(
        "Get the team schedule for the " +
        "2024 Ohio Bobcats field hockey team (D1, ID: 586476)."
    )
    df = get_field_hockey_team_schedule(586476)
    print(df)

    # Get the team schedule for the
    # 2023 Limestone field hockey team (D2, ID: 557419).
    print(
        "Get the team schedule for the " +
        "2023 Limestone field hockey team (D2, ID: 557419)."
    )
    df = get_field_hockey_team_schedule(557419)
    print(df)

    # Get the team schedule for the
    # 2022 Randolph-Macon field hockey team (D3, ID: 539636).
    print(
        "Get the team schedule for the " +
        "2022 Randolph-Macon field hockey team (D3, ID: 539636)."
    )
    df = get_field_hockey_team_schedule(539636)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with an NCAA field hockey team's schedule.
    """
    sport_id = "WFH"
    schools_df = _get_schools()
    temp_df = pd.DataFrame()
    games_df = pd.DataFrame()
    games_df_arr = []
    season = 0
    load_from_cache = True

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    url = f"https://stats.ncaa.org/teams/{team_id}"

    team_df = load_field_hockey_teams()
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

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/")

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/team_schedule/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/team_schedule/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/field_hockey/team_schedule/"
        + f"{team_id}_team_schedule.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/field_hockey/team_schedule/"
            + f"{team_id}_team_schedule.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/"
                + "field_hockey/team_schedule/"
                + f"{team_id}_team_schedule.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime
    if age.days > 1 and season >= now.year and now.month <= 7:
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
    # For NCAA field_hockey, the season always starts in the fall semester,
    # and ends in the spring semester.
    # Thus, if `season_name` = "2011-12",
    # this is the "2012" field_hockey season,
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
        + "field_hockey/team_schedule/"
        + f"{team_id}_team_schedule.csv",
        index=False,
    )

    return games_df


def get_field_hockey_day_schedule(
    game_date: str | date | datetime,
    level: str | int = "I",
):
    """
    Given a date and NCAA level,
    this function retrieves field hockey every game for that date.

    Parameters
    ----------
    `game_date` (int, mandatory):
        Required argument.
        Specifies the date you want a field hockey schedule from.
        For best results, pass a string formatted as "YYYY-MM-DD".

    `level` (int, mandatory):
        Required argument.
        Specifies the level/division you want a
        NCAA field hockey schedule from.
        This can either be an integer (1-3) or a string ("I"-"III").

    Usage
    ----------
    ```python

    from ncaa_stats_py.field_hockey import get_field_hockey_day_schedule


    # Get all DI games that were played on November 24th, 2024.
    print("Get all games that were played on November 24th, 2024.")
    df = get_field_hockey_day_schedule("2024-11-24", level=1)
    print(df)

    # Get all division II games that were played on October 14th, 2024.
    print("Get all division II games that were played on October 14th, 2024.")
    df = get_field_hockey_day_schedule("2024-10-14", level="I")
    print(df)

    # Get all DI games that were played on September 7th, 2024.
    print("Get all DI games that were played on September 7th, 2024.")
    df = get_field_hockey_day_schedule("2024-09-07")
    print(df)

    # Get all DII games that were played on October 24th, 2023.
    print("Get all games DII that were played on October 24th, 2023.")
    df = get_field_hockey_day_schedule("2023-10-24", level="II")
    print(df)

    # Get all DII games played on September 14th, 2024.
    print("Get all DII games played on September 14th, 2024.")
    df = get_field_hockey_day_schedule("2023-01-14", level="III")
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with all field hockey games played on that day,
    for that NCAA division/level.

    """

    season = 0
    sport_id = "WFH"

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


def get_full_field_hockey_schedule(
    season: int,
    level: str | int = "I"
) -> pd.DataFrame:
    """
    Retrieves a full field hockey schedule,
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

    from ncaa_stats_py.field_hockey import get_full_field_hockey_schedule

    # Get the entire 2024 schedule for the 2024 D1 field hockey season.
    print("Get the entire 2024 schedule for the 2024 D1 field hockey season.")
    df = get_full_field_hockey_schedule(season=2024, level="I")
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
    df = get_full_field_hockey_schedule(season=2024, level=1)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with an NCAA field hockey
    schedule for a specific season and level.
    """

    sport_id = "WFH"
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

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/")

    if exists(f"{home_dir}/.ncaa_stats_py/" + "field_hockey/full_schedule/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/" + "field_hockey/full_schedule/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/field_hockey/full_schedule/"
        + f"{season}_{formatted_level}_full_schedule.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/field_hockey/full_schedule/"
            + f"{season}_{formatted_level}_full_schedule.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/"
                + "field_hockey/full_schedule/"
                + f"{season}_{formatted_level}_full_schedule.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if age.days > 1 and season >= now.year and now.month <= 7:
        load_from_cache = False

    if load_from_cache is True:
        return teams_df

    teams_df = load_field_hockey_teams()
    teams_df = teams_df[
        (teams_df["season"] == season) &
        (teams_df["ncaa_division"] == ncaa_level)
    ]
    team_ids_arr = teams_df["team_id"].to_numpy()

    for team_id in tqdm(team_ids_arr):
        temp_df = get_field_hockey_team_schedule(team_id=team_id)
        schedule_df_arr.append(temp_df)

    schedule_df = pd.concat(schedule_df_arr, ignore_index=True)
    schedule_df = schedule_df.drop_duplicates(subset="game_id", keep="first")
    schedule_df["sport_id"] = sport_id
    schedule_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/"
        + "field_hockey/full_schedule/"
        + f"{season}_{formatted_level}_full_schedule.csv",
        index=False,
    )
    return schedule_df


def get_field_hockey_team_roster(team_id: int) -> pd.DataFrame:
    """
    Retrieves a field hockey team's roster from a given team ID.

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

    from ncaa_stats_py.field_hockey import get_field_hockey_team_roster

    # Get the roster of the 2024 Quinnipac field hockey team (D1, ID: 586482).
    print(
        "Get the roster of the 2024 Quinnipac " +
        "field hockey team (D1, ID: 586482)."
    )
    df = get_field_hockey_team_roster(team_id=586482)
    print(df)

    # Get the roster of the 2023 Slippery Rock
    # field hockey team (D2, ID: 557409).
    print(
        "Get the roster of the 2023 Slippery Rock " +
        "field hockey team (D2, ID: 548310)."
    )
    df = get_field_hockey_team_roster(team_id=557409)
    print(df)


    # Get the roster of the 2022 Ferrum field hockey team (D3, ID: 539551).
    print(
        "Get the roster of the 2022 Ferrum field hockey team (D3, ID: 539551)."
    )
    df = get_field_hockey_team_roster(team_id=539551)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with
    an NCAA field hockey team's roster for that season.
    """
    sport_id = "WFH"
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

    team_df = load_field_hockey_teams()

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

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/")

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/rosters/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/rosters/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/field_hockey/rosters/" +
        f"{team_id}_roster.csv"
    ):
        teams_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/field_hockey/rosters/" +
            f"{team_id}_roster.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/field_hockey/rosters/"
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
    # For NCAA field_hockey, the season always starts in the spring semester,
    # and ends in the fall semester.
    # Thus, if `season_name` = "2011-12",
    # this is the "2012" field_hockey season,
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
        f"{home_dir}/.ncaa_stats_py/field_hockey/rosters/" +
        f"{team_id}_roster.csv",
        index=False,
    )
    return roster_df


def get_field_hockey_player_season_stats(
    team_id: int,
) -> pd.DataFrame:
    """
    Given a team ID, this function retrieves and parses
    the season stats for all of the players in a given field hockey team.

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

    from ncaa_stats_py.field_hockey import get_field_hockey_player_season_stats

    # Get the season stats of the
    # 2024 Harvard field hockey team (D1, ID: 586464).
    print(
        "Get the season stats of the " +
        "2024 Harvard field hockey team (D1, ID: 586464)."
    )
    df = get_field_hockey_player_season_stats(team_id=586464)
    print(df)

    # Get the season stats of the
    # 2024 Kutztown field hockey team (D1, ID: 557375).
    print(
        "Get the season stats of the " +
        "2024 Kutztown field hockey team (D1, ID: 557375)."
    )
    df = get_field_hockey_player_season_stats(team_id=557375)
    print(df)


    # Get the season stats of the
    # 2024 Swarthmore field hockey team (D1, ID: 539344).
    print(
        "Get the season stats of the " +
        "2024 Swarthmore field hockey team (D1, ID: 539344)."
    )
    df = get_field_hockey_player_season_stats(team_id=539344)
    print(df)
    ```

    Returns
    ----------
    A pandas `DataFrame` object with the season stats for
    all players with a given NCAA field hockey team.
    """
    sport_id = "WFH"
    load_from_cache = True
    gk_df = pd.DataFrame()
    gk_df_arr = []
    players_df = pd.DataFrame()
    players_df_arr = []
    stats_df = pd.DataFrame()
    # stats_df_arr = []
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
        "player_positions",
        "player_GP",
        "player_GS",

        "player_G",
        "player_AST",
        "player_PTS",
        "player_SH",
        "player_Fouls",
        "player_RC",
        "player_YC",
        "player_GC",
        "player_DSV",

        "goalie_GP",
        "goalie_GS",
        "goalie_minutes_played",
        "goalie_seconds_played",
        "goalie_GA",
        "goalie_GAA",
        "goalie_SV",
    ]

    team_df = load_field_hockey_teams()

    team_df = team_df[team_df["team_id"] == team_id]

    season = team_df["season"].iloc[0]
    ncaa_division = team_df["ncaa_division"].iloc[0]
    ncaa_division_formatted = team_df["ncaa_division_formatted"].iloc[0]
    team_conference_name = team_df["team_conference_name"].iloc[0]
    school_name = team_df["school_name"].iloc[0]
    school_id = int(team_df["school_id"].iloc[0])

    del team_df

    players_stat_id = _get_stat_id(
        sport="field_hockey", season=season, stat_type="non_goalkeepers"
    )

    goalkeepers_stat_id = _get_stat_id(
        sport="field_hockey", season=season, stat_type="goalkeepers"
    )

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    players_url = (
        f"https://stats.ncaa.org/teams/{team_id}/season_to_date_stats?"
        + f"year_stat_category_id={players_stat_id}"
    )

    gk_url = (
        f"https://stats.ncaa.org/teams/{team_id}/season_to_date_stats?"
        + f"year_stat_category_id={goalkeepers_stat_id}"
    )

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/")

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/player_season_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/player_season_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/field_hockey/player_season_stats/"
        + f"{season:00d}_{school_id:00d}_player_season_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/field_hockey/player_season_stats/"
            + f"{season:00d}_{school_id:00d}_player_season_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/field_hockey/"
                + "player_season_stats/"
                + f"{season:00d}_{school_id:00d}"
                + "_player_season_stats.csv"
            )
        )
    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if age.days > 1 and season >= now.year and now.month <= 7:
        load_from_cache = False

    if load_from_cache is True:
        return games_df

    # Parse both normal and goalie game data
    for url in [players_url, gk_url]:
        stat_type_str = ""
        if str(players_stat_id) in url:
            stat_type_str = "players"
        else:
            stat_type_str = "goalkeepers"

        response = _get_webpage(url=url)
        soup = BeautifulSoup(response.text, features="lxml")

        season_name = (
            soup.find("select", {"id": "year_list"})
            .find("option", {"selected": "selected"})
            .text
        )

        season = f"{season_name[0:4]}"
        season = int(season)

        table_data = soup.find(
            "table",
            {
                "id": "stat_grid",
                "class": "small_font dataTable table-bordered"
            }
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
            if p_sortable == "-":
                continue

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

            player_id = player_id.replace("/players", "").replace("/", "")

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

            if stat_type_str == "players":
                players_df_arr.append(temp_df)
            else:
                gk_df_arr.append(temp_df)

    gk_df = pd.concat(gk_df_arr, ignore_index=True)
    gk_df = gk_df.replace("", None)
    gk_df = gk_df.fillna(0)
    gk_df.rename(
        columns={
            "#": "player_jersey_number",
            "Player": "player_full_name",
            "Yr": "player_class",
            "Pos": "player_positions",
            "GP": "goalie_GP",
            "GS": "goalie_GS",
            "Min": "goalie_minutes_played",
            "GA": "goalie_GA",
            "GAA": "goalie_GAA",
            "SV": "goalie_SV",
        },
        inplace=True,
    )
    gk_df[["gk_min", "gk_sec"]] = gk_df["goalie_minutes_played"].str.split(
        ":", expand=True
    )
    gk_df[["gk_min", "gk_sec"]] = gk_df[["gk_min", "gk_sec"]].astype("uint64")

    gk_df["goalie_seconds_played"] = (gk_df["gk_min"] * 60) + gk_df["gk_sec"]

    gk_df.drop(
        columns=["gk_min", "gk_sec"],
        inplace=True
    )

    players_df = pd.concat(players_df_arr, ignore_index=True)
    players_df = players_df.replace("", None)
    players_df = players_df.infer_objects().fillna(0)

    players_df.rename(
        columns={
            "#": "player_jersey_number",
            "Player": "player_full_name",
            "Yr": "player_class",
            "Pos": "player_positions",
            "GP": "player_GP",
            "GS": "player_GS",
            "Goals": "player_G",
            "AST": "player_AST",
            "PTS": "player_PTS",
            "ShAtt": "player_SH",
            "Fouls": "player_Fouls",
            "RC": "player_RC",
            "YC": "player_YC",
            "GC": "player_GC",
            "DSv": "player_DSV",
        },
        inplace=True,
    )

    stats_df = players_df.merge(
        gk_df,
        how="outer",
        on=[
            "player_id",
            "player_jersey_number",
            "player_full_name",
            "player_class",
            "player_positions",
            "player_last_name",
            "player_first_name",
        ],
    )

    stats_df["season"] = season
    stats_df["season_name"] = season_name
    stats_df["school_id"] = school_id
    stats_df["school_name"] = school_name
    stats_df["ncaa_division"] = ncaa_division
    stats_df["ncaa_division_formatted"] = ncaa_division_formatted
    stats_df["team_conference_name"] = team_conference_name
    stats_df["team_id"] = team_id
    stats_df["sport_id"] = sport_id

    for i in stats_df.columns:
        if i in stat_columns:
            pass
        else:
            raise ValueError(
                f"Unhandled column name {i}"
            )

    stats_df = stats_df.infer_objects().reindex(columns=stat_columns)

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/field_hockey/"
        + "player_season_stats/"
        + f"{season:00d}_{school_id:00d}_player_season_stats.csv",
        index=False,
    )

    return stats_df


def get_field_hockey_player_game_stats(
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

    from ncaa_stats_py.field_hockey import (
        get_field_hockey_player_game_stats
    )

    # Get the game stats of Filou Verhoueven in 2023 (Bryant).
    print(
        "Get the game stats of Filou Verhoueven in 2023 (Bryant)."
    )
    df = get_field_hockey_player_game_stats(player_id=8872955, season=2024)
    print(df)

    # Get the game stats of Valen Luna Paratore in 2024 (Richmond).
    print(
        "Get the game stats of Valen Luna Paratore in 2024 (Richmond)."
    )
    df = get_field_hockey_player_game_stats(player_id=8956065, season=2024)
    print(df)

    # Get the game stats of Avery Congleton in 2023 (Mount Olive).
    print(
        "Get the game stats of Avery Congleton in 2023 (ECU)."
    )
    df = get_field_hockey_player_game_stats(player_id=8106592, season=2023)
    print(df)

    # Get the game stats of Meaghan Hogan in 2022 (Endicott).
    print(
        "Get the game stats of Meaghan Hogan in 2022 (Endicott)."
    )
    df = get_field_hockey_player_game_stats(player_id=7599852, season=2022)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a player's batting game logs
    in a given season.
    """
    # sport_id = "WFH"

    # stat_columns = []
    load_from_cache = True

    stats_df = pd.DataFrame()
    stats_df_arr = []
    init_df = pd.DataFrame()
    init_df_arr = []
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

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/")

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/player_game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/player_game_stats/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/field_hockey/player_game_stats/"
        + f"{season}_{player_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/field_hockey/player_game_stats/"
            + f"{season}_{player_id}_player_game_stats.csv"
        )
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/field_hockey/"
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
        age.days > 1 and
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
        df = get_field_hockey_game_player_stats(game_id)
        stats_df_arr.append(df)
        del df

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    stats_df = stats_df[stats_df["player_id"] == player_id]

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/field_hockey/player_game_stats/"
        + f"{season}_{player_id}_player_game_stats.csv",
        index=False
    )
    return stats_df


def get_field_hockey_game_player_stats(game_id: int) -> pd.DataFrame:
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

    from ncaa_stats_py.field_hockey import (
        get_field_hockey_game_player_stats
    )

    # Get the player game stats of a November 10th, 2024 game between
    # the Michigan Wolverines and the Northwestern Wildcats.
    print(
        "Get the player game stats of a November 10th, 2024 game between " +
        "the Michigan Wolverines and the Northwestern Wildcats"
    )
    df = get_field_hockey_game_player_stats(
        game_id=5834079
    )
    print(df)


    # Get the player game stats of a November 10th, 2024 game between
    # the Monmouth Hawks and the Delaware Blue Hens.
    print(
        "Get the player game stats of a November 10th, 2024 game between " +
        "the Monmouth Hawks and the Delaware Blue Hens."
    )
    df = get_field_hockey_game_player_stats(
        game_id=5834140
    )
    print(df)

    # Get the player game stats of a November 10th, 2024 game between the
    # Frostburg St. Bobcats and the Pace Setters on September 10th, 2023.
    print(
        "Get the player game stats of a September 10th, 2023 game between " +
        "Frostburg St. Bobcats and the Pace Setters."
    )
    df = get_field_hockey_game_player_stats(
        game_id=3954181
    )
    print(df)

    # Get the player game stats of a September 16th, 2022 game between the
    # Immaculata Mighty Macs and the Lancaster Bible Chargers.
    print(
        "Get the player game stats of " +
        "a September 16th, 2022 game between the " +
        "Immaculata Mighty Macs and the Lancaster Bible Chargers."
    )
    df = get_field_hockey_game_player_stats(
        game_id=2312967
    )
    print(df)
    ```

    Returns
    ----------
    A pandas `DataFrame` object with a player game stats in a given game.

    """
    sport_id = "WFH"
    load_from_cache = True
    season = 0
    team_df = load_field_hockey_teams()

    stats_df = pd.DataFrame()

    players_df = pd.DataFrame()
    players_df_arr = []

    gk_df = pd.DataFrame()
    gk_df_arr = []

    temp_df = pd.DataFrame()
    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    stat_columns = [
        "season",
        "game_id",
        "sport_id",
        "team_id",
        "player_id",
        "player_jersey_number",
        "player_full_name",
        "player_positions",
        "player_GP",
        "player_MP",
        "player_seconds_played",
        "player_G",
        "player_SH",
        "player_AST",
        "player_SOG",
        "player_Fouls",
        "player_YC",
        "player_GC",
        "player_RC",
        "player_PTS",
        "goalie_GP",
        "goalie_minutes_played",
        "goalie_seconds_played",
        "goalie_GA",
        "goalie_SV",
        "goalie_GAA",
    ]

    url = f"https://stats.ncaa.org/contests/{game_id}/individual_stats"

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/")

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/game_stats/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/game_stats/")

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/game_stats/player/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/game_stats/player/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/field_hockey/game_stats/player/"
        + f"{game_id}_player_game_stats.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/field_hockey/game_stats/player/"
            + f"{game_id}_player_game_stats.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/field_hockey/game_stats/player/"
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

    # table_data = soup.find_all(
    #     "table",
    #     {"class": "small_font dataTable table-bordered"}
    # )[1]
    table_boxes = soup.find_all("div", {"class": "card p-0 table-responsive"})

    for box in table_boxes:
        t_header = box.find("div", {"class": "card-header"}).find(
            "div", {"class": "row"}
        )
        # t_header_str = t_header.text
        team_id = t_header.find("a").get("href")
        team_id = team_id.replace("/teams", "")
        team_id = team_id.replace("/", "")
        team_id = int(team_id)

        table_data = box.find(
            "table", {"class": "display dataTable small_font"}
        )
        table_headers = box.find("thead").find_all("th")
        table_headers = [x.text for x in table_headers]

        temp_t_rows = table_data.find("tbody")
        temp_t_rows = temp_t_rows.find_all("tr")

        spec_stats_df = pd.DataFrame()
        spec_stats_df_arr = []
        for t in temp_t_rows:
            # row_id = t.get("id")
            # game_played = 1
            # game_started = 1
            try:
                player_id = t.find("a").get("href")
                player_id = player_id.replace("/players", "")
                player_id = player_id.replace("/player", "")
                player_id = player_id.replace("/", "")
            except Exception as e:
                logging.debug(
                    "Could not replace player IDs. " + f"Full exception: `{e}`"
                )
                player_id = team_id * -1

            t_cells = t.find_all("td")
            # p_name = t_cells[1].text.replace("\n", "")
            # if "\xa0" in p_name:
            #     game_started = 0
            t_cells = [x.text.strip() for x in t_cells]
            player_id = int(player_id)

            temp_df = pd.DataFrame(data=[t_cells], columns=table_headers)
            temp_df["player_id"] = player_id
            # temp_df["GP"] = game_played
            # temp_df["GS"] = game_started
            spec_stats_df_arr.append(temp_df)

        spec_stats_df = pd.concat(spec_stats_df_arr, ignore_index=True)
        spec_stats_df["team_id"] = team_id
        spec_stats_df = spec_stats_df[
            (spec_stats_df["player_id"] > 0) |
            (spec_stats_df["Name"] == "TEAM")
        ]

        df = team_df[team_df["team_id"] == team_id]
        season = df["season"].iloc[0]
        del df

        if "GA" in table_headers:
            # This means that it's goalie data,
            # because "GA" = Goals Allowed.
            gk_df_arr.append(spec_stats_df)
        else:
            # This means that it's data for non-goalies.
            players_df_arr.append(spec_stats_df)

        del spec_stats_df

    players_df = pd.concat(players_df_arr, ignore_index=True)
    players_df.rename(
        columns={
            "#": "player_jersey_number",
            "Player": "player_full_name",
            "Name": "player_full_name",
            "Yr": "player_class",
            "Pos": "player_positions",
            "P": "player_positions",
            "G": "player_GP",
            "MP": "player_MP",
            "GP": "player_GP",
            "GS": "player_GS",
            "Goals": "player_G",
            "SoG": "player_SOG",
            "AST": "player_AST",
            "PTS": "player_PTS",
            "ShAtt": "player_SH",
            "Fouls": "player_Fouls",
            "RC": "player_RC",
            "YC": "player_YC",
            "GC": "player_GC",
            "DSv": "player_DSV",
        },
        inplace=True,
    )
    if "player_MP" in players_df.columns:

        players_df["player_seconds_played"] = players_df["player_MP"].map(
            _get_seconds_from_time_str
        )

    gk_df = pd.concat(gk_df_arr, ignore_index=True)
    gk_df.rename(
        columns={
            "#": "player_jersey_number",
            "Player": "player_full_name",
            "Name": "player_full_name",
            "Yr": "player_class",
            "Pos": "player_positions",
            "P": "player_positions",
            "GP": "goalie_GP",
            "GS": "goalie_GS",
            "Min": "goalie_minutes_played",
            "GA": "goalie_GA",
            "GAA": "goalie_GAA",
            "SV": "goalie_SV",
        },
        inplace=True,
    )
    gk_df["goalie_GP"] = 1

    gk_df["goalie_seconds_played"] = gk_df["goalie_minutes_played"].map(
        _get_seconds_from_time_str
    )

    stats_df = pd.merge(
        left=players_df,
        right=gk_df,
        on=[
            "player_jersey_number",
            "player_full_name",
            "player_positions",
            "player_id",
            "team_id",
        ],
        how="outer",
    )

    # print(players_df.columns)
    # print(gk_df.columns)

    stats_df["game_id"] = game_id
    stats_df["sport_id"] = sport_id
    for i in stats_df.columns:
        if i in stat_columns:
            pass
        else:
            raise ValueError(f"Unhandled column name {i}")

    stats_df = stats_df.reindex(columns=stat_columns)

    # print(stats_df.columns)

    stats_df = stats_df.infer_objects().fillna(0)
    stats_df["goalie_GAA"] = stats_df["goalie_GAA"].astype("string")
    stats_df["goalie_GAA"] = stats_df["goalie_GAA"].str.replace(",", "")
    stats_df["season"] = season
    stats_df = stats_df.astype(
        {
            "game_id": "int64",
            "team_id": "int64",
            "player_id": "int64",
            "player_jersey_number": "string",
            "player_full_name": "string",
            "player_positions": "string",
            "sport_id": "string",
            "player_GP": "uint16",
            "player_G": "uint16",
            "player_SH": "uint16",
            "player_AST": "uint16",
            "player_SOG": "uint16",
            "player_Fouls": "uint16",
            "player_YC": "uint16",
            "player_GC": "uint16",
            "player_RC": "uint16",
            "player_PTS": "uint16",
            "goalie_minutes_played": "string",
            "goalie_GA": "uint16",
            "goalie_SV": "uint16",
            "goalie_GAA": "float32",
            "goalie_GP": "uint16",
        }
    )
    stats_df["goalie_GAA"] = stats_df["goalie_GAA"].round(3)

    stats_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/field_hockey/game_stats/player/"
        + f"{game_id}_player_game_stats.csv",
        index=False
    )
    return stats_df


def get_field_hockey_game_team_stats(game_id: int) -> pd.DataFrame:
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

    from ncaa_stats_py.field_hockey import (
        get_field_hockey_game_team_stats
    )

    # Get the team game stats of a November 10th, 2024 game between
    # the Michigan Wolverines and the Northwestern Wildcats.
    print(
        "Get the team game stats of a November 10th, 2024 game between " +
        "the Michigan Wolverines and the Northwestern Wildcats"
    )
    df = get_field_hockey_game_team_stats(
        game_id=5834079
    )
    print(df)


    # Get the team game stats of a November 10th, 2024 game between
    # the Monmouth Hawks and the Delaware Blue Hens.
    print(
        "Get the team game stats of a November 10th, 2024 game between " +
        "the Monmouth Hawks and the Delaware Blue Hens."
    )
    df = get_field_hockey_game_team_stats(
        game_id=5834140
    )
    print(df)

    # Get the team game stats of a November 10th, 2024 game between the
    # Frostburg St. Bobcats and the Pace Setters on September 10th, 2023.
    print(
        "Get the team game stats of a September 10th, 2023 game between " +
        "Frostburg St. Bobcats and the Pace Setters."
    )
    df = get_field_hockey_game_team_stats(
        game_id=3954181
    )
    print(df)

    # Get the team game stats of a September 16th, 2022 game between the
    # Immaculata Mighty Macs and the Lancaster Bible Chargers.
    print(
        "Get the team game stats of " +
        "a September 16th, 2022 game between the " +
        "Immaculata Mighty Macs and the Lancaster Bible Chargers."
    )
    df = get_field_hockey_game_team_stats(
        game_id=2312967
    )
    print(df)
    ```

    Returns
    ----------
    A pandas `DataFrame` object with team game stats in a given game.
    """
    df = get_field_hockey_game_player_stats(game_id=game_id)
    df = df.infer_objects()
    print(df.columns)

    stats_df = df.groupby(
        ["season", "game_id", "team_id"],
        as_index=False
    ).agg(
        {
            "player_seconds_played": "sum",
            "player_G": "sum",
            "player_SH": "sum",
            "player_AST": "sum",
            "player_SOG": "sum",
            "player_Fouls": "sum",
            "player_YC": "sum",
            "player_GC": "sum",
            "player_RC": "sum",
            "player_PTS": "sum",
            "goalie_GP": "sum",
            # "goalie_minutes_played":"",
            "goalie_seconds_played": "sum",
            "goalie_GA": "sum",
            "goalie_SV": "sum",
        }
    )

    return stats_df


def get_field_hockey_raw_pbp(game_id: int) -> pd.DataFrame:
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

    from ncaa_stats_py.field_hockey import get_field_hockey_raw_pbp


    # Get the play-by-play data of the
    # 2024 NCAA D1 Field Hockey National Championship game.
    print(
        "Get the play-by-play data of the "
        + "2024 NCAA D1 Field Hockey National Championship game."
    )
    df = get_field_hockey_raw_pbp(5835105)
    print(df)

    # Get the play-by-play data of a September 22nd, 2024
    # game between the Saint Joseph's Hawks and the Duke Blue Devils.
    print(
        "Get the play-by-play data of a September 22nd, 2024 "
        + "game between the Saint Joseph's Hawks and the Duke Blue Devils."
    )
    df = get_field_hockey_raw_pbp(5685953)
    print(df)

    # Get the play-by-play data of a double overtime game
    # between the Syracuse Orange and the North Carolina Tar Heels
    # on September 23rd, 2016.
    print(
        "Get the play-by-play data of a double overtime game "
        "between the Syracuse Orange and the North Carolina Tar Heels "
        + "on September 23rd, 2016."
    )
    df = get_field_hockey_raw_pbp(157178)
    print(df)

    # Get the play-by-play data of a March 28th, 2021
    # game between a Ohio State Buckyes team and the Northwestern Wildcats.
    print(
        "Get the play-by-play data of a March 28th, 2021 "
        + "game between a Ohio State Buckyes team "
        + "and the Northwestern Wildcats."
    )
    df = get_field_hockey_raw_pbp(2034526)
    print(df)

    # Get the play-by-play data of a November 1st, 2024 game
    # between the Central Michigan Chippewas and the Ohio Bobcats.
    print(
        "Get the play-by-play data of a November 1st, 2024 game "
        + "between the Central Michigan Chippewas and the Ohio Bobcats."
    )
    df = get_field_hockey_raw_pbp(5687288)
    print(df)

    # Get the play-by-play data of a September 2nd, 2023 game
    # between the Saint Michael's Purple Knights and the Kutztown Golden Bears.
    print(
        "Get the play-by-play data of a September 2nd, 2023 game "
        + "between the Saint Michael's Purple Knights "
        + "and the Kutztown Golden Bears."
    )
    df = get_field_hockey_raw_pbp(3483126)
    print(df)

    # Get the play-by-play data of an October 1st, 2022 game
    # between the Gwynedd Mercy Griffins and the Keystone Giants.
    print(
        "Get the play-by-play data of an October 1st, 2022 game "
        + "between the Gwynedd Mercy Griffins and the Keystone Giants."
    )
    df = get_field_hockey_raw_pbp(2313570)
    print(df)

    ```

    Returns
    ----------
    A pandas `DataFrame` object with a play-by-play (PBP) data in a given game.

    """
    load_from_cache = True
    is_overtime = False

    sport_id = "WFH"
    season = 0
    away_score = 0
    home_score = 0

    quarter_seconds_remaining = 0
    half_seconds_remaining = 0
    game_seconds_remaining = 0

    # teams_df = load_field_hockey_teams()
    # team_ids_arr = teams_df["team_id"].to_list()

    pbp_df = pd.DataFrame()
    pbp_df_arr = []
    temp_df = pd.DataFrame()

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

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/")

    if exists(f"{home_dir}/.ncaa_stats_py/field_hockey/raw_pbp/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/field_hockey/raw_pbp/")

    if exists(
        f"{home_dir}/.ncaa_stats_py/field_hockey/raw_pbp/"
        + f"{game_id}_raw_pbp.csv"
    ):
        games_df = pd.read_csv(
            f"{home_dir}/.ncaa_stats_py/field_hockey/raw_pbp/"
            + f"{game_id}_raw_pbp.csv"
        )
        games_df = games_df.infer_objects()
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(
                f"{home_dir}/.ncaa_stats_py/field_hockey/raw_pbp/"
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
        quarter_num = re.findall(
            r"([0-9]+)",
            half_str
        )

        quarter_num = int(quarter_num[0])

        if "ot" in half_str.lower():
            is_overtime = True
            quarter_num += 4
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

            try:
                temp_time_minutes, temp_time_seconds = \
                    game_time_str.split(":")
            except ValueError as e:
                logging.info(
                    f"Value error detected, full exception `{e}`"
                )
                # event_text = t_cells[0]
                # temp_time_minutes = 0
                # temp_time_seconds = 0
            except Exception as e:
                raise Exception(
                    f"Unhandled exception `{e}`"
                )

            temp_time_minutes = int(temp_time_minutes)
            temp_time_seconds = int(temp_time_seconds)
            # game_time_ms = int(game_time_ms)
            # game_time_seconds = temp_time_seconds + (temp_time_minutes * 60)

            if (
                "end of" in t_cells[1].lower() and
                "quarter" in t_cells[1].lower()
            ):
                quarter_seconds_remaining = (
                    3600 - (900 * quarter_num)
                )
                half_seconds_remaining = (
                    3600 - (900 * quarter_num)
                )
                game_seconds_remaining = (
                    3600 - (900 * quarter_num)
                )
            if season >= 2019:
                # In 2019, to conform to a major
                # International Hockey Federation (IHF) rule change going from
                # two 35 minute halves to four 15 minute quarters.
                # https://ncaaorg.s3.amazonaws.com/championships/sports/fieldhockey/rules/2019-20PRWFH_MajorRulesModifications.pdf
                if quarter_num == 1:
                    quarter_seconds_remaining = (
                        (900 * 1) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    half_seconds_remaining = (
                        (900 * 2) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    game_seconds_remaining = (
                        (900 * 4) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                elif quarter_num == 2:
                    quarter_seconds_remaining = (
                        (900 * 2) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    half_seconds_remaining = (
                        (900 * 2) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    game_seconds_remaining = (
                        (900 * 4) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                elif quarter_num == 3:
                    quarter_seconds_remaining = (
                        (900 * 3) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    half_seconds_remaining = (
                        (900 * 4) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    game_seconds_remaining = (
                        (900 * 4) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                elif quarter_num == 4:
                    quarter_seconds_remaining = (
                        (900 * 4) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    half_seconds_remaining = (
                        (900 * 4) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    game_seconds_remaining = (
                        (900 * 4) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                elif quarter_num == 5:
                    # Starting in 2018, Field hockey overtimes
                    # were reduced from 15 minute periods,
                    # to 10 minute periods.
                    # https://www.ncaa.com/news/fieldhockey/article/2018-06-13/overtime-length-reduced-two-10-minute-periods-ncaa-field-hockey
                    quarter_seconds_remaining = (
                        (900 * 4) + 600 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    half_seconds_remaining = (
                        (900 * 4) + 1200 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    game_seconds_remaining = (
                        (900 * 4) + 1200 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                elif quarter_num == 6:
                    quarter_seconds_remaining = (
                        (900 * 4) + 1200 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    half_seconds_remaining = (
                        (900 * 4) + 1200 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    game_seconds_remaining = (
                        (900 * 4) + 1200 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
            elif season <= 2018:
                if quarter_num == 1:
                    quarter_seconds_remaining = (
                        (35 * 60) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    half_seconds_remaining = quarter_seconds_remaining
                    game_seconds_remaining = (
                        (70 * 60) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                elif quarter_num == 2:
                    quarter_seconds_remaining = (
                        (70 * 60) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    half_seconds_remaining = quarter_seconds_remaining
                    game_seconds_remaining = (
                        (70 * 60) -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                elif season == 2018:
                    # Specifically in 2018, before the move to quarters,
                    # field hockey overtimes
                    # were reduced from 15 minute periods,
                    # to 10 minute periods.
                    # https://www.ncaa.com/news/fieldhockey/article/2018-06-13/overtime-length-reduced-two-10-minute-periods-ncaa-field-hockey

                    quarter_seconds_remaining = (
                        (70 * 60) + 600 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    half_seconds_remaining = (
                        (70 * 60) + 600 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    game_seconds_remaining = (
                        (70 * 60) + 600 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )

                    if quarter_num == 5:
                        half_seconds_remaining += 600
                        game_seconds_remaining += 600
                else:
                    # Specifically in 2018, before the move to quarters,
                    # field hockey overtimes
                    # were reduced from 15 minute periods,
                    # to 10 minute periods.
                    # https://www.ncaa.com/news/fieldhockey/article/2018-06-13/overtime-length-reduced-two-10-minute-periods-ncaa-field-hockey

                    quarter_seconds_remaining = (
                        (70 * 60) + 900 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    half_seconds_remaining = (
                        (70 * 60) + 900 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )
                    game_seconds_remaining = (
                        (70 * 60) + 900 -
                        ((temp_time_minutes * 60) + temp_time_seconds)
                    )

                    if quarter_num >= 3:
                        half_seconds_remaining += 900
                        game_seconds_remaining += 900

                    if quarter_num == 4:
                        quarter_seconds_remaining += 900

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
                    "quarter_seconds_remaining": quarter_seconds_remaining,
                    "half_seconds_remaining": half_seconds_remaining,
                    "game_seconds_remaining": game_seconds_remaining,
                    "quarter_num": quarter_num,
                    "event_team": event_team,
                    "event_text": event_text,
                    "is_overtime": is_overtime
                },
                index=[0],
            )
            pbp_df_arr.append(temp_df)

        if season >= 2019:
            # In 2019, to conform to a major
            # International Hockey Federation (IHF) rule change going from
            # two 35 minute halves to four 15 minute quarters.
            # https://ncaaorg.s3.amazonaws.com/championships/sports/fieldhockey/rules/2019-20PRWFH_MajorRulesModifications.pdf
            if quarter_num == 1:
                game_time_str = "15:00"
                quarter_seconds_remaining = 0
                half_seconds_remaining = 900
                game_seconds_remaining = (900 * 3)
            elif quarter_num == 2:
                game_time_str = "30:00"
                quarter_seconds_remaining = 0
                half_seconds_remaining = 0
                game_seconds_remaining = (900 * 2)
            elif quarter_num == 3:
                game_time_str = "45:00"
                quarter_seconds_remaining = 0
                half_seconds_remaining = 900
                game_seconds_remaining = (900 * 1)
            elif quarter_num == 4:
                game_time_str = "60:00"
                quarter_seconds_remaining = 0
                half_seconds_remaining = 0
                game_seconds_remaining = 0
            else:
                # Starting in 2018, Field hockey overtimes
                # were reduced from 15 minute periods,
                # to 10 minute periods.
                # https://www.ncaa.com/news/fieldhockey/article/2018-06-13/overtime-length-reduced-two-10-minute-periods-ncaa-field-hockey
                quarter_seconds_remaining = 0
                half_seconds_remaining = 0
                game_seconds_remaining = 0
        elif season <= 2018:
            if quarter_num == 1:
                game_time_str = "35:00"
                quarter_seconds_remaining = 0
                half_seconds_remaining = quarter_seconds_remaining
                game_seconds_remaining = (35 * 60)
            elif quarter_num == 2:
                game_time_str = "70:00"
                quarter_seconds_remaining = 0
                half_seconds_remaining = quarter_seconds_remaining
                game_seconds_remaining = 0
            elif season == 2018:
                quarter_seconds_remaining = 0
                half_seconds_remaining = 600
                game_seconds_remaining = 600
            else:
                quarter_seconds_remaining = 0
                half_seconds_remaining = 0
                game_seconds_remaining = 0

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
                "quarter_seconds_remaining": 0,
                "half_seconds_remaining": half_seconds_remaining,
                "game_seconds_remaining": game_seconds_remaining,
                "quarter_num": quarter_num,
                "event_team": event_team,
                "event_text": "End of Quarter",
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
    pbp_df.to_csv(
        f"{home_dir}/.ncaa_stats_py/field_hockey/raw_pbp/"
        + f"{game_id}_raw_pbp.csv",
        index=False
    )
    return pbp_df
