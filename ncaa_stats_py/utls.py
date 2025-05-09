# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: `utls.py`
# Purpose: Houses functions and data points that need to be accessed
#     by multiple functions in this python package.
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


import logging
import time
from datetime import datetime
from os import mkdir
from os.path import exists, expanduser, getmtime
from secrets import SystemRandom

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


def _stat_id_dict() -> dict:
    # For sports that span across the fall and spring
    # semesters, the year will be whichever year is last.
    # For example, if the sport has a season of 2020-21,
    # the official season is "2021".
    stat_id_dict = {
        "baseball": {  # MBA
            2025: {
                "season": 2025,
                "batting": 15687,
                "pitching": 15688,
                "fielding": 15689,
            },
            2024: {
                "season": 2024,
                "batting": 15080,
                "pitching": 15081,
                "fielding": 15082,
            },
            2023: {
                "season": 2023,
                "batting": 15000,
                "pitching": 15001,
                "fielding": 15002,
            },
            2022: {
                "season": 2022,
                "batting": 14940,
                "pitching": 14941,
                "fielding": 14942,
            },
            2021: {
                "season": 2021,
                "batting": 14760,
                "pitching": 14761,
                "fielding": 14762,
            },
            2020: {
                "season": 2020,
                "batting": 14760,
                "pitching": 14761,
                "fielding": 14762,
            },
            2019: {
                "season": 2019,
                "batting": 14643,
                "pitching": 14644,
                "fielding": 14645,
            },
            2018: {
                "season": 2018,
                "batting": 11953,
                "pitching": 11954,
                "fielding": 11955,
            },
            2017: {
                "season": 2017,
                "batting": 11000,
                "pitching": 11001,
                "fielding": 11002,
            },
            2016: {
                "season": 2016,
                "batting": 10946,
                "pitching": 10947,
                "fielding": 10948,
            },
            2015: {
                "season": 2015,
                "batting": 10780,
                "pitching": 10781,
                "fielding": 10782,
            },
            2014: {
                "season": 2014,
                "batting": 10460,
                "pitching": 10461,
                "fielding": 10462,
            },
            2013: {
                "season": 2013,
                "batting": 10120,
                "pitching": 10121,
                "fielding": 10122,
            },
            2012: {
                "season": 2012,
                "batting": 10082,
                "pitching": 10083,
                "fielding": 10084,
            },
            2011: {
                "season": 2011,
                "batting": 10002,
                "pitching": 10001,
                "fielding": 10000,
            },
            # 2010: {
            #     "season": 2010,
            #     "batting": 10002,
            #     "pitching": 10001,
            #     "fielding": 10000,
            # },
        },
        "mbb": {  # Men's basketball
            2025: {"season": 2025},
            2024: {"season": 2024},
            2023: {"season": 2023},
            2022: {"season": 2022},
            2021: {"season": 2021},
            2020: {"season": 2020},
            2019: {"season": 2019},
            2018: {"season": 2018},
            2017: {"season": 2017},
            2016: {"season": 2016},
            2015: {"season": 2015},
            2014: {"season": 2014},
            2013: {"season": 2013},
            2012: {"season": 2012},
            2011: {"season": 2011},
            2010: {"season": 2010},
            2009: {"season": 2009},
        },
        "wbb": {  # Women's basketball
            2025: {"season": 2025},
            2024: {"season": 2024},
            2023: {"season": 2023},
            2022: {"season": 2022},
            2021: {"season": 2021},
            2020: {"season": 2020},
            2019: {"season": 2019},
            2018: {"season": 2018},
            2017: {"season": 2017},
            2016: {"season": 2016},
            2015: {"season": 2015},
            2014: {"season": 2014},
            2013: {"season": 2013},
            2012: {"season": 2012},
            2011: {"season": 2011},
            2010: {"season": 2010},
        },
        "field_hockey": {  # WFH
            # for this part, a season is indicated by the
            # first part of an academic year (like "2024-25").
            # So if you are looking for the stat IDs
            # for games that happen in the 2024-25 academic year,
            # look for "2024", because the field hockey season
            # typically happens between August and November
            # (except for the 2020 COVID spring season).
            2024: {
                "season": 2024,
                "goalkeepers": 15505,
                "non_goalkeepers": 15504
            },
            2023: {
                "season": 2023,
                "goalkeepers": 15151,
                "non_goalkeepers": 15150
            },
            2022: {
                "season": 2022,
                "goalkeepers": 15475,
                "non_goalkeepers": 15474
            },
            2021: {
                "season": 2021,
                "goalkeepers": 15477,
                "non_goalkeepers": 15476
            },
            2020: {
                "season": 2020,
                "goalkeepers": 15479,
                "non_goalkeepers": 15478
            },
            2019: {
                "season": 2019,
                "goalkeepers": 15481,
                "non_goalkeepers": 15480
            },
            2018: {
                "season": 2018,
                "goalkeepers": 15483,
                "non_goalkeepers": 15482
            },
            2017: {
                "season": 2017,
                "goalkeepers": 15485,
                "non_goalkeepers": 15484
            },
            2016: {
                "season": 2016,
                "goalkeepers": 15487,
                "non_goalkeepers": 15486
            },
            2015: {
                "season": 2015,
                "goalkeepers": 15489,
                "non_goalkeepers": 15488
            },
            2014: {
                "season": 2014,
                "goalkeepers": 15491,
                "non_goalkeepers": 15490
            },
            2013: {
                "season": 2013,
                "goalkeepers": 15493,
                "non_goalkeepers": 15492
            },
            2012: {
                "season": 2012,
                "goalkeepers": 15495,
                "non_goalkeepers": 15494
            },
            2011: {
                "season": 2011,
                "goalkeepers": 15497,
                "non_goalkeepers": 15496
            },
            2010: {
                "season": 2010,
                "goalkeepers": 15499,
                "non_goalkeepers": 15498
            },
            2009: {
                "season": 2009,
                "goalkeepers": 15501,
                "non_goalkeepers": 15500
            },
            2008: {
                "season": 2008,
                "goalkeepers": 15503,
                "non_goalkeepers": 15502
            },
        },
        "mens_hockey": {  # MIH
            2025: {
                "season": 2025,
                "goalkeepers": 15581,
                "non_goalkeepers": 15580
            },
            2024: {
                "season": 2024,
                "goalkeepers": 15189,
                "non_goalkeepers": 15188
            },
            2023: {
                "season": 2023,
                "goalkeepers": 15565,
                "non_goalkeepers": 15564
            },
            2022: {
                "season": 2022,
                "goalkeepers": 15567,
                "non_goalkeepers": 15566
            },
            2021: {
                "season": 2021,
                "goalkeepers": 15569,
                "non_goalkeepers": 15568
            },
            2020: {
                "season": 2020,
                "goalkeepers": 15571,
                "non_goalkeepers": 15570
            },
            2019: {
                "season": 2019,
                "goalkeepers": 15573,
                "non_goalkeepers": 15572
            },
            2018: {
                "season": 2018,
                "goalkeepers": 15575,
                "non_goalkeepers": 15574
            },
            2017: {
                "season": 2017,
                "goalkeepers": 15579,
                "non_goalkeepers": 15578
            },
            2016: {
                "season": 2016,
                "goalkeepers": 15577,
                "non_goalkeepers": 15576
            },
        },
        "womens_hockey": {  # WIH
            2025: {
                "season": 2025,
                "goalkeepers": 15599,
                "non_goalkeepers": 15598
            },
            2024: {
                "season": 2024,
                "goalkeepers": 15599,
                "non_goalkeepers": 15598
            },
            2023: {
                "season": 2023,
                "goalkeepers": 15583,
                "non_goalkeepers": 15582
            },
            2022: {
                "season": 2022,
                "goalkeepers": 15585,
                "non_goalkeepers": 15584
            },
            2021: {
                "season": 2021,
                "goalkeepers": 15587,
                "non_goalkeepers": 15586
            },
            2020: {
                "season": 2020,
                "goalkeepers": 15589,
                "non_goalkeepers": 15588
            },
            2019: {
                "season": 2019,
                "goalkeepers": 15591,
                "non_goalkeepers": 15590
            },
            2018: {
                "season": 2018,
                "goalkeepers": 15593,
                "non_goalkeepers": 15592
            },
            2017: {
                "season": 2017,
                "goalkeepers": 15595,
                "non_goalkeepers": 15594
            },
            2016: {
                "season": 2016,
                "goalkeepers": 15597,
                "non_goalkeepers": 15596
            },
        },
        "softball": {
            2025: {
                "season": 2025,
                "batting": 15667,
                "pitching": 15668,
                "fielding": 15669,
            },
            2024: {
                "season": 2024,
                "batting": 15060,
                "pitching": 15061,
                "fielding": 15062,
            },
            2023: {
                "season": 2023,
                "batting": 15020,
                "pitching": 15021,
                "fielding": 15022,
            },
            2022: {
                "season": 2022,
                "batting": 14960,
                "pitching": 14961,
                "fielding": 14962,
            },
            2021: {
                "season": 2021,
                "batting": 14860,
                "pitching": 14861,
                "fielding": 14862,
            },
            2020: {
                "season": 2020,
                "batting": 14780,
                "pitching": 14781,
                "fielding": 14782,
            },
            2019: {
                "season": 2019,
                "batting": 14660,
                "pitching": 14661,
                "fielding": 14662,
            },
            2018: {
                "season": 2018,
                "batting": 13300,
                "pitching": 13301,
                "fielding": 13302,
            },
            2017: {
                "season": 2017,
                "batting": 11020,
                "pitching": 11021,
                "fielding": 11022,
            },
            2016: {
                "season": 2016,
                "batting": 10840,
                "pitching": 10841,
                "fielding": 10842,
            },
            2015: {
                "season": 2015,
                "batting": 10800,
                "pitching": 10801,
                "fielding": 10802,
            },
            2014: {
                "season": 2014,
                "batting": 10480,
                "pitching": 10481,
                "fielding": 10482,
            },
            2013: {
                "season": 2013,
                "batting": 10100,
                "pitching": 10101,
                "fielding": 10102,
            },
            2012: {
                "season": 2012,
                "batting": 1,
                "pitching": 2,
                "fielding": 3,
            },
        },
        "mens_lacrosse": {  # MLA
            2025: {
                "season": 2025,
                "goalkeepers": 15650,
                "non_goalkeepers": 15649
            },
            2024: {
                "season": 2024,
                "goalkeepers": 15167,
                "non_goalkeepers": 15166
            },
            2023: {
                "season": 2023,
                "goalkeepers": 15507,
                "non_goalkeepers": 15506
            },
            2022: {
                "season": 2022,
                "goalkeepers": 15509,
                "non_goalkeepers": 15508
            },
            2021: {
                "season": 2021,
                "goalkeepers": 15511,
                "non_goalkeepers": 15510
            },
            2020: {
                "season": 2020,
                "goalkeepers": 15513,
                "non_goalkeepers": 15512
            },
            2019: {
                "season": 2019,
                "goalkeepers": 15515,
                "non_goalkeepers": 15514
            },
            2018: {
                "season": 2018,
                "goalkeepers": 15517,
                "non_goalkeepers": 15516
            },
            2017: {
                "season": 2017,
                "goalkeepers": 15519,
                "non_goalkeepers": 15518
            },
            2016: {
                "season": 2016,
                "goalkeepers": 15521,
                "non_goalkeepers": 15520
            },
            2015: {
                "season": 2015,
                "goalkeepers": 15523,
                "non_goalkeepers": 15522
            },
            2014: {
                "season": 2014,
                "goalkeepers": 15525,
                "non_goalkeepers": 15524
            },
            2013: {
                "season": 2013,
                "goalkeepers": 15527,
                "non_goalkeepers": 15526
            },
            2012: {
                "season": 2012,
                "goalkeepers": 15529,
                "non_goalkeepers": 15528
            },
            2011: {
                "season": 2011,
                "goalkeepers": 15531,
                "non_goalkeepers": 15530
            },
        },
        "womens_lacrosse": {  # WLA
            2024: {
                "season": 2024,
                "goalkeepers": 15155,
                "non_goalkeepers": 15154
            },
            2023: {
                "season": 2023,
                "goalkeepers": 15537,
                "non_goalkeepers": 15536
            },
            2022: {
                "season": 2022,
                "goalkeepers": 15539,
                "non_goalkeepers": 15538
            },
            2021: {
                "season": 2021,
                "goalkeepers": 15541,
                "non_goalkeepers": 15540
            },
            2020: {
                "season": 2020,
                "goalkeepers": 15543,
                "non_goalkeepers": 15542
            },
            2019: {
                "season": 2019,
                "goalkeepers": 15545,
                "non_goalkeepers": 15544
            },
            2018: {
                "season": 2018,
                "goalkeepers": 15547,
                "non_goalkeepers": 15546
            },
            2017: {
                "season": 2017,
                "goalkeepers": 15549,
                "non_goalkeepers": 15548
            },
            2016: {
                "season": 2016,
                "goalkeepers": 15551,
                "non_goalkeepers": 15550
            },
            2015: {
                "season": 2015,
                "goalkeepers": 15553,
                "non_goalkeepers": 15552
            },
            2014: {
                "season": 2014,
                "goalkeepers": 15555,
                "non_goalkeepers": 15554
            },
            2013: {
                "season": 2013,
                "goalkeepers": 15557,
                "non_goalkeepers": 15556
            },
            2012: {
                "season": 2012,
                "goalkeepers": 15559,
                "non_goalkeepers": 15558
            },
            2011: {
                "season": 2011,
                "goalkeepers": 15561,
                "non_goalkeepers": 15560
            },
            2010: {
                "season": 2010,
                "goalkeepers": 15563,
                "non_goalkeepers": 15562
            },
        },
        "womens_volleyball": {  # Women's basketball
            2025: {"season": 2025},
            2024: {"season": 2024},
            2023: {"season": 2023},
            2022: {"season": 2022},
            2021: {"season": 2021},
            2020: {"season": 2020},
            2019: {"season": 2019},
            2018: {"season": 2018},
            2017: {"season": 2017},
            2016: {"season": 2016},
            2015: {"season": 2015},
            2014: {"season": 2014},
            2013: {"season": 2013},
            2012: {"season": 2012},
            2011: {"season": 2011},
            2010: {"season": 2010},
        },
        "mens_volleyball": {  # Men's basketball
            2025: {"season": 2025},
            2024: {"season": 2024},
            2023: {"season": 2023},
            2022: {"season": 2022},
            2021: {"season": 2021},
            2020: {"season": 2020},
            2019: {"season": 2019},
            2018: {"season": 2018},
            2017: {"season": 2017},
            2016: {"season": 2016},
            2015: {"season": 2015},
            2014: {"season": 2014},
            2013: {"season": 2013},
            2012: {"season": 2012},
            2011: {"season": 2011},
        },
    }
    return stat_id_dict


def _web_headers() -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/135.0.0.0 Safari/537.36",
    }
    return headers


def _get_webpage(url: str) -> requests.Response:
    """ """
    rng = SystemRandom()
    headers = _web_headers()
    response = requests.get(headers=headers, url=url, timeout=30)
    random_integer = 5 + rng.randint(a=0, b=5)
    time.sleep(random_integer)
    if response.status_code == 200:
        return response
    elif response.status_code == 400:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 400]: Could not access "
            + "the following URL due to a malformed request "
            + "being pushed by the client (A.K.A. the computer running "
            + f"this code that just got this error).\nURL:{url}"
        )
    elif response.status_code == 401:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 401]: Could not access the following URL "
            + "because the website does not authorize your access "
            + f"to this part of the website.\nURL:{url}"
        )
    elif response.status_code == 403:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 403]: Could not access the following URL "
            + "because the website outright refuses your access "
            + "to this part of the website, and/or the website as a whole."
            + f"\nURL:{url}"
        )
    elif response.status_code == 404:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 404]: Could not find anything associated "
            + "with the following URL at this time."
            + f"\nURL:{url}"
        )
    elif response.status_code == 408:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 408]: The request for the following URL timed out." +
            f"\nURL:{url}"
        )
    elif response.status_code == 418:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 418]: The request for the following URL "
            + "could not be completed because the server is a teapot."
            + f"\nURL:{url}"
        )
    elif response.status_code == 429:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 429]: The request for the following URL "
            + "could not be completed because the server believes that "
            + "you have sent too many requests in too short of a timeframe."
            + f"\nURL:{url}"
        )
    elif response.status_code == 451:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 451]: The request for the following URL "
            + "could not be completed because the contents of the URL "
            + "are unavailable for legal reasons."
            + f"\nURL:{url}"
        )
    elif response.status_code == 500:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 500]: The request for the following URL "
            + "could not be completed due to an internal server error."
            + f"\nURL:{url}"
        )
    elif response.status_code == 502:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 502]: The request for the following URL "
            + "could not be completed due to a bad gateway."
            + f"\nURL:{url}"
        )
    elif response.status_code == 503:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 503]: The request for the following URL "
            + "could not be completed because the webpage is unavailable."
            + f"\nURL:{url}"
        )
    elif response.status_code == 504:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 504]: The request for the following URL "
            + "could not be completed due to a gateway timeout."
            + f"\nURL:{url}"
        )
    elif response.status_code == 511:
        time.sleep(10)
        raise ConnectionRefusedError(
            "[HTTP 500]: The request for the following URL "
            + "could not be completed because you need to authenticate "
            + "to gain network access."
            + f"\nURL:{url}"
        )
    else:
        time.sleep(10)
        raise ConnectionAbortedError(
            "Could not access the following URL, and received "
            + f"an unhandled status code of `{response.status_code}`"
            + f"\nURL: `{url}`"
        )


def _format_folder_str(folder_str: str) -> str:
    folder_str = folder_str.replace("\\", "/")
    folder_str = folder_str.replace("//", "/")
    return folder_str


def _get_schools() -> pd.DataFrame:
    """ """
    load_from_cache = True
    schools_df = pd.DataFrame()
    schools_df_arr = []
    temp_df = pd.DataFrame()

    home_dir = expanduser("~")
    home_dir = _format_folder_str(home_dir)

    if exists(f"{home_dir}/.ncaa_stats_py/"):
        pass
    else:
        mkdir(f"{home_dir}/.ncaa_stats_py/")

    if exists(f"{home_dir}/.ncaa_stats_py/schools.csv"):
        schools_df = pd.read_csv(f"{home_dir}/.ncaa_stats_py/schools.csv")
        file_mod_datetime = datetime.fromtimestamp(
            getmtime(f"{home_dir}/.ncaa_stats_py/schools.csv")
        )

    else:
        file_mod_datetime = datetime.today()
        load_from_cache = False

    now = datetime.today()

    age = now - file_mod_datetime

    if age.days > 90:
        load_from_cache = False

    if load_from_cache is True:
        return schools_df
    else:
        url = "https://stats.ncaa.org/teams/history"
        response = _get_webpage(url=url)

        soup = BeautifulSoup(response.text, features="lxml")
        schools_ar = soup.find(
            "select",
            {"name": "org_id", "id": "org_id_select"}
        )
        schools_ar = schools_ar.find_all("option")

        for s in schools_ar:

            school_id = s.get("value")
            school_name = s.text

            if len(school_id) == 0:
                pass
            elif school_name.lower() == "career":
                pass
            elif "Z_Do_Not_Use_" in school_name:
                pass
            else:
                school_id = int(school_id)

                temp_df = pd.DataFrame(
                    {"school_id": school_id, "school_name": school_name},
                    index=[0]
                )
                schools_df_arr.append(temp_df)
                del temp_df

    schools_df = pd.concat(schools_df_arr, ignore_index=True)
    schools_df.sort_values(by=["school_id"], inplace=True)
    schools_df.drop_duplicates(subset=["school_name"], inplace=True)
    schools_df.to_csv(f"{home_dir}/.ncaa_stats_py/schools.csv", index=False)

    return schools_df


def _get_stat_id(sport: str, season: int, stat_type: str) -> int:
    """ """
    data = _stat_id_dict()
    try:
        t_data = data[sport.lower()][season]

        for key, value in t_data.items():
            if key == stat_type:
                return value
    except Exception:
        logging.warning(
            "An error occurred when attempting to locate a stat ID. "
            + "Attempting a slower method of finding your stat ID. "
            + "If you keep seeing this message, "
            + "please raise an issue at "
            + "\n https://github.com/armstjc/ncaa_stats_py/issues \n"
        )

    # If we can't find the requested stat type, raise this error.
    raise LookupError(
        f"Could not locate a stat ID in {sport} for {stat_type} "
        + f"in the {season} season."
    )


def _get_minute_formatted_time_from_seconds(seconds: int) -> str:
    """ """
    t_minutes = seconds // 60
    t_seconds = seconds % 60
    return f"{t_minutes:02d}:{t_seconds:02d}"


def _get_seconds_from_time_str(time_str: str) -> int:
    """ """
    if ":" not in time_str:
        return 0

    t_minutes, t_seconds = time_str.split(":")
    t_minutes = int(t_minutes)
    t_seconds = int(t_seconds)
    time_seconds = (t_minutes * 60) + t_seconds
    return time_seconds


def _name_smother(name_str: str) -> str:
    # name_str = name_str.replace("3a")
    if name_str is None:
        return name_str
    elif isinstance(name_str, float):
        return name_str
    elif name_str == np.nan:
        return name_str

    name_str = name_str.strip()

    if " (" in name_str:
        name_str = name_str.split(" (")[0]
    elif ", block error" in name_str:
        name_str = name_str.split(", block error")[0]

    if "," not in name_str:
        return name_str
    elif name_str.count(",") == 2:
        l_name, sfx, f_name = name_str.split(",")
        name_str = f"{f_name} {l_name} {sfx}"
        name_str = name_str.strip()
        return name_str
    elif name_str.count(",") > name_str.count(" "):
        try:
            l_name, f_name = name_str.split(",")
            name_str = f"{f_name} {l_name}"
            return name_str
        except ValueError:
            return name_str
    elif "," in name_str:
        l_name, f_name = name_str.split(",")
        l_name = l_name.strip()
        f_name = f_name.strip()
        name_str = f"{f_name} {l_name}"
        return name_str
    else:
        # raise ValueError(f"unhandled string {name_str}")
        return name_str


if __name__ == "__main__":
    # url = "https://stats.ncaa.org/teams/574226/season_to_date_stats"
    # response = _get_webpage(url=url)

    # season = 2024
    # sport = "baseball"
    # stat_type = "batting"
    # stat_id = _get_stat_id(
    #     sport=sport, season=season, stat_type=stat_type
    # )
    print(_get_minute_formatted_time_from_seconds(15604))
