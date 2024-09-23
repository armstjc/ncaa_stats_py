# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: `utls.py`
# Purpose: Houses functions and data points that need to be accessed
#     by multiple functions in this python package.
# Creation Date: 2024-06-25 07:40 PM EDT
# Update History:
# - 2024-06-25 07:40 PM EDT
# - 2024-09-19 04:30 PM EDT
# - 2024-09-23 10:30 PM EDT


import logging
import time
from datetime import datetime
from os import mkdir
from os.path import exists, expanduser, getmtime

import pandas as pd
import requests
from bs4 import BeautifulSoup


def _stat_id_dict() -> dict:
    stat_id_dict = {
        "baseball": {
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
        }
    }
    return stat_id_dict


def _web_headers() -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/129.0.0.0 Safari/537.36",
    }
    return headers


def _get_webpage(url: str) -> requests.Response:
    """ """
    headers = _web_headers()
    response = requests.get(headers=headers, url=url)
    time.sleep(5)
    if response.status_code == 200:
        return response
    elif response.status_code == 400:
        raise ConnectionRefusedError(
            "[HTTP 400]: Could not access "
            + "the following URL due to a malformed request "
            + "being pushed by the client (A.K.A. the computer running "
            + f"this code that just got this error).\nURL:{url}"
        )
    elif response.status_code == 401:
        raise ConnectionRefusedError(
            "[HTTP 401]: Could not access the following URL "
            + "because the website does not authorize your access "
            + f"to this part of the website.\nURL:{url}"
        )
    elif response.status_code == 403:
        raise ConnectionRefusedError(
            "[HTTP 403]: Could not access the following URL "
            + "because the website outright refuses your access "
            + "to this part of the website, and/or the website as a whole."
            + f"\nURL:{url}"
        )
    elif response.status_code == 404:
        raise ConnectionRefusedError(
            "[HTTP 404]: Could not find anything associated "
            + "with the following URL at this time."
            + f"\nURL:{url}"
        )
    elif response.status_code == 408:
        raise ConnectionRefusedError(
            "[HTTP 408]: The request for the following URL timed out." +
            f"\nURL:{url}"
        )
    elif response.status_code == 418:
        raise ConnectionRefusedError(
            "[HTTP 418]: The request for the following URL "
            + "could not be completed because the server is a teapot."
            + f"\nURL:{url}"
        )
    elif response.status_code == 429:
        raise ConnectionRefusedError(
            "[HTTP 429]: The request for the following URL "
            + "could not be completed because the server believes that "
            + "you have sent too many requests in too short of a timeframe."
            + f"\nURL:{url}"
        )
    elif response.status_code == 451:
        raise ConnectionRefusedError(
            "[HTTP 451]: The request for the following URL "
            + "could not be completed because the contents of the URL "
            + "are unavailable for legal reasons."
            + f"\nURL:{url}"
        )
    elif response.status_code == 500:
        raise ConnectionRefusedError(
            "[HTTP 500]: The request for the following URL "
            + "could not be completed due to an internal server error."
            + f"\nURL:{url}"
        )
    elif response.status_code == 502:
        raise ConnectionRefusedError(
            "[HTTP 502]: The request for the following URL "
            + "could not be completed due to a bad gateway."
            + f"\nURL:{url}"
        )
    elif response.status_code == 503:
        raise ConnectionRefusedError(
            "[HTTP 503]: The request for the following URL "
            + "could not be completed because the webpage is unavailable."
            + f"\nURL:{url}"
        )
    elif response.status_code == 504:
        raise ConnectionRefusedError(
            "[HTTP 504]: The request for the following URL "
            + "could not be completed due to a gateway timeout."
            + f"\nURL:{url}"
        )
    elif response.status_code == 511:
        raise ConnectionRefusedError(
            "[HTTP 500]: The request for the following URL "
            + "could not be completed because you need to authenticate "
            + "to gain network access."
            + f"\nURL:{url}"
        )
    else:
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
                    {
                        "school_id": school_id,
                        "school_name": school_name
                    },
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


if __name__ == "__main__":
    url = "https://stats.ncaa.org/teams/574226/season_to_date_stats"
    response = _get_webpage(url=url)

    season = 2024
    sport = "baseball"
    stat_type = "batting"
    stat_id = _get_stat_id(
        sport=sport, season=season, stat_type=stat_type, html_data=response
    )
