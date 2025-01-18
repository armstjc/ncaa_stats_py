# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: `volleyball.py`
# Purpose: Houses functions to parse NCAA volleyball play-by-play data
# Creation Date: 2025-01-16 12:15 AM EDT

import logging
import re

import pandas as pd


def _volleyball_pbp_helper(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    NOT INTENDED TO BE CALLED DIRECTLY BY A USER!

    See `ncaa_stats_py.volleyball.get_parsed_volleyball_pbp()`
    """

    pbp_df = pd.DataFrame()
    pbp_df_arr = []

    season = raw_df["season"].iloc[0]
    game_id = raw_df["game_id"].iloc[0]
    sport_id = raw_df["sport_id"].iloc[0]
    game_datetime = raw_df["game_datetime"].iloc[0]
    stadium_name = raw_df["stadium_name"].iloc[0]
    attendance = raw_df["attendance"].iloc[0]

    home_team_id = raw_df["home_team_id"].iloc[0]
    away_team_id = raw_df["away_team_id"].iloc[0]

    home_team_name = raw_df["home_team_name"].iloc[0]
    away_team_name = raw_df["away_team_name"].iloc[0]

    home_set_1_score = raw_df["home_set_1_score"].iloc[0]
    away_set_1_score = raw_df["away_set_1_score"].iloc[0]

    home_set_2_score = raw_df["home_set_2_score"].iloc[0]
    away_set_2_score = raw_df["away_set_2_score"].iloc[0]

    home_set_3_score = raw_df["home_set_3_score"].iloc[0]
    away_set_3_score = raw_df["away_set_3_score"].iloc[0]

    home_set_4_score = raw_df["home_set_4_score"].iloc[0]
    away_set_4_score = raw_df["away_set_4_score"].iloc[0]

    home_set_5_score = raw_df["home_set_5_score"].iloc[0]
    away_set_5_score = raw_df["away_set_5_score"].iloc[0]

    set_num_arr = raw_df["set_num"].to_numpy()
    event_num_arr = raw_df["event_num"].to_numpy()
    event_team_arr = raw_df["event_team"].to_numpy()
    event_text_arr = raw_df["event_text"].to_numpy()
    is_scoring_play_arr = raw_df["is_scoring_play"].to_numpy()
    is_extra_points_arr = raw_df["is_extra_points"].to_numpy()

    home_set_score_arr = raw_df["home_set_score"].to_numpy()
    away_set_score_arr = raw_df["away_set_score"].to_numpy()

    home_cumulative_score_arr = raw_df["home_cumulative_score"].to_numpy()
    away_cumulative_score_arr = raw_df["away_cumulative_score"].to_numpy()

    home_sets_won_arr = raw_df["home_sets_won"].to_numpy()
    away_sets_won_arr = raw_df["away_sets_won"].to_numpy()

    del raw_df

    for i in range(0, len(event_num_arr)):
        set_num = set_num_arr[i]
        event_num = event_num_arr[i]
        event_team = event_team_arr[i]
        event_text = event_text_arr[i]
        is_scoring_play = is_scoring_play_arr[i]
        is_extra_points = is_extra_points_arr[i]

        home_set_score = home_set_score_arr[i]
        away_set_score = away_set_score_arr[i]

        home_cumulative_score = home_cumulative_score_arr[i]
        away_cumulative_score = away_cumulative_score_arr[i]

        home_sets_won = home_sets_won_arr[i]
        away_sets_won = away_sets_won_arr[i]

        temp_df = pd.DataFrame(
            {
                "season": season,
                "game_id": game_id,
                "sport_id": sport_id,
                "game_datetime": game_datetime,
                "home_team_id": home_team_id,
                "home_team_name": home_team_name,
                "away_team_id": away_team_id,
                "away_team_name": away_team_name,
                "set_num": set_num,
                "event_num": event_num,
                "event_team": event_team,
                "event_text": event_text,
                "is_scoring_play": is_scoring_play,
                "is_extra_points": is_extra_points,
                "home_set_score": home_set_score,
                "away_set_score": away_set_score,
                "home_cumulative_score": home_cumulative_score,
                "away_cumulative_score": away_cumulative_score,
                "home_sets_won": home_sets_won,
                "away_sets_won": away_sets_won,
                "is_substitution": False,
                "is_sub_in": False,
                "is_sub_out": False,
                "substitution_player_1_id": None,
                "substitution_player_1_name": None,
                "substitution_player_2_id": None,
                "substitution_player_2_name": None,
                "substitution_player_3_id": None,
                "substitution_player_3_name": None,
                "substitution_player_4_id": None,
                "substitution_player_4_name": None,
                "is_timeout": False,
                "timeout_team": None,
                "is_starting_lineup": False,
                "is_serve": False,
                "is_service_error": False,
                "is_service_ace": False,
                "serve_player_id": None,
                "serve_player_name": None,
                "is_reception": False,
                "reception_type": None,
                "reception_player_id": None,
                "reception_player_name": None,
                "is_set": False,
                "set_type": None,
                "set_player_id": None,
                "set_player_name": None,
                "set_error_player_id": None,
                "set_error_player_name": None,
                "is_attack": False,
                "is_attack_error": False,
                "attack_type": None,
                "attack_player_id": None,
                "attack_player_name": None,
                "is_dig": False,
                "dig_player_id": None,
                "dig_player_name": None,
                "is_kill": False,
                "is_first_ball_kill": False,
                "kill_player_id": None,
                "kill_player_name": None,
                "is_block_attempt": False,
                "is_assisted_block": False,
                "is_block_error": False,
                "block_type": None,
                "block_player_1_id": None,
                "block_player_1_name": None,
                "block_player_2_id": None,
                "block_player_2_name": None,
                "is_ball_handling_error": False,
                "ball_handling_error_player_id": None,
                "ball_handling_error_player_name": None,
                "is_set_error": False,
                "is_dig_error": False,
                "dig_error_player_id": None,
                "dig_error_player_name": None,
                "is_challenge": False,
                "is_end_of_set": False,
                "is_end_of_match": False,
                "home_set_1_score": home_set_1_score,
                "away_set_1_score": away_set_1_score,
                "home_set_2_score": home_set_2_score,
                "away_set_2_score": away_set_2_score,
                "home_set_3_score": home_set_3_score,
                "away_set_3_score": away_set_3_score,
                "home_set_4_score": home_set_4_score,
                "away_set_4_score": away_set_4_score,
                "home_set_5_score": home_set_5_score,
                "away_set_5_score": away_set_5_score,
                "stadium_name": stadium_name,
                "attendance": attendance,
            },
            index=[0],
        )

        if "match started" in event_text.lower():
            pass
        elif "set started" in event_text.lower():
            pass
        elif "set ended" in event_text.lower():
            pass
        elif "match ended" in event_text.lower():
            pass
        elif "end match" in event_text.lower():
            temp_df["is_end_of_match"] = True
        elif event_text == "Team(Independent) by Team":
            # If this is the case, this is a data-side error,
            # and there's nothing we can parse here.
            # So let's skip it, and move on.
            continue
        elif (
            "end of" in event_text.lower() and
            "set" in event_text.lower()
        ):
            continue
        elif "end set " in event_text.lower():
            temp_df["is_end_of_set"] = True
        elif "media timeout" in event_text.lower():
            temp_df["is_timeout"] = True
        elif "facultative timeout" in event_text.lower():
            temp_df["is_timeout"] = True
        elif "timeout " in event_text.lower():
            play_arr = re.findall(
                r"Timeout ([a-zA-Z0-9\,\.\s\-\'\(\)]+)\.",
                event_text
            )
            temp_df["is_timeout"] = True
            temp_df["timeout_team"] = play_arr[0]
        elif "starters:" in event_text.lower():
            temp_df["is_starting_lineup"] = True
        elif "challenge" in event_text.lower():
            temp_df["is_challenge"] = True
        elif "sub in" in event_text.lower():
            play_arr = re.findall(
                r"Sub in ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_substitution"] = True
            temp_df["is_sub_in"] = True
            temp_df["substitution_player_1_name"] = play_arr[0]
        elif "sub out" in event_text.lower():
            play_arr = re.findall(
                r"Sub out ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_substitution"] = True
            temp_df["is_sub_out"] = True
            temp_df["substitution_player_1_name"] = play_arr[0]
        elif "substitution by" in event_text.lower():
            play_arr = re.findall(
                r"Substitution by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_substitution"] = True
            # temp_df["is_sub_out"] = True
            temp_df["substitution_player_1_name"] = play_arr[0]
        elif "subs:" in event_text.lower():
            player_1 = ""
            player_2 = ""
            try:
                play_arr = re.findall(
                    r"([a-zA-Z\.\s\-\'\(\)]+) subs: " +
                    r"([a-zA-Z0-9\,\.\s\-\'\(\)]+) " +
                    r"([a-zA-Z0-9\,\.\s\-\'\(\)]+)\.",
                    event_text
                )
                temp_df["is_substitution"] = True
                temp_df["is_sub_out"] = True
                temp_df["is_sub_in"] = True

                player_1 = play_arr[0][1]
                player_2 = play_arr[0][2]

            except Exception as e:
                logging.warning(e)
                # raise e
                play_arr = re.findall(
                    r"([a-zA-Z\.\s\-\'\(\)]+) subs: " +
                    r"([a-zA-Z0-9\,\.\s\-\'\(\)]+)\.",
                    event_text
                )
                temp_df["is_substitution"] = True
                temp_df["is_sub_out"] = True
                temp_df["is_sub_in"] = True
                player_1 = play_arr[0][1]

            if "," in player_1:
                player_arr = player_1.split(" ")
            else:
                player_arr = [player_1]
            if len(player_arr) == 4:
                temp_df["substitution_player_1_name"] = player_arr[0]
                temp_df["substitution_player_2_name"] = player_arr[1]
                temp_df["substitution_player_3_name"] = player_arr[2]
                temp_df["substitution_player_4_name"] = player_arr[3]
            elif len(player_arr) == 3:
                temp_df["substitution_player_1_name"] = player_arr[0]
                temp_df["substitution_player_2_name"] = player_arr[1]
                temp_df["substitution_player_3_name"] = player_arr[2]
            elif len(player_arr) > 4:
                raise ValueError(f"{player_arr}")
            else:
                temp_df["substitution_player_1_name"] = player_1
                temp_df["substitution_player_2_name"] = player_2
        elif "serves" in event_text.lower():
            play_arr = re.findall(
                r"([a-zA-Z0-9\,\.\s\-\'\(\)]+) serves",
                event_text
            )
            temp_df["is_serve"] = True
            temp_df["serve_player_name"] = play_arr[0]
        elif ") service ace" in event_text.lower():
            play_arr = re.findall(
                r"Point ([a-zA-Z\.\s\-\'\(\)]+): " +
                r"\(([a-zA-Z0-9\,\.\s\-\'\(\)]+)\) Service ace",
                event_text
            )
            temp_df["is_service_ace"] = True
            temp_df["serve_player_name"] = play_arr[0][1]
        elif ") service error" in event_text.lower():
            play_arr = re.findall(
                r"Point ([a-zA-Z\.\s\-\'\(\)]+): " +
                r"\(([a-zA-Z0-9\,\.\s\-\'\(\)]+)\) Service error\.",
                event_text
            )
            temp_df["is_service_error"] = True
            temp_df["serve_player_name"] = play_arr[0][1]
        elif "service error" in event_text.lower():
            play_arr = re.findall(
                r"([a-zA-Z0-9\,\.\s\-\'\(\)]+) service error",
                event_text
            )
            temp_df["is_service_error"] = True
            temp_df["serve_player_name"] = play_arr[0]
        elif "reception by" in event_text.lower():
            play_arr = re.findall(
                r"Reception by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_reception"] = True
            temp_df["reception_player_name"] = play_arr[0]
        elif "bad set by" in event_text.lower():
            play_arr = re.findall(
                r"Point ([a-zA-Z\.\s\-\'\(\)]+): " +
                r"\(([a-zA-Z0-9\,\.\s\-\']+)\) " +
                r"Bad set by ([a-zA-Z0-9\,\.\s\-\']+)\.",
                event_text
            )
            temp_df["is_set_error"] = True
            temp_df["set_error_player_name"] = play_arr[0][2]
        elif "set(" in event_text.lower() and ") by" in event_text.lower():
            play_arr = re.findall(
                r"set\(([a-zA-Z\s]+)\) by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_set"] = True
            temp_df["set_type"] = play_arr[0][0]
            temp_df["set_player_name"] = play_arr[0][1]
        elif "set by" in event_text.lower():
            play_arr = re.findall(
                r"Set by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_set"] = True
            temp_df["set_player_name"] = play_arr[0]
        elif "set error by" in event_text.lower():
            play_arr = re.findall(
                r"Set error by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_set_error"] = True
            temp_df["set_error_player_name"] = play_arr[0]
        elif "attack error by" in event_text.lower():
            play_arr = re.findall(
                r"Attack error by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            # temp_df["is_attack"] = True
            temp_df["is_attack_error"] = True
            temp_df["attack_player_name"] = play_arr[0]
        elif "attack(" in event_text.lower() and ") by" in event_text.lower():
            play_arr = re.findall(
                r"attack\(([a-zA-Z\s]+)\) by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_attack"] = True
            temp_df["attack_type"] = play_arr[0][0]
            temp_df["attack_player_name"] = play_arr[0][1]
        elif "attack by" in event_text.lower():
            play_arr = re.findall(
                r"Attack by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_attack"] = True
            temp_df["attack_player_name"] = play_arr[0]
        elif "dig by" in event_text.lower():
            play_arr = re.findall(
                r"Dig by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_dig"] = True
            temp_df["dig_player_name"] = play_arr[0]
        elif "dig error by" in event_text.lower():
            play_arr = re.findall(
                r"Dig error by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_dig_error"] = True
            temp_df["dig_error_player_name"] = play_arr[0]
        elif "first ball kill" in event_text.lower():
            play_arr = re.findall(
                r"First ball kill by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_kill"] = True
            temp_df["is_first_ball_kill"] = True
            temp_df["kill_player_name"] = play_arr[0]
        elif "kill by " in event_text.lower():
            play_arr = re.findall(
                r"Kill by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_kill"] = True
            temp_df["kill_player_name"] = play_arr[0]
        elif "block error by" in event_text.lower():
            play_arr = re.findall(
                r"Block error by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            # temp_df["is_block_attempt"] = True
            temp_df["is_block_error"] = True
            temp_df["block_player_1_name"] = play_arr[0]
        elif "block by" in event_text.lower():
            try:
                play_arr = re.findall(
                    r"Block by ([a-zA-Z0-9\,\.\s\-\'\(\)]+), " +
                    r"([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                    event_text
                )
                temp_df["is_block_attempt"] = True
                temp_df["is_assisted_block"] = True
                temp_df["block_player_1_name"] = play_arr[0][0]
                temp_df["block_player_2_name"] = play_arr[0][1]
            except Exception:
                play_arr = re.findall(
                    r"Block by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                    event_text
                )
                temp_df["is_block_attempt"] = True
                temp_df["block_player_1_name"] = play_arr[0]
        elif "block(" in event_text.lower() and ") by" in event_text.lower():
            play_arr = re.findall(
                r"block\(([a-zA-Z\s]+)\) by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_block_attempt"] = True
            temp_df["block_type"] = play_arr[0][0]
            temp_df["block_player_1_name"] = play_arr[0][0]
        elif "ball handling error by" in event_text.lower():
            play_arr = re.findall(
                r"Ball handling error by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["is_ball_handling_error"] = True
            temp_df["ball_handling_error_player_name"] = play_arr[0]
        elif (
            "reception(" in event_text.lower() and
            ") by" in event_text.lower()
        ):
            play_arr = re.findall(
                r"reception\(([a-zA-Z\s]+)\) by ([a-zA-Z0-9\,\.\s\-\'\(\)]+)",
                event_text
            )
            temp_df["reception_type"] = play_arr[0][0]
            temp_df["reception_player_name"] = play_arr[0][1]
        else:
            raise ValueError(f"Unhandled play `{event_text}`")

        pbp_df_arr.append(temp_df)

    pbp_df = pd.concat(pbp_df_arr, ignore_index=True)

    return pbp_df
