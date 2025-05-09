import re
import logging
from datetime import datetime
import pandas as pd
from tqdm import tqdm


def _get_yardline(yardline: str, is_posteam_side: bool):
    """ """
    try:
        yardline_temp = re.findall(r"([0-9]+)", yardline)[-1]
    except Exception as e:
        logging.info(
            f"Cannot get a yardline number with {yardline}." +
            f"Full exception {e}"
        )
        yardline_100 = yardline

    if len(str(yardline_temp)) == 4:
        yardline_temp = yardline_temp[-2:-1]
    elif len(str(yardline_temp)) == 3:
        yardline_temp = yardline_temp[-1]

    if (is_posteam_side is True) and ("end zone" in yardline.lower()):
        yardline_100 = 100
    elif (is_posteam_side is False) and ("end zone" in yardline.lower()):
        yardline_100 = 0
    elif is_posteam_side is True:
        yardline_temp = int(yardline_temp)
        yardline_100 = 100 - yardline_temp
    else:
        yardline_temp = int(yardline_temp)
        yardline_100 = 100 - (50 - yardline_temp) - 50

    return yardline_100


def _football_pbp_helper(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    NOT INTENDED TO BE CALLED DIRECTLY BY A USER!

    See `ncaa_stats_py.football.get_parsed_football_pbp()`
    """
    home_score = 0
    away_score = 0
    home_score_post = 0
    away_score_post = 0

    pbp_df = pd.DataFrame()
    pbp_df_arr = []

    season = raw_df["season"].iloc[0]
    game_id = raw_df["game_id"].iloc[0]
    sport_id = raw_df["sport_id"].iloc[0]
    # 2025-01-20T19:30:00-05:00
    # game_datetime = raw_df["game_datetime"].iloc[0],

    game_datetime = datetime.strptime(
        raw_df["game_datetime"].iloc[0],
        "%Y-%m-%dT%H:%M:%S%z"
    )
    game_date = game_datetime.strftime("%Y-%m-%d")
    stadium_name = raw_df["stadium_name"].iloc[0]
    attendance = raw_df["attendance"].iloc[0]

    home_team_id = raw_df["home_team_id"].iloc[0]
    away_team_id = raw_df["away_team_id"].iloc[0]

    home_school_id = raw_df["home_school_id"].iloc[0]
    away_school_id = raw_df["away_school_id"].iloc[0]

    home_team_name = raw_df["home_team_name"].iloc[0]
    away_team_name = raw_df["away_team_name"].iloc[0]

    home_team_abv = raw_df["home_team_abv"].iloc[0]
    away_team_abv = raw_df["away_team_abv"].iloc[0]

    drive_num_arr = raw_df["drive_num"].to_numpy()
    possession_team_arr = raw_df["possession_team"].to_numpy()
    defensive_team_arr = raw_df["defensive_team"].to_numpy()
    posteam_type_arr = raw_df["posteam_type"].to_numpy()
    quarter_num_arr = raw_df["quarter_num"].to_numpy()
    time_str_arr = raw_df["time_str"].to_numpy()
    yardline_100_arr = raw_df["yardline_100"].to_numpy()
    play_text_arr = raw_df["play_text"].to_numpy()
    down_arr = raw_df["down"].to_numpy()
    distance_arr = raw_df["distance"].to_numpy()
    yrdln_arr = raw_df["yrdln"].to_numpy()
    team_side_arr = raw_df["team_side"].to_numpy()
    is_overtime_arr = raw_df["is_overtime"].to_numpy()
    quarter_seconds_remaining_arr = raw_df[
        "quarter_seconds_remaining"
    ].to_numpy()
    half_seconds_remaining_arr = raw_df["half_seconds_remaining"].to_numpy()
    game_seconds_remaining_arr = raw_df["game_seconds_remaining"].to_numpy()
    play_id_arr = raw_df["event_num"].to_numpy()

    drive_str_arr = raw_df["drive_str"].to_numpy()
    drive_result_arr = raw_df["drive_result"].to_numpy()
    drive_plays_arr = raw_df["drive_plays"].to_numpy()
    drive_yards_arr = raw_df["drive_yards"].to_numpy()

    del raw_df

    for i in tqdm(range(0, len(play_id_arr))):
        is_no_play = False
        home_score = home_score_post
        away_score = away_score_post
        event_text = play_text_arr[i]
        tacklers_arr = []
        sack_players_arr = []
        temp_df = pd.DataFrame(
            {
                "season": season,
                "game_id": game_id,
                "sport_id": sport_id,
                "game_datetime": game_datetime,
                "game_date": game_date,
                "home_team_id": home_team_id,
                "home_school_id": home_school_id,
                "home_team_name": home_team_name,
                "away_team_id": away_team_id,
                "away_school_id": away_school_id,
                "away_team_name": away_team_name,
                "posteam": possession_team_arr[i],
                "defteam": defensive_team_arr[i],
                "posteam_type": posteam_type_arr[i],
                "quarter": quarter_num_arr[i],
                "quarter_seconds_remaining": quarter_seconds_remaining_arr[i],
                "half_seconds_remaining": half_seconds_remaining_arr[i],
                "game_seconds_remaining": game_seconds_remaining_arr[i],
                "down": down_arr[i],
                "ydstogo": distance_arr[i],
                "time": time_str_arr[i],
                "yrdln": yrdln_arr[i],
                "yardline_100": yardline_100_arr[i],
                "desc": event_text,
                "side_of_field": team_side_arr[i],
                "home_score": home_score,
                "away_score": away_score,
                "home_score_post": home_score_post,
                "away_score_post": away_score_post,
                "kick_distance": None,
                "two_point_conv_result": None,
                "return_team": None,
                "return_yards": None,
                "td_team": None,
                "td_player_id": None,
                "td_player_name": None,
                "yards_gained": None,
                "extra_point_result": None,
                "pass_length": None,
                "pass_location": None,
                "air_yards": None,
                "yards_after_catch": None,
                "run_location": None,
                "run_gap": None,
                "field_goal_result": None,
                "passer_player_id": None,
                "passer_player_name": None,
                "passing_yards": None,
                "receiver_player_id": None,
                "receiver_player_name": None,
                "receiving_yards": None,
                "rusher_player_id": None,
                "rusher_player_name": None,
                "rushing_yards": None,
                "lateral_receiver_player_id": None,
                "lateral_receiver_player_name": None,
                "lateral_receiving_yards": None,
                "lateral_rusher_player_id": None,
                "lateral_rusher_player_name": None,
                "lateral_rushing_yards": None,
                "lateral_sack_player_id": None,
                "lateral_sack_player_name": None,
                "interception_player_id": None,
                "interception_player_name": None,
                "lateral_interception_player_id": None,
                "lateral_interception_player_name": None,
                "punt_returner_player_id": None,
                "punt_returner_player_name": None,
                "lateral_punt_returner_player_id": None,
                "lateral_punt_returner_player_name": None,
                "kickoff_returner_player_name": None,
                "kickoff_returner_player_id": None,
                "lateral_kickoff_returner_player_id": None,
                "lateral_kickoff_returner_player_name": None,
                "punter_player_id": None,
                "punter_player_name": None,
                "kicker_player_name": None,
                "kicker_player_id": None,
                "own_kickoff_recovery_player_id": None,
                "own_kickoff_recovery_player_name": None,
                "blocked_player_id": None,
                "blocked_player_name": None,
                "long_snapper_player_id": None,
                "long_snapper_player_name": None,
                "holder_player_id": None,
                "holder_player_name": None,
                "tackle_for_loss_1_player_id": None,
                "tackle_for_loss_1_player_name": None,
                "tackle_for_loss_2_player_id": None,
                "tackle_for_loss_2_player_name": None,
                "qb_hit_1_player_id": None,
                "qb_hit_1_player_name": None,
                "qb_hit_2_player_id": None,
                "qb_hit_2_player_name": None,
                "forced_fumble_player_1_team": None,
                "forced_fumble_player_1_player_id": None,
                "forced_fumble_player_1_player_name": None,
                "forced_fumble_player_2_team": None,
                "forced_fumble_player_2_player_id": None,
                "forced_fumble_player_2_player_name": None,
                "solo_tackle_1_team": None,
                "solo_tackle_2_team": None,
                "solo_tackle_1_player_id": None,
                "solo_tackle_2_player_id": None,
                "solo_tackle_1_player_name": None,
                "solo_tackle_2_player_name": None,
                "assist_tackle_1_player_id": None,
                "assist_tackle_1_player_name": None,
                "assist_tackle_1_team": None,
                "assist_tackle_2_player_id": None,
                "assist_tackle_2_player_name": None,
                "assist_tackle_2_team": None,
                "assist_tackle_3_player_id": None,
                "assist_tackle_3_player_name": None,
                "assist_tackle_3_team": None,
                "assist_tackle_4_player_id": None,
                "assist_tackle_4_player_name": None,
                "assist_tackle_4_team": None,
                "tackle_with_assist": None,
                "tackle_with_assist_1_player_id": None,
                "tackle_with_assist_1_player_name": None,
                "tackle_with_assist_1_team": None,
                "tackle_with_assist_2_player_id": None,
                "tackle_with_assist_2_player_name": None,
                "tackle_with_assist_2_team": None,
                "pass_defense_1_player_id": None,
                "pass_defense_1_player_name": None,
                "pass_defense_2_player_id": None,
                "pass_defense_2_player_name": None,
                "fumbled_1_team": None,
                "fumbled_1_player_id": None,
                "fumbled_1_player_name": None,
                "fumbled_2_player_id": None,
                "fumbled_2_player_name": None,
                "fumbled_2_team": None,
                "fumble_recovery_1_team": None,
                "fumble_recovery_1_yards": None,
                "fumble_recovery_1_player_id": None,
                "fumble_recovery_1_player_name": None,
                "fumble_recovery_2_team": None,
                "fumble_recovery_2_yards": None,
                "fumble_recovery_2_player_id": None,
                "fumble_recovery_2_player_name": None,
                "sack_player_id": None,
                "sack_player_name": None,
                "half_sack_1_player_id": None,
                "half_sack_1_player_name": None,
                "half_sack_2_player_id": None,
                "half_sack_2_player_name": None,
                "penalty_team": None,
                "penalty_player_id": None,
                "penalty_player_name": None,
                "penalty_yards": None,
                "replay_or_challenge": None,
                "replay_or_challenge_result": None,
                "penalty_type": None,
                "is_punt_blocked": False,
                "is_first_down_rush": False,
                "is_first_down_pass": False,
                "is_first_down_penalty": False,
                "is_third_down_converted": False,
                "is_third_down_failed": False,
                "is_fourth_down_converted": False,
                "is_fourth_down_failed": False,
                "is_incomplete_pass": False,
                "is_touchback": False,
                "is_interception": False,
                "is_punt_inside_twenty": False,
                "is_punt_in_endzone": False,
                "is_punt_out_of_bounds": False,
                "is_punt_downed": False,
                "is_punt_fair_catch": False,
                "is_kickoff_inside_twenty": False,
                "is_kickoff_in_endzone": False,
                "is_kickoff_out_of_bounds": False,
                "is_kickoff_downed": False,
                "is_kickoff_fair_catch": False,
                "is_fumble_forced": False,
                "is_fumble_not_forced": False,
                "is_fumble_out_of_bounds": False,
                "is_solo_tackle": False,
                "is_safety": False,
                "is_penalty": False,
                "is_tackled_for_loss": False,
                "is_fumble_lost": False,
                "is_own_kickoff_recovery": False,
                "is_own_kickoff_recovery_td": False,
                "is_qb_hit": False,
                "is_rush_attempt": False,
                "is_pass_attempt": False,
                "is_sack": False,
                "is_touchdown": False,
                "is_pass_touchdown": False,
                "is_rush_touchdown": False,
                "is_return_touchdown": False,
                "is_extra_point_attempt": False,
                "is_one_point_attempt": False,
                "is_one_point_attempt_success": False,
                "is_two_point_attempt": False,
                "is_two_point_attempt_success": False,
                "is_three_point_attempt": False,
                "is_three_point_attempt_success": False,
                "is_field_goal_attempt": False,
                "is_kickoff_attempt": False,
                "is_punt_attempt": False,
                "is_fumble": False,
                "is_complete_pass": False,
                "is_assist_tackle": False,
                "is_lateral_reception": False,
                "is_lateral_rush": False,
                "is_lateral_return": False,
                "is_lateral_recovery": False,
                "drive_num": drive_num_arr[i],
                "drive_str": drive_str_arr[i],
                "drive_result": drive_result_arr[i],
                "drive_plays": drive_plays_arr[i],
                "drive_yards": drive_yards_arr[i],
                "is_overtime": is_overtime_arr[i],
                "stadium_name": stadium_name,
                "attendance": attendance,
            },
            index=[0],
        )

        if "shotgun-no huddle" in event_text.lower():
            temp_df["is_shotgun"] = True
            temp_df["is_no_huddle"] = True
            event_text = event_text.replace("Shotgun-No Huddle", "")
        if "no huddle-shotgun" in event_text.lower():
            temp_df["is_shotgun"] = True
            temp_df["is_no_huddle"] = True
            event_text = event_text.replace("No Huddle-Shotgun", "")
        if "shotgun" in event_text.lower():
            temp_df["is_shotgun"] = True
            event_text = event_text.replace("Shotgun", "")
        if "no huddle" in event_text.lower():
            temp_df["is_no_huddle"] = True
            event_text = event_text.replace("No Huddle", "")
        if "first down" in event_text.lower():
            temp_df["is_first_down"] = True
        if ", 1st down" in event_text.lower():
            temp_df["is_first_down"] = True
            event_text = re.sub(
                r", 1ST DOWN ([a-zA-Z0-9]+)",
                "",
                event_text
            )
            event_text = event_text.replace("  ", " ")
        if "1st down" in event_text.lower():
            temp_df["is_first_down"] = True
            event_text = re.sub(
                r"1ST DOWN ([a-zA-Z0-9]+)",
                "",
                event_text
            )
            event_text = event_text.replace("  ", " ")
        if "no play" in event_text.lower():
            is_no_play = True
            temp_df["is_no_play"] = is_no_play
        if "yards gain (" in event_text.lower():
            event_text = re.sub(
                r"yards gain \(([0-9]+)\)",
                "yards gain",
                event_text
            )
        if "qb hurried by" in event_text.lower():
            play_arr = re.findall(
                r"QB hurried by ([a-zA-Z\.\,\'\-]+) ",
                event_text
            )
            if len(play_arr) == 0:
                play_arr = re.findall(
                    r"QB hurried by ([a-zA-Z\.\,\'\-]+)\.",
                    event_text
                )
                temp_df["qb_hit_1_player_name"] = play_arr[0]
                event_text = re.sub(
                    r"QB hurried by ([a-zA-Z\.\,\'\-]+)\.",
                    "",
                    event_text
                )
            else:
                temp_df["qb_hit_1_player_name"] = play_arr[0]
                event_text = re.sub(
                    r"QB hurried by ([a-zA-Z\.\,\'\-]+) ",
                    "",
                    event_text
                )
        if "3a" in event_text.lower():
            event_text = event_text.replace("3a", ";")
        if "50 yardline24" in event_text.lower():
            event_text = event_text.replace("50 yardline24", "50")
        if "50 yardline" in event_text.lower():
            event_text = event_text.replace("50 yardline", "50")

        if "drive start" in event_text.lower():
            temp_df["is_drive_start"] = True
        elif (
            "end of half" in event_text.lower()
        ):
            pass
        elif (
            "start of" in event_text.lower() and
            "quarter" in event_text.lower()
        ):
            temp_df["is_quarter_start"] = True
        elif ("end of game" in event_text.lower()):
            temp_df["is_end_of_game"] = True
        elif (
            "will receive" in event_text.lower() and
            "will defend" in event_text.lower()
        ):
            pass
        # Passing
        elif (
            "pass incomplete" in event_text.lower() and
            "penalty" in event_text.lower() and
            "no play" in event_text.lower()
        ):
            # we can handle the play later
            pass
        elif (
            "pass incomplete" in event_text.lower() and
            "thrown to" in event_text.lower() and
            event_text.count("to") == 1
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_incomplete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass incomplete " +
                r"([a-zA-Z0-9]+) ([a-zA-Z0-9]+) " +
                r"thrown to ([A-Z0-9]+)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            # temp_df["receiver_player_name"] = play_arr[0][3]

            temp_yd_line = play_arr[0][3]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            del temp_yd_line
        elif (
            "pass intercepted" in event_text.lower() and
            "broken up by" in event_text.lower() and
            (
                "end of play" in event_text.lower() or
                "end-of-play" in event_text.lower()
            )
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_incomplete_pass"] = True
            temp_df["is_interception"] = True
            # temp_df["is_touchback"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass intercepted by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) broken up by ([a-zA-Z\.\,\'\s\-]+)\, [END|end]+ [OF|of]+ [PLAY|play]+",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["interception_player_name"] = play_arr[0][1]
            temp_df["pass_defense_1_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = 0
        elif (
            "pass intercepted" in event_text.lower() and
            "return" in event_text.lower() and
            "touchback" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_incomplete_pass"] = True
            temp_df["is_interception"] = True
            temp_df["is_touchback"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass intercepted by ([a-zA-Z\.\,\'\s\-]+) at the ([A-Z0-9]+)\, ([a-zA-Z\.\,\'\s\-]+) return ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, [TOUCHBACK|touchback]+",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["interception_player_name"] = play_arr[0][1]
            temp_df["return_yards"] = int(play_arr[0][4])

            temp_yd_line = play_arr[0][2]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            del temp_yd_line
        elif (
            "pass intercepted" in event_text.lower() and
            "touchback" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_incomplete_pass"] = True
            temp_df["is_interception"] = True
            temp_df["is_touchback"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass intercepted by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)\, [TOUCHBACK|touchback]+",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["interception_player_name"] = play_arr[0][1]
            temp_df["return_yards"] = 0

            temp_yd_line = play_arr[0][2]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            del temp_yd_line
        elif (
            "pass intercepted" in event_text.lower() and
            "return" in event_text.lower() and
            (
                "out of bounds" in event_text.lower() or
                "out-of-bounds" in event_text.lower()
            )
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_incomplete_pass"] = True
            temp_df["is_interception"] = True
            # temp_df["is_touchback"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass intercepted by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) return ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+ at ([a-zA-Z0-9]+)",
                event_text
            )

            if len(play_arr) == 0:
                play_arr = re.findall(
                    r"([a-zA-Z\.\,\'\s\-]+) pass intercepted by ([a-zA-Z\.\,\'\s\-]+) at [the]+ ?([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+",
                    event_text
                )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["interception_player_name"] = play_arr[0][1]
            temp_df["return_yards"] = play_arr[0][4]
        elif (
            "pass intercepted" in event_text.lower() and
            "touchdown" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_interception"] = True
            temp_df["is_return_touchdown"] = True
            temp_df["is_incomplete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass intercepted by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) return ([\-0-9]+) yard[s]? to the ([A-Z0-9]+) TOUCHDOWN",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["interception_player_name"] = play_arr[0][1]
            temp_df["return_yards"] = int(play_arr[0][4])

            temp_yd_line = play_arr[0][2]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            del temp_yd_line
        elif (
            "pass intercepted" in event_text.lower() and
            "return" in event_text.lower() and
            "end of play" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_interception"] = True
            temp_df["is_return_touchdown"] = True
            temp_df["is_incomplete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass intercepted by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) return ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, [END|end]+ [OF|of]+ [PLAY|play]+\.",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["interception_player_name"] = play_arr[0][1]
            temp_df["return_yards"] = int(play_arr[0][4])

            temp_yd_line = play_arr[0][2]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            del temp_yd_line
        elif (
            "pass intercepted" in event_text.lower() and
            "return" in event_text.lower() and
            "(" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_incomplete_pass"] = True
            temp_df["is_interception"] = True
            # temp_df["is_touchback"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass intercepted by ([a-zA-Z\.\,\'\s\-]+) at[ the]+ ([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([0-9\-]+) yard[s]? to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["interception_player_name"] = play_arr[0][1]
            temp_df["return_yards"] = int(play_arr[0][4])

            tacklers_arr = play_arr[0][6]
        elif (
            "pass intercepted" in event_text.lower() and
            "return" in event_text.lower() and
            "(" not in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_incomplete_pass"] = True
            temp_df["is_interception"] = True
            # temp_df["is_touchback"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass intercepted by ([a-zA-Z\.\,\'\s\-]+) at[ the]+ ([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([0-9\-]+) yard[s]? to the ([A-Z0-9]+)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["interception_player_name"] = play_arr[0][1]
            temp_df["return_yards"] = int(play_arr[0][4])

            # tacklers_arr = play_arr[0][6]
        elif (
            "pass intercepted" in event_text.lower() and
            "end of play" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_interception"] = True
            temp_df["is_return_touchdown"] = True
            temp_df["is_incomplete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass intercepted by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)\,? [END|end]+ [OF|of]+ [PLAY|play]+",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["interception_player_name"] = play_arr[0][1]
            temp_df["return_yards"] = 0
        elif (
            "pass incomplete" in event_text.lower() and
            "thrown to" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_incomplete_pass"] = True
            temp_df["is_incomplete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass incomplete " +
                r"([a-zA-Z0-9]+) ([a-zA-Z0-9]+) to ([a-zA-Z\.\,\'\s\-]+) " +
                r"thrown to ([A-Z0-9]+)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]

            temp_yd_line = play_arr[0][4]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            del temp_yd_line
        elif (
            "sacked for loss" in event_text.lower()
        ):
            temp_df["is_sack"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) sacked for loss of " +
                r"([\-0-9]+) yard[s]? to the ([A-Z0-9]+) " +
                r"\(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["yards_gained"] = int(play_arr[0][1]) * -1
            temp_df["rushing_yards"] = int(play_arr[0][1]) * -1
            temp_df["sack_yards"] = int(play_arr[0][1]) * -1
            tacklers_arr = play_arr[0][3]
        elif (
            "pass complete" in event_text.lower() and
            "caught at" in event_text.lower() and
            "out of bounds at" in event_text.lower()
        ):
            temp_df["is_out_of_bounds"] = True
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete " +
                r"([a-zA-Z0-9]+) ([a-zA-Z0-9]+) to ([a-zA-Z\.\,\'\s\-]+) " +
                r"caught at ([a-zA-Z0-9]+)\, for ([\-0-9]+) yard[s]? " +
                r"to the ([a-zA-Z0-9]+), out of bounds at ([a-zA-Z0-9]+)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][5])
            temp_df["receiving_yards"] = int(play_arr[0][5])
            temp_df["yards_gained"] = int(play_arr[0][5])
            temp_yd_line = play_arr[0][4]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["yards_after_catch"] = (
                temp_df["receiving_yards"] - temp_df["air_yards"]
            )

            del temp_yd_line
        elif (
            "pass complete" in event_text.lower() and
            "caught at" in event_text.lower() and
            "touchdown" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True
            temp_df["is_touchdown"] = True
            temp_df["is_pass_touchdown"] = True
            temp_df["td_team"] = possession_team_arr[i]

            if is_no_play is True:
                pass
            elif possession_team_arr[i] == away_team_id:
                away_score_post += 6
            elif possession_team_arr[i] == home_team_id:
                home_score_post += 6

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass " +
                r"complete ([a-zA-Z0-9]+) ([a-zA-Z0-9]+) " +
                r"to ([a-zA-Z\.\,\'\s\-]+) caught at ([a-zA-Z0-9]+)\, " +
                r"for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+) " +
                r"TOUCHDOWN",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["td_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][5])
            temp_df["receiving_yards"] = int(play_arr[0][5])
            temp_df["yards_gained"] = int(play_arr[0][5])
            temp_yd_line = play_arr[0][4]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["yards_after_catch"] = (
                temp_df["receiving_yards"] - temp_df["air_yards"]
            )
            del temp_yd_line
        elif (
            "pass complete" in event_text.lower() and
            "caught at" in event_text.lower() and
            "end of play" in event_text.lower() and
            "fumbled by" in event_text.lower() and
            "forced by" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "end of play" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True
            temp_df["is_no_tackle"] = True
            temp_df["fumbled_1_team"] = possession_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete " +
                r"([a-zA-Z0-9]+) ([a-zA-Z0-9]+) to ([a-zA-Z\.\,\'\s\-]+) " +
                r"caught at ([a-zA-Z0-9]+)\, for ([\-0-9]+) yard[s]? " +
                r"to the ([a-zA-Z0-9]+) fumbled by ([a-zA-Z\.\,\'\s\-]+) " +
                r"at ([a-zA-Z0-9]+) forced by ([a-zA-Z\.\,\'\s\-]+) " +
                r"recovered by ([a-zA-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) " +
                r"at ([a-zA-Z0-9]+), End Of Play",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][5])
            temp_df["receiving_yards"] = int(play_arr[0][5])
            temp_df["yards_gained"] = int(play_arr[0][5])
            temp_yd_line = play_arr[0][4]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["yards_after_catch"] = (
                temp_df["receiving_yards"] - temp_df["air_yards"]
            )
            temp_df["fumbled_1_player_name"] = play_arr[0][7]
            temp_df["forced_fumble_player_1_team"] = defensive_team_arr[i]
            temp_df["forced_fumble_player_1_player_name"] = play_arr[0][9]
            temp_df["fumble_recovery_1_team"] = play_arr[0][10]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][11]
        elif (
            "pass complete" in event_text.lower() and
            "caught at" in event_text.lower() and
            "end of play" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True
            temp_df["is_no_tackle"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete " +
                r"([a-zA-Z0-9]+) ([a-zA-Z0-9]+) to ([a-zA-Z\.\,\'\s\-]+) " +
                r"caught at ([a-zA-Z0-9]+)\, for ([\-0-9]+) yard[s]? " +
                r"to the ([a-zA-Z0-9]+), " +
                r"[End|end]+ [Of|of]+ [Play|play]+",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][5])
            temp_df["receiving_yards"] = int(play_arr[0][5])
            temp_df["yards_gained"] = int(play_arr[0][5])
            temp_yd_line = play_arr[0][4]

            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)

            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["yards_after_catch"] = (
                temp_df["receiving_yards"] - temp_df["air_yards"]
            )
        elif (
            "pass complete" in event_text.lower() and
            "caught at" in event_text.lower() and
            (
                "yard loss to the" in event_text.lower() or
                "yards loss to the" in event_text.lower()
            )
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass " +
                r"complete ([a-zA-Z0-9]+) ([a-zA-Z0-9]+) " +
                r"to ([a-zA-Z\.\,\'\s\-]+) caught at ([a-zA-Z0-9]+)\, " +
                r"for ([\-0-9]+) yard[s]? loss to the ([a-zA-Z0-9]+) " +
                r"\(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][5]) * -1
            temp_df["receiving_yards"] = int(play_arr[0][5]) * -1
            temp_df["yards_gained"] = int(play_arr[0][5]) * -1
            temp_yd_line = play_arr[0][4]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["yards_after_catch"] = (
                temp_df["receiving_yards"] - temp_df["air_yards"]
            )
            tacklers_arr = play_arr[0][7]

            del temp_yd_line
        elif (
            "pass complete" in event_text.lower() and
            "caught at" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass " +
                r"complete ([a-zA-Z0-9]+) ([a-zA-Z0-9]+) " +
                r"to ([a-zA-Z\.\,\'\s\-]+) caught at ([a-zA-Z0-9]+)\, " +
                r"for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+) " +
                r"\(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][5])
            temp_df["receiving_yards"] = int(play_arr[0][5])
            temp_df["yards_gained"] = int(play_arr[0][5])
            temp_yd_line = play_arr[0][4]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["yards_after_catch"] = (
                temp_df["receiving_yards"] - temp_df["air_yards"]
            )
            tacklers_arr = play_arr[0][7]

            del temp_yd_line
        elif (
            "pass complete" in event_text.lower() and
            "fumble forced by" in event_text.lower() and
            "fumble by" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "touchdown" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True
            temp_df["is_fumble"] = True
            temp_df["is_fumble_forced"] = True
            temp_df["is_return_touchdown"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+)to the ([a-zA-Z0-9]+)\, fumble forced by ([a-zA-Z\.\,\'\s\-]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([a-zA-Z0-9]+)\, ([a-zA-Z\.\,\'\s\-]+) for ([0-9\-]+) yard[s]? to the ([a-zA-Z0-9]+)\, TOUCHDOWN",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_yd_line = play_arr[0][2]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["receiver_player_name"] = play_arr[0][1]
            temp_df["passing_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["receiving_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["yards_gained"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["forced_fumble_player_1_player_name"] = play_arr[0][3]
            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][4]
            temp_df["fumble_recovery_1_team"] = play_arr[0][5]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][6]
            temp_df["fumble_recovery_1_yards"] = int(play_arr[0][9])
        elif (
            "pass complete" in event_text.lower() and
            "touchdown" in event_text.lower() and
            (
                " left" in event_text.lower() or
                " middle" in event_text.lower() or
                " right" in event_text.lower()
            )
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True
            temp_df["is_touchdown"] = True
            temp_df["is_pass_touchdown"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete ([a-z]+) ([a-z]+) to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+)\,? TOUCHDOWN",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][4])
            temp_df["receiving_yards"] = int(play_arr[0][4])
            temp_df["yards_gained"] = int(play_arr[0][4])
        elif (
            "pass complete" in event_text.lower() and
            "touchdown" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True
            temp_df["is_touchdown"] = True
            temp_df["is_pass_touchdown"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+)\,? TOUCHDOWN",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            # temp_df["pass_length"] = play_arr[0][1]
            # temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][1]
            temp_df["passing_yards"] = int(play_arr[0][2])
            temp_df["receiving_yards"] = int(play_arr[0][2])
            temp_df["yards_gained"] = int(play_arr[0][2])
        elif (
            "pass complete for loss of" in event_text.lower() and
            "(" not in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete for loss of ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            # temp_df["pass_length"] = play_arr[0][1]
            # temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = possession_team_arr[i]
            temp_df["passing_yards"] = int(play_arr[0][1]) * -1
            temp_df["receiving_yards"] = int(play_arr[0][1]) * -1
            temp_df["yards_gained"] = int(play_arr[0][1]) * -1
            # tacklers_arr = play_arr[0][4]
        elif (
            "pass complete" in event_text.lower() and
            "for loss of" in event_text.lower() and
            (
                "out-of-bounds" in event_text.lower() or
                "out of bounds" in event_text.lower()
            ) and
            "(" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+) for loss of ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+)\, [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+ \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            # temp_df["pass_length"] = play_arr[0][1]
            # temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][1]
            temp_df["passing_yards"] = int(play_arr[0][2]) * -1
            temp_df["receiving_yards"] = int(play_arr[0][2]) * -1
            temp_df["yards_gained"] = int(play_arr[0][2]) * -1
            tacklers_arr = play_arr[0][4]
        elif (
            "pass complete" in event_text.lower() and
            "for loss of" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+) for loss of ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            # temp_df["pass_length"] = play_arr[0][1]
            # temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][1]
            temp_df["passing_yards"] = int(play_arr[0][2]) * -1
            temp_df["receiving_yards"] = int(play_arr[0][2]) * -1
            temp_df["yards_gained"] = int(play_arr[0][2]) * -1
            tacklers_arr = play_arr[0][4]
        elif (
            "pass complete" in event_text.lower() and
            (
                "out-of-bounds at" in event_text.lower() or
                "out of bounds at" in event_text.lower()
            ) and
            (
                " left" in event_text.lower() or
                " middle" in event_text.lower() or
                " right" in event_text.lower()
            ) and
            ")." in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+), [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+ at ([a-zA-Z0-9]+)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][4])
            temp_df["receiving_yards"] = int(play_arr[0][4])
            temp_df["yards_gained"] = int(play_arr[0][4])
            # tacklers_arr = play_arr[0][4]
        elif (
            "pass complete" in event_text.lower() and
            (
                "out-of-bounds at" in event_text.lower() or
                "out of bounds at" in event_text.lower()
            ) and
            (
                " left" in event_text.lower() or
                " middle" in event_text.lower() or
                " right" in event_text.lower()
            )
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete ([a-zA-Z]+) ([a-zA-Z]+) to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+), [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+ at ([a-zA-Z0-9]+)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][4])
            temp_df["receiving_yards"] = int(play_arr[0][4])
            temp_df["yards_gained"] = int(play_arr[0][4])
            # tacklers_arr = play_arr[0][4]
        elif (
            "pass complete" in event_text.lower() and
            (
                "out-of-bounds at" in event_text.lower() or
                "out of bounds at" in event_text.lower()
            )
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+), [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+ at ([a-zA-Z0-9]+)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            # temp_df["pass_length"] = play_arr[0][1]
            # temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][1]
            temp_df["passing_yards"] = int(play_arr[0][2])
            temp_df["receiving_yards"] = int(play_arr[0][2])
            temp_df["yards_gained"] = int(play_arr[0][2])
            tacklers_arr = play_arr[0][4]
        elif (
            "pass complete" in event_text.lower() and
            (
                "out-of-bounds" in event_text.lower() or
                "out of bounds" in event_text.lower()
            ) and
            (
                ")." in event_text.lower() or
                "), penalty" in event_text.lower()
            )
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+), [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+ \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            # temp_df["pass_length"] = play_arr[0][1]
            # temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][1]
            temp_df["passing_yards"] = int(play_arr[0][2])
            temp_df["receiving_yards"] = int(play_arr[0][2])
            temp_df["yards_gained"] = int(play_arr[0][2])
            tacklers_arr = play_arr[0][4]
        elif (
            "pass complete" in event_text.lower() and
            "(" not in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            # temp_df["pass_length"] = play_arr[0][1]
            # temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][1]
            temp_df["passing_yards"] = int(play_arr[0][2])
            temp_df["receiving_yards"] = int(play_arr[0][2])
            temp_df["yards_gained"] = int(play_arr[0][2])
            # tacklers_arr = play_arr[0][4]
        elif (
            "pass complete" in event_text.lower() and
            (
                " left" in event_text.lower() or
                " middle" in event_text.lower() or
                " right" in event_text.lower()
            ) and "end of play" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete ([a-z]+) ([a-z]+) to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+)\, [END|end]+ [OF|of]+ [PLAY|play]+",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][4])
            temp_df["receiving_yards"] = int(play_arr[0][4])
            temp_df["yards_gained"] = int(play_arr[0][4])
            # tacklers_arr = play_arr[0][6]
        elif (
            "pass complete" in event_text.lower() and
            (
                " left" in event_text.lower() or
                " middle" in event_text.lower() or
                " right" in event_text.lower()
            ) and (
                "yards loss" in event_text.lower() or
                "yard loss" in event_text.lower()
            )
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete ([a-z]+) ([a-z]+) to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? loss to the ([a-zA-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][4]) * -1
            temp_df["receiving_yards"] = int(play_arr[0][4]) * -1
            temp_df["yards_gained"] = int(play_arr[0][4]) * -1
            tacklers_arr = play_arr[0][6]
        elif (
            "pass complete" in event_text.lower() and
            (
                " left" in event_text.lower() or
                " middle" in event_text.lower() or
                " right" in event_text.lower()
            )
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete ([a-z]+) ([a-z]+) to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["pass_length"] = play_arr[0][1]
            temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][3]
            temp_df["passing_yards"] = int(play_arr[0][4])
            temp_df["receiving_yards"] = int(play_arr[0][4])
            temp_df["yards_gained"] = int(play_arr[0][4])
            tacklers_arr = play_arr[0][6]
        elif (
            "pass complete" in event_text.lower() and
            (
                "yards loss" in event_text.lower() or
                "yard loss" in event_text.lower()
            )
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? loss to the ([a-zA-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            # temp_df["pass_length"] = play_arr[0][1]
            # temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][1]
            temp_df["passing_yards"] = int(play_arr[0][2]) * -1
            temp_df["receiving_yards"] = int(play_arr[0][2]) * -1
            temp_df["yards_gained"] = int(play_arr[0][2]) * -1
            tacklers_arr = play_arr[0][4]
        elif (
            "pass complete" in event_text.lower() and
            "for no gain" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+) for no gain to the ([a-zA-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            # temp_df["pass_length"] = play_arr[0][1]
            # temp_df["pass_location"] = play_arr[0][2]
            temp_df["receiver_player_name"] = play_arr[0][1]
            temp_df["passing_yards"] = 0
            temp_df["receiving_yards"] = 0
            temp_df["yards_gained"] = 0
            tacklers_arr = play_arr[0][3]
        elif (
            "pass complete" in event_text.lower() and
            " for " not in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["receiver_player_name"] = play_arr[0][1]
        elif (
            "pass complete" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_complete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            if len(play_arr) == 0:
                play_arr = re.findall(
                    r"([a-zA-Z\.\,\'\s\-]+) pass complete to ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+)",
                    event_text
                )
                temp_df["passer_player_name"] = play_arr[0][0]
                # temp_df["pass_length"] = play_arr[0][1]
                # temp_df["pass_location"] = play_arr[0][2]
                temp_df["receiver_player_name"] = play_arr[0][1]
                temp_df["passing_yards"] = int(play_arr[0][2])
                temp_df["receiving_yards"] = int(play_arr[0][2])
                temp_df["yards_gained"] = int(play_arr[0][2])
            else:
                temp_df["passer_player_name"] = play_arr[0][0]
                # temp_df["pass_length"] = play_arr[0][1]
                # temp_df["pass_location"] = play_arr[0][2]
                temp_df["receiver_player_name"] = play_arr[0][1]
                temp_df["passing_yards"] = int(play_arr[0][2])
                temp_df["receiving_yards"] = int(play_arr[0][2])
                temp_df["yards_gained"] = int(play_arr[0][2])
                tacklers_arr = play_arr[0][4]
        elif (
            "pass incomplete" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_pass_attempt"] = True
            temp_df["is_incomplete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass incomplete \(([a-zA-Z\.\,\'\s\-]+)\)",
                event_text
            )
            if len(play_arr) == 0:
                play_arr = re.findall(
                    r"([a-zA-Z\.\,\'\s\-]+) pass incomplete",
                    event_text
                )
                temp_df["passer_player_name"] = play_arr[0][0]
            else:
                temp_df["passer_player_name"] = play_arr[0][0]
                temp_df["receiver_player_name"] = play_arr[0][1]
        # Passing (sacks)
        elif (
            "sacked for" in event_text.lower()
        ):
            temp_df["is_qb_dropback"] = True
            temp_df["is_sack"] = True
            # temp_df["is_incomplete_pass"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) sacked for ([\-0-9]+) yard[s]? to the ([a-zA-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["yards_gained"] = int(play_arr[0][1]) * -1
            tacklers_arr = play_arr[0][3]
            sack_players_arr = play_arr[0][3]
        # Rushing
        elif (
            "rush" in event_text.lower() and
            (
                "gain" not in event_text.lower() and
                "loss" not in event_text.lower()
            ) and
            (
                "left" not in event_text.lower() and
                "right" not in event_text.lower() and
                "middle" not in event_text.lower()
            ) and (
                "out-of-bounds" in event_text.lower() or
                "out of bounds" in event_text.lower()
            ) and
            ")." in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for ([\-0-9]+) yard[s]? to the ([A-Z0-9]+), [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+ \(([a-zA-Z\.\,\;\'\s\-]+)\)\.",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][1])
            temp_df["yards_gained"] = int(play_arr[0][1])

            tacklers_arr = play_arr[0][3]
        elif (
            "rush" in event_text.lower() and
            (
                "gain" not in event_text.lower() and
                "loss" not in event_text.lower()
            ) and
            (
                "left" not in event_text.lower() and
                "right" not in event_text.lower() and
                "middle" not in event_text.lower()
            ) and
            "touchdown" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            temp_df["is_touchdown"] = True
            temp_df["is_rush_touchdown"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, TOUCHDOWN",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][1])
            temp_df["yards_gained"] = int(play_arr[0][1])

            # tacklers_arr = play_arr[0][3]
        elif (
            "rush for" in event_text.lower() and
            (
                "gain" not in event_text.lower() and
                "loss" not in event_text.lower()
            ) and
            (
                "left" not in event_text.lower() and
                "right" not in event_text.lower() and
                "middle" not in event_text.lower()
            ) and "fumble by" in event_text.lower()
            and "fumble forced by" in event_text.lower()
            and "recovered by" in event_text.lower()
            and "(" not in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for ([0-9\-]) yard[s]? to the ([A-Z0-9]+)\, fumble forced by ([a-zA-Z\.\,\'\s\-]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Za-z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)\.",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_df["rushing_yards"] = int(play_arr[0][1])
            temp_df["yards_gained"] = int(play_arr[0][1])

            temp_df["forced_fumble_player_1_team"] = defensive_team_arr[i]
            temp_df["forced_fumble_player_1_player_name"] = play_arr[0][3]

            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][4]

            temp_df["fumble_recovery_1_team"] = play_arr[0][5]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][6]
            temp_df["fumble_recovery_1_yards"] = 0

        elif (
            "rush" in event_text.lower() and
            (
                "gain" not in event_text.lower() and
                "loss" not in event_text.lower()
            ) and
            (
                "left" not in event_text.lower() and
                "right" not in event_text.lower() and
                "middle" not in event_text.lower()
            ) and "fumble by" in event_text.lower()
            and "fumble forced by" in event_text.lower()
            and "recovered by" in event_text.lower()
            and "(" not in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush to the ([A-Z0-9]+)\, fumble forced by ([a-zA-Z\.\,\'\s\-]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Za-z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)\.",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_yd_line = play_arr[0][4]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            # temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["rushing_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["yards_gained"] = temp_df["yardline_100"] - temp_yd_line

            temp_df["forced_fumble_player_1_team"] = defensive_team_arr[i]
            temp_df["forced_fumble_player_1_player_name"] = play_arr[0][2]

            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][3]

            temp_df["fumble_recovery_1_team"] = play_arr[0][4]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][5]
            temp_df["fumble_recovery_1_yards"] = 0
        elif (
            "rush" in event_text.lower() and
            (
                "gain" not in event_text.lower() and
                "loss" not in event_text.lower()
            ) and
            (
                "left" not in event_text.lower() and
                "right" not in event_text.lower() and
                "middle" not in event_text.lower()
            ) and "(" not in event_text.lower() and
            "fumble by" in event_text.lower() and
            "recovered by" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            temp_df["is_fumble"] = True
            temp_df["is_fumble_not_forced"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush to the ([A-Z0-9]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Za-z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)\.",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_yd_line = play_arr[0][1]
            if (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            elif (
                away_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "home"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, True)
            elif (
                home_team_abv in temp_yd_line and
                posteam_type_arr[i] == "away"
            ):
                temp_yd_line = _get_yardline(temp_yd_line, False)
            temp_df["air_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["rushing_yards"] = temp_df["yardline_100"] - temp_yd_line
            temp_df["yards_gained"] = temp_df["yardline_100"] - temp_yd_line

            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][2]

            temp_df["fumble_recovery_1_team"] = play_arr[0][3]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][5]
            temp_df["fumble_recovery_1_yards"] = 0
        elif (
            "rush" in event_text.lower() and
            (
                "gain" not in event_text.lower() and
                "loss" not in event_text.lower()
            ) and
            (
                "left" not in event_text.lower() and
                "right" not in event_text.lower() and
                "middle" not in event_text.lower()
            ) and "(" not in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush " +
                r"for ([\-0-9]+) yard[s]? " +
                r"to the ([A-Z0-9]+)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][1])
            temp_df["yards_gained"] = int(play_arr[0][1])

            # tacklers_arr = play_arr[0][3]
        elif (
            "rush" in event_text.lower() and
            (
                "gain" not in event_text.lower() and
                "loss" not in event_text.lower()
            ) and
            (
                "left" not in event_text.lower() and
                "right" not in event_text.lower() and
                "middle" not in event_text.lower()
            )
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush " +
                r"for ([\-0-9]+) yard[s]? " +
                r"to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            if len(play_arr) == 0:
                play_arr = re.findall(
                    r"([a-zA-Z\.\,\'\s\-]+) rush " +
                    r"for ([\-0-9]+) yard[s]? " +
                    r"to the ([A-Z0-9]+)",
                    event_text
                )
                temp_df["rusher_player_name"] = play_arr[0][0]
                # temp_df["run_location"] = play_arr[0][1]
                temp_df["rushing_yards"] = int(play_arr[0][1])
                temp_df["yards_gained"] = int(play_arr[0][1])
            else:
                temp_df["rusher_player_name"] = play_arr[0][0]
                # temp_df["run_location"] = play_arr[0][1]
                temp_df["rushing_yards"] = int(play_arr[0][1])
                temp_df["yards_gained"] = int(play_arr[0][1])

                tacklers_arr = play_arr[0][3]
        elif (
            "rush for" in event_text.lower() and
            (
                "gain" not in event_text.lower() and
                "loss" not in event_text.lower()
            )
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for ([\-0-9]+) yard[s]? to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][1])
            temp_df["yards_gained"] = int(play_arr[0][1])

            tacklers_arr = play_arr[0][3]
        elif (
            "rush" in event_text.lower() and
            (
                "gain" not in event_text.lower() and
                "loss" not in event_text.lower()
            )
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush " +
                r"([a-zA-Z0-9]+) for ([\-0-9]+) yard[s]? " +
                r"to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][2])
            temp_df["yards_gained"] = int(play_arr[0][2])

            tacklers_arr = play_arr[0][4]
        elif (
            "rush" in event_text.lower() and
            (
                "yards gain" in event_text.lower() or
                "yard gain" in event_text.lower()
            ) and "touchdown" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            temp_df["is_touchdown"] = True
            temp_df["is_rush_touchdown"] = True
            temp_df["td_team"] = possession_team_arr[i]

            if is_no_play is True:
                pass
            elif possession_team_arr[i] == away_team_id:
                away_score_post += 6
            elif possession_team_arr[i] == home_team_id:
                home_score_post += 6

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush ([a-zA-Z0-9]+) " +
                r"for ([\-0-9]+) yard[s]? gain " +
                r"to the ([A-Z0-9]+) TOUCHDOWN",
                event_text
            )
            if len(play_arr) == 0:
                play_arr = re.findall(
                    r"([a-zA-Z\.\,\'\s\-]+) rush for ([\-0-9]+) yard[s]? gain to the ([A-Z0-9]+) TOUCHDOWN",
                    event_text
                )
                temp_df["rusher_player_name"] = play_arr[0][0]
                temp_df["td_player_name"] = play_arr[0][0]
                # temp_df["run_location"] = play_arr[0][1]
                temp_df["rushing_yards"] = int(play_arr[0][1])
                temp_df["yards_gained"] = int(play_arr[0][1])
            else:
                temp_df["rusher_player_name"] = play_arr[0][0]
                temp_df["td_player_name"] = play_arr[0][0]
                temp_df["run_location"] = play_arr[0][1]
                temp_df["rushing_yards"] = int(play_arr[0][2])
                temp_df["yards_gained"] = int(play_arr[0][2])
        elif (
            "rush" in event_text.lower() and
            (
                "yards gain" in event_text.lower() or
                "yard gain" in event_text.lower()
            ) and
            "end of play" in event_text.lower() and
            "fumbled by" in event_text.lower() and
            "forced by" in event_text.lower() and
            "recovered by" in event_text.lower() and (
                " left" in event_text.lower() or
                " middle" in event_text.lower() or
                " right" in event_text.lower()
            )
        ):
            temp_df["is_rush_attempt"] = True
            temp_df["is_no_tackle"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush ([a-zA-Z0-9]+) for ([\-0-9]+) yard[s]? gain to the ([A-Z0-9]+) fumbled by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) forced by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Za-z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)\, [END|end]+ [OF|of]+ [PLAY|play]+",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][2])
            temp_df["yards_gained"] = int(play_arr[0][2])

            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][4]
            temp_df["forced_fumble_player_1_team"] = defensive_team_arr[i]
            temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]

            temp_df["fumble_recovery_1_team"] = play_arr[0][7]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][8]
            temp_df["fumble_recovery_1_yards"] = 0
        elif (
            "rush" in event_text.lower() and
            (
                "yards gain" in event_text.lower() or
                "yard gain" in event_text.lower()
            ) and
            "end of play" in event_text.lower() and
            "fumbled by" in event_text.lower() and
            "forced by" in event_text.lower() and
            "recovered by" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            temp_df["is_no_tackle"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for ([\-0-9]+) yard[s]? gain to the ([A-Z0-9]+) fumbled by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) forced by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Za-z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)\, [END|end]+ [OF|of]+ [PLAY|play]+",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][1])
            temp_df["yards_gained"] = int(play_arr[0][1])

            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][3]
            temp_df["forced_fumble_player_1_team"] = defensive_team_arr[i]
            temp_df["forced_fumble_player_1_player_name"] = play_arr[0][5]

            temp_df["fumble_recovery_1_team"] = play_arr[0][6]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][7]
            temp_df["fumble_recovery_1_yards"] = 0
        elif (
            "rush for loss of" in event_text.lower() and
            "fumble by" in event_text.lower() and
            "fumble forced by" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "(" not in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            temp_df["is_no_tackle"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for loss of ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, fumble forced by ([a-zA-Z\.\,\'\s\-]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Za-z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][1]) * -1
            temp_df["yards_gained"] = int(play_arr[0][1]) * -1

            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][4]
            temp_df["forced_fumble_player_1_team"] = defensive_team_arr[i]
            temp_df["forced_fumble_player_1_player_name"] = play_arr[0][3]

            temp_df["fumble_recovery_1_team"] = play_arr[0][5]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][6]
            temp_df["fumble_recovery_1_yards"] = 0
        elif (
            "rush" in event_text.lower() and
            (
                "yards gain" in event_text.lower() or
                "yard gain" in event_text.lower()
            ) and
            "end of play" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            temp_df["is_no_tackle"] = True

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush ([a-zA-Z0-9]+) " +
                r"for ([\-0-9]+) yard[s]? gain " +
                r"to the ([A-Z0-9]+), " +
                r"[End|end]+ [Of|of]+ [Play|play]+",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][2])
            temp_df["yards_gained"] = int(play_arr[0][2])
        elif (
            "rush" in event_text.lower() and
            (
                "yards gain" in event_text.lower() or
                "yard gain" in event_text.lower()
            ) and (
                "out-of-bounds" in event_text.lower() or
                "out of bounds" in event_text.lower()
            ) and ")," not in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush ([a-zA-Z0-9]+) for ([\-0-9]+) yard[s]? gain to the ([A-Z0-9]+)\, [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+ at ([A-Z0-9]+)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][2])
            temp_df["yards_gained"] = int(play_arr[0][2])

            # tacklers_arr = play_arr[0][4]
        elif (
            "rush" in event_text.lower() and
            (
                "yards gain" in event_text.lower() or
                "yard gain" in event_text.lower()
            )
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush ([a-zA-Z0-9]+) " +
                r"for ([\-0-9]+) yard[s]? gain " +
                r"to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            if len(play_arr) == 0:
                play_arr = re.findall(
                    r"([a-zA-Z\.\,\'\s\-]+) rush " +
                    r"for ([\-0-9]+) yard[s]? gain " +
                    r"to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                    event_text
                )
                temp_df["rusher_player_name"] = play_arr[0][0]
                # temp_df["run_location"] = play_arr[0][1]
                temp_df["rushing_yards"] = int(play_arr[0][1])
                temp_df["yards_gained"] = int(play_arr[0][1])

                tacklers_arr = play_arr[0][3]
            else:
                temp_df["rusher_player_name"] = play_arr[0][0]
                temp_df["run_location"] = play_arr[0][1]
                temp_df["rushing_yards"] = int(play_arr[0][2])
                temp_df["yards_gained"] = int(play_arr[0][2])

                tacklers_arr = play_arr[0][4]
        elif (
            "rush" in event_text.lower() and
            (
                "yard loss" in event_text.lower() or
                "yards loss" in event_text.lower()
            ) and "fumbled by" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "end of play" in event_text.lower() and (
                " left" in event_text.lower() or
                " middle" in event_text.lower() or
                " right" in event_text.lower()
            )
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush ([a-zA-Z0-9]+) for ([\-0-9]+) yard[s]? loss to the ([A-Z0-9]+) fumbled by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][2]) * -1
            temp_df["yards_gained"] = int(play_arr[0][2]) * -1

            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][4]

            temp_df["fumble_recovery_1_team"] = play_arr[0][6]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][7]

            tacklers_arr = play_arr[0][9]
        elif (
            "rush" in event_text.lower() and
            (
                "yard loss" in event_text.lower() or
                "yards loss" in event_text.lower()
            ) and "fumbled by" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "end of play" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for ([\-0-9]+) yard[s]? loss to the ([A-Z0-9]+) fumbled by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)\, [END|end]+ [OF|of]+ [PLAY|play]+",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][1]) * -1
            temp_df["yards_gained"] = int(play_arr[0][1]) * -1

            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][3]

            temp_df["fumble_recovery_1_team"] = play_arr[0][5]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][6]

            # tacklers_arr = play_arr[0][9]
        elif (
            "rush" in event_text.lower() and
            (
                "yard loss" in event_text.lower() or
                "yards loss" in event_text.lower()
            ) and "fumbled by" in event_text.lower() and
            "recovered by" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush ([a-zA-Z0-9]+) for ([\-0-9]+) yard[s]? loss to the ([A-Z0-9]+) fumbled by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][2]) * -1
            temp_df["yards_gained"] = int(play_arr[0][2]) * -1

            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][4]

            temp_df["fumble_recovery_1_team"] = play_arr[0][6]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][7]

            tacklers_arr = play_arr[0][9]
        elif (
            "rush" in event_text.lower() and
            (
                "yard loss" in event_text.lower() or
                "yards loss" in event_text.lower()
            ) and
            "end of play" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush ([a-zA-Z0-9]+) for ([\-0-9]+) yard[s]? loss to the ([A-Z0-9]+)\, [END|end]+ [OF|of]+ [PLAY|play]+\.",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][2]) * -1
            temp_df["yards_gained"] = int(play_arr[0][2]) * -1

            # tacklers_arr = play_arr[0][4]
        elif (
            "rush" in event_text.lower() and
            (
                "yard loss" in event_text.lower() or
                "yards loss" in event_text.lower()
            ) and (
                " left" in event_text.lower() or
                " middle" in event_text.lower() or
                " right" in event_text.lower()
            )
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush ([a-zA-Z0-9]+) " +
                r"for ([\-0-9]+) yard[s]? loss " +
                r"to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][2]) * -1
            temp_df["yards_gained"] = int(play_arr[0][2]) * -1

            tacklers_arr = play_arr[0][4]
        elif (
            "rush" in event_text.lower() and
            (
                "yard loss" in event_text.lower() or
                "yards loss" in event_text.lower()
            ) and (
                " left" not in event_text.lower() or
                " middle" not in event_text.lower() or
                " right" not in event_text.lower()
            )
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush " +
                r"for ([\-0-9]+) yard[s]? loss " +
                r"to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][1]) * -1
            temp_df["yards_gained"] = int(play_arr[0][1]) * -1

            tacklers_arr = play_arr[0][3]
        elif (
            "rush for loss of" in event_text.lower() and
            "fumble by" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "for no gain to the" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for loss of ([0-9\-]+) yard[s]? to the ([A-Z0-9]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)\, ([a-zA-Z\.\,\'\s\-]+) for no gain to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][1]) * -1
            temp_df["yards_gained"] = int(play_arr[0][1]) * -1

            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player"] = play_arr[0][3]

            temp_df["fumble_recovery_1_team"] = play_arr[0][4]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][5]
            temp_df["fumble_recovery_1_yards"] = 0
            tacklers_arr = play_arr[0][9]
        elif (
            "rush for loss of" in event_text.lower() and
            "(" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for loss of ([0-9\-]+) yard[s]? to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][1]) * -1
            temp_df["yards_gained"] = int(play_arr[0][1]) * -1

            tacklers_arr = play_arr[0][3]
        elif (
            "rush for loss of" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for loss of ([0-9\-]+) yard[s]? to the ([A-Z0-9]+)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][1]) * -1
            temp_df["yards_gained"] = int(play_arr[0][1]) * -1

            # tacklers_arr = play_arr[0][3]
        elif (
            "team rush for no gain to the" in event_text.lower() and
            "fumble by" in event_text.lower() and
            "recovered by" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"[TEAM|team]+ rush for no gain to the ([A-Z0-9]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)\.",
                event_text
            )
            temp_df["rusher_player_name"] = possession_team_arr[i]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = 0
            temp_df["yards_gained"] = 0
            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player_name"] = possession_team_arr[i]
            temp_df["fumble_recovery_1_team"] = play_arr[0][2]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][3]
        elif (
            "team rush for no gain to the" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"[TEAM|team]+ rush for no gain to the ([A-Z0-9]+)\.",
                event_text
            )
            temp_df["rusher_player_name"] = possession_team_arr[i]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = 0
            temp_df["yards_gained"] = 0

            tacklers_arr = play_arr[0][2]
        elif (
            "rush for no gain to the " in event_text.lower() and
            ")." in event_text.lower() and
            (
                "out-of-bounds" in event_text.lower() or
                "out of bounds" in event_text.lower()
            )
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for no gain to the ([A-Z0-9]+)\, [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+ \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = 0
            temp_df["yards_gained"] = 0

            tacklers_arr = play_arr[0][2]
        elif (
            "rush for no gain to the" in event_text.lower() and
            ")." in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for no gain to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = 0
            temp_df["yards_gained"] = 0

            tacklers_arr = play_arr[0][2]
        elif (
            "rush for no gain to the" in event_text.lower()
        ):
            temp_df["is_rush_attempt"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) rush for no gain to the ([A-Z0-9]+)",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = 0
            temp_df["yards_gained"] = 0
        elif ("kneel down" in event_text.lower()):
            temp_df["is_rush_attempt"] = True
            temp_df["is_kneel_down"] = True
            play_arr = re.findall(
                r"Kneel down by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) for loss of ([\-0-9]+) yard[s]?",
                event_text
            )
            temp_df["rusher_player_name"] = play_arr[0][0]
            # temp_df["run_location"] = play_arr[0][1]
            temp_df["rushing_yards"] = int(play_arr[0][2]) * -1
            temp_df["yards_gained"] = int(play_arr[0][2]) * -1
        # Kickoffs
        elif (
            "onside kickoff" in event_text.lower() and
            "end of play" in event_text.lower()
        ):
            temp_df["is_kickoff_attempt"] = True
            temp_df["is_onside_kickoff_attempt"] = True
            temp_df["return_team"] = possession_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) onside kickoff ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, [END|end]+ [OF|of]+ [PLAY|play]+\.",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
        elif (
            "on-side kick" in event_text.lower() and
            "recovered by" in event_text.lower()
        ):
            temp_df["is_kickoff_attempt"] = True
            temp_df["is_onside_kickoff_attempt"] = True
            temp_df["return_team"] = possession_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kickoff ([0-9]+) yard[s]? to the ([A-Z0-9]+)\,? on\-side kick\, recovered by ([a-zA-Z\.\,\'\s\-]+) on ([A-Z0-9]+)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["kickoff_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = 0
        elif (
            "kickoff" in event_text.lower() and
            "touchback" in event_text.lower()
        ):
            temp_df["is_kickoff_attempt"] = True
            temp_df["is_touchback"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kickoff ([0-9\-]+) yard[s]? " +
                r"to the ([A-Z0-9]+), [Touchback|touchback]+",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
        elif (
            "kickoff" in event_text.lower() and
            "fair catch" in event_text.lower()
        ):
            temp_df["is_kickoff_attempt"] = True
            temp_df["is_kickoff_fair_catch"] = True
            temp_df["return_team"] = possession_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kickoff ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? fair catch by ([a-zA-Z\.\,\'\s\-]+)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["kickoff_returner_player_name"] = play_arr[0][3]
        elif (
            "kickoff" in event_text.lower() and
            "out of bounds" in event_text.lower() and
            "return" not in event_text.lower()
        ):
            temp_df["is_kickoff_attempt"] = True
            temp_df["return_team"] = possession_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kickoff ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, out of bounds[ at]?+([A-Z0-9]+)?",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
        elif (
            "kickoff" in event_text.lower() and
            "return" in event_text.lower() and
            "out of bounds at" in event_text.lower()
        ):
            temp_df["is_kickoff_attempt"] = True
            temp_df["return_team"] = possession_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kickoff ([\-0-9]+) yard[s]? to the ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) return ([0-9\-]+) yard[s]? to the ([A-Z0-9]+)\, out of bounds at ([A-Z0-9]+)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["kickoff_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4])

            tacklers_arr = play_arr[0][6]
        elif (
            "kickoff" in event_text.lower() and
            "return" in event_text.lower() and
            (
                "out-of-bounds" in event_text.lower() or
                "out of bounds" in event_text.lower()
            )
        ):
            temp_df["is_kickoff_attempt"] = True
            temp_df["return_team"] = possession_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kickoff ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([0-9\-]+) yard[s]? to the ([A-Z0-9]+)\, [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+ \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            if len(play_arr) == 0:
                play_arr = re.findall(
                    r"([a-zA-Z\.\,\'\s\-]+) kickoff ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([0-9\-]+) yard[s]? to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)\, [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+",
                    event_text
                )
                temp_df["kicker_player_name"] = play_arr[0][0]
                temp_df["kick_distance"] = int(play_arr[0][1])
                temp_df["kickoff_returner_player_name"] = play_arr[0][3]
                temp_df["return_yards"] = int(play_arr[0][4])

                tacklers_arr = play_arr[0][6]
            else:
                temp_df["kicker_player_name"] = play_arr[0][0]
                temp_df["kick_distance"] = int(play_arr[0][1])
                temp_df["kickoff_returner_player_name"] = play_arr[0][3]
                temp_df["return_yards"] = int(play_arr[0][4])

                tacklers_arr = play_arr[0][6]
        elif (
            "kickoff" in event_text.lower() and
            "return" in event_text.lower() and
            "fumble by" in event_text.lower() and
            "fumble forced by" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "(" not in event_text.lower()
        ):
            temp_df["is_kickoff_attempt"] = True
            temp_df["is_fumble"] = True
            temp_df["is_fumble_forced"] = True
            temp_df["return_team"] = possession_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kickoff ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([0-9\-]+) yard[s]? to the ([A-Z0-9]+)\, fumble forced by ([a-zA-Z\.\,\'\s\-]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["kickoff_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4])
            temp_df["forced_fumble_player_1_team"] = defensive_team_arr[i]
            temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]
            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player"] = play_arr[0][7]

            temp_df["fumble_recovery_1_team"] = play_arr[0][8]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][9]
        elif (
            "kickoff" in event_text.lower() and
            "return" in event_text.lower() and
            "fumble by" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "(" not in event_text.lower()
        ):
            temp_df["is_kickoff_attempt"] = True
            temp_df["is_fumble"] = True
            temp_df["is_fumble_not_forced"] = True
            temp_df["return_team"] = possession_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kickoff ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([0-9\-]+) yard[s]? to the ([A-Z0-9]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["kickoff_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4])

            temp_df["fumbled_1_team"] = possession_team_arr[i]
            temp_df["fumbled_1_player"] = play_arr[0][6]

            temp_df["fumble_recovery_1_team"] = play_arr[0][7]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][8]
        elif (
            "kickoff" in event_text.lower() and
            "return" in event_text.lower() and
            "(" not in event_text.lower()
        ):
            temp_df["is_kickoff_attempt"] = True
            temp_df["return_team"] = possession_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kickoff ([\-0-9]+) yard[s]? to the " +
                r"([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) " +
                r"return ([0-9\-]+) yard[s]? to the ([A-Z0-9]+)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["kickoff_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4])

            # tacklers_arr = play_arr[0][6]
        elif (
            "kickoff" in event_text.lower() and
            "return" in event_text.lower()
        ):
            temp_df["is_kickoff_attempt"] = True
            temp_df["return_team"] = possession_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kickoff ([\-0-9]+) yard[s]? to the " +
                r"([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) " +
                r"return ([0-9\-]+) yard[s]? to the ([A-Z0-9]+) " +
                r"\(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["kickoff_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4])

            tacklers_arr = play_arr[0][6]
        # FGs/XPs
        elif (
            "kick attempt good" in event_text.lower() and
            "H: " in event_text and
            "LS: " in event_text
        ):
            temp_df["is_extra_point_attempt"] = True
            temp_df["extra_point_result"] = "good"

            if is_no_play is True:
                pass
            elif possession_team_arr[i] == away_team_id:
                away_score_post += 1
            elif possession_team_arr[i] == home_team_id:
                home_score_post += 1

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kick attempt good " +
                r"\(H: ([a-zA-Z\.\,\'\s\-]+), LS: ([a-zA-Z\.\,\'\s\-]+)\)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["holder_player_name"] = play_arr[0][1]
            temp_df["long_snapper_player_name"] = play_arr[0][2]
        elif ("kick attempt" in event_text.lower()):
            temp_df["is_extra_point_attempt"] = True

            if is_no_play is True:
                pass
            elif possession_team_arr[i] == away_team_id:
                away_score_post += 1
            elif possession_team_arr[i] == home_team_id:
                home_score_post += 1

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) kick attempt ([A-Za-z ]+)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["extra_point_result"] = play_arr[0][1]
            # temp_df["holder_player_name"] = play_arr[0][1]
            # temp_df["long_snapper_player_name"] = play_arr[0][2]
        elif (
            "field goal attempt" in event_text.lower() and
            "H:" in event_text and
            "LS:" in event_text
        ):
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) field goal attempt from " +
                r"([\-0-9]+) yard[s]? ([A-Za-z\s]+) " +
                r"\(H: ([a-zA-Z\.\,\'\s\-]+), LS: ([a-zA-Z\.\,\'\s\-]+)\)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["field_goal_result"] = play_arr[0][2]
            temp_df["holder_player_name"] = play_arr[0][3]
            temp_df["long_snapper_player_name"] = play_arr[0][4]

            if is_no_play is True:
                pass
            elif ("no good" in play_arr[0][2].lower()):
                pass
            elif (
                possession_team_arr[i] == away_team_id and
                "good" in play_arr[0][2].lower()
            ):
                away_score_post += 3
            elif (
                possession_team_arr[i] == home_team_id and
                "good" in play_arr[0][2].lower()
            ):
                home_score_post += 3
        elif (
            "field goal attempt" in event_text.lower() and
            "yard" in event_text.lower()
        ):
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) field goal attempt from " +
                r"([\-0-9]+) yard[s]? ([A-Za-z\s]+)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["field_goal_result"] = play_arr[0][2]
            # temp_df["holder_player_name"] = play_arr[0][3]
            # temp_df["long_snapper_player_name"] = play_arr[0][4]

            if is_no_play is True:
                pass
            elif ("no good" in play_arr[0][2].lower()):
                pass
            elif (
                possession_team_arr[i] == away_team_id and
                "good" in play_arr[0][2].lower()
            ):
                away_score_post += 3
            elif (
                possession_team_arr[i] == home_team_id and
                "good" in play_arr[0][2].lower()
            ):
                home_score_post += 3
        elif (
            "field goal attempt" in event_text.lower()
        ):
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) field goal attempt from " +
                r"([0-9]+) ([A-Za-z\s]+)",
                event_text
            )
            temp_df["kicker_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["field_goal_result"] = play_arr[0][2]
            # temp_df["holder_player_name"] = play_arr[0][3]
            # temp_df["long_snapper_player_name"] = play_arr[0][4]

            if is_no_play is True:
                pass
            elif ("no good" in play_arr[0][2].lower()):
                pass
            elif (
                possession_team_arr[i] == away_team_id and
                "good" in play_arr[0][2].lower()
            ):
                away_score_post += 3
            elif (
                possession_team_arr[i] == home_team_id and
                "good" in play_arr[0][2].lower()
            ):
                home_score_post += 3
        # Punts
        elif (
            "punt" in event_text.lower() and
            "return for loss of" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the " +
                r"([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) return " +
                r"for loss of ([\-0-9]+) yard[s]? to the ([A-Z0-9]+) " +
                r"\(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["punt_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4]) * -1
            # temp_yd_line = play_arr[0][5]
            temp_df["net_punt_yards"] = (
                temp_df["gross_punt_yards"] + int(play_arr[0][4])
            )

            tacklers_arr = play_arr[0][6]
        elif (
            "punt" in event_text.lower() and
            "fair catch" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            temp_df["is_punt_fair_catch"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? fair catch by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)",
                event_text
            )
            if len(play_arr) == 0:
                play_arr = re.findall(
                    r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? fair catch by ([a-zA-Z\.\,\'\s\-]+)",
                    event_text
                )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["net_punt_yards"] = int(play_arr[0][1])
            temp_df["punt_returner_player_name"] = play_arr[0][3]
        elif (
            "punt" in event_text.lower() and
            "out of bounds" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            # temp_df["is_punt_fair_catch"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, out of bounds at ([A-Z0-9]+)\.",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["net_punt_yards"] = int(play_arr[0][1])
        elif (
            "punt" in event_text.lower() and
            (
                "out-of-bounds" in event_text.lower() or
                "out of bounds" in event_text.lower()
            ) and "return" in event_text.lower() and
            "(" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            # temp_df["is_punt_out_of_bounds"] = True
            # temp_df["is_punt_fair_catch"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, ([a-zA-Z\.\,\'\s\-]+) return ([0-9\-]+) yard[s]? to the ([A-Z0-9]+)\, [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+ \(([a-zA-Z\.\,\;\'\s\-]+)\)\.",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["net_punt_yards"] = int(play_arr[0][1])
            temp_df["punt_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4]) * -1
            # temp_yd_line = play_arr[0][5]
            temp_df["net_punt_yards"] = (
                temp_df["gross_punt_yards"] - int(play_arr[0][4])
            )
            tacklers_arr = play_arr[0][6]
        elif (
            "punt" in event_text.lower() and
            (
                "out-of-bounds" in event_text.lower() or
                "out of bounds" in event_text.lower()
            ) and "return" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            # temp_df["is_punt_out_of_bounds"] = True
            # temp_df["is_punt_fair_catch"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, ([a-zA-Z\.\,\'\s\-]+) return ([0-9\-]+) yard[s]? to the ([A-Z0-9]+)\, [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+\.",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["net_punt_yards"] = int(play_arr[0][1])
            temp_df["punt_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4]) * -1
            # temp_yd_line = play_arr[0][5]
            temp_df["net_punt_yards"] = (
                temp_df["gross_punt_yards"] - int(play_arr[0][4])
            )
            # tacklers_arr = play_arr[0][6]
        elif (
            "punt" in event_text.lower() and
            (
                "out-of-bounds" in event_text.lower() or
                "out of bounds" in event_text.lower()
            )
        ):
            temp_df["is_punt"] = True
            temp_df["is_punt_out_of_bounds"] = True
            # temp_df["is_punt_fair_catch"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, [OUT|out]+\-? ?[OF|of]+\-? ?[BOUNDS|bounds]+\.",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["net_punt_yards"] = int(play_arr[0][1])
        elif (
            "punt" in event_text.lower() and
            "downed" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            temp_df["is_punt_downed"] = True
            # temp_df["is_punt_fair_catch"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, downed",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["net_punt_yards"] = int(play_arr[0][1])
        elif (
            "punt" in event_text.lower() and
            "muffed" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "end of play" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            temp_df["is_fumble"] = True
            temp_df["is_fumble_not_forced"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+) muffed by ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)\, [END|end]+ [OF|of]+ [PLAY|play]+\.",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["net_punt_yards"] = int(play_arr[0][1])
            temp_df["punt_returner_player_name"] = play_arr[0][3]
            temp_df["fumbled_1_team"] = defensive_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][3]

            temp_df["fumble_recovery_1_team"] = play_arr[0][5]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][6]
            temp_df["fumble_recovery_1_yards"] = 0
        elif (
            "punt loss of" in event_text.lower() and
            "blocked by" in event_text.lower() and
            "touchdown" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            temp_df["is_return_touchdown"] = True

            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt loss of ([\-0-9]+) yard[s]? to the ([A-Z0-9]+) \(blocked by ([a-zA-Z\.\,\'\s\-]+)\)\, ([a-zA-Z\.\,\'\s\-]+) for ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, TOUCHDOWN",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["net_punt_yards"] = int(play_arr[0][1])
            temp_df["blocked_player_name"] = play_arr[0][3]
            temp_df["fumble_recovery_1_team"] = defensive_team_arr[i]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][4]
            temp_df["fumble_recovery_1_yards"] = int(play_arr[0][5])
            # temp_df["return_yards"] = int(play_arr[0][5])
        elif (
            "punt" in event_text.lower() and
            "return" in event_text.lower() and
            "fumble by" in event_text.lower() and
            "forced by" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "(" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            temp_df["is_fumble"] = True
            temp_df["is_fumble_forced"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, fumble forced by ([a-zA-Z\.\,\'\s\-]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["punt_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4])

            temp_df["forced_fumble_player_1_team"] = possession_team_arr[i]
            temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]
            temp_df["fumbled_1_team"] = defensive_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][7]

            temp_df["fumble_recovery_1_team"] = play_arr[0][8]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][9]
            temp_df["fumble_recovery_1_yards"] = 0
            temp_yd_line = play_arr[0][11]

            temp_df["net_punt_yards"] = (
                temp_df["gross_punt_yards"] - temp_df["return_yards"]
            )

            # tacklers_arr = play_arr[0][6]
        elif (
            "punt" in event_text.lower() and
            "return" in event_text.lower() and
            "fumble by" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "(" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            temp_df["is_fumble"] = True
            temp_df["is_fumble_forced"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["punt_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4])

            # temp_df["forced_fumble_player_1_team"] = possession_team_arr[i]
            # temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]
            temp_df["fumbled_1_team"] = defensive_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][6]

            temp_df["fumble_recovery_1_team"] = play_arr[0][7]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][8]
            temp_df["fumble_recovery_1_yards"] = 0
            temp_yd_line = play_arr[0][9]

            temp_df["net_punt_yards"] = (
                temp_df["gross_punt_yards"] - temp_df["return_yards"]
            )

            # tacklers_arr = play_arr[0][6]

        elif (
            "punt" in event_text.lower() and
            "return" in event_text.lower() and
            "fumble by" in event_text.lower() and
            "recovered by" in event_text.lower() and
            "(" not in event_text.lower()
        ):
            temp_df["is_punt"] = True
            temp_df["is_fumble"] = True
            temp_df["is_fumble_not_forced"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, fumble by ([a-zA-Z\.\,\'\s\-]+) recovered by ([A-Z0-9]+) ([a-zA-Z\.\,\'\s\-]+) at ([A-Z0-9]+)",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["punt_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4])

            temp_df["fumbled_1_team"] = defensive_team_arr[i]
            temp_df["fumbled_1_player_name"] = play_arr[0][6]

            temp_df["fumble_recovery_1_team"] = play_arr[0][7]
            temp_df["fumble_recovery_1_player_name"] = play_arr[0][8]
            temp_df["fumble_recovery_1_yards"] = 0
            temp_yd_line = play_arr[0][9]

            temp_df["net_punt_yards"] = (
                temp_df["gross_punt_yards"] - temp_df["return_yards"]
            )

            # tacklers_arr = play_arr[0][6]
        elif (
            "punt" in event_text.lower() and
            "return" in event_text.lower() and
            "(" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([\-0-9]+) yard[s]? to the ([A-Z0-9]+) \(([a-zA-Z\.\,\;\'\s\-]+)\)",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["punt_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4])
            temp_yd_line = play_arr[0][5]

            temp_df["net_punt_yards"] = (
                temp_df["gross_punt_yards"] - temp_df["return_yards"]
            )

            tacklers_arr = play_arr[0][6]
        elif (
            "punt" in event_text.lower() and
            "return" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\,? ([a-zA-Z\.\,\'\s\-]+) return ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["punt_returner_player_name"] = play_arr[0][3]
            temp_df["return_yards"] = int(play_arr[0][4])
            temp_yd_line = play_arr[0][5]

            temp_df["net_punt_yards"] = (
                temp_df["gross_punt_yards"] - temp_df["return_yards"]
            )

            # tacklers_arr = play_arr[0][6]
        elif (
            "punt" in event_text.lower() and
            "touchback" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            temp_df["is_touchback"] = True
            # temp_df["is_punt_fair_catch"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\, [TOUCHBACK|touchback]+\.",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["net_punt_yards"] = int(play_arr[0][1]) - 20
        elif (
            "punt" in event_text.lower()
        ):
            temp_df["is_punt"] = True
            # temp_df["is_punt_fair_catch"] = True
            temp_df["return_team"] = defensive_team_arr[i]
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) punt ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)\.",
                event_text
            )
            temp_df["punter_player_name"] = play_arr[0][0]
            temp_df["kick_distance"] = int(play_arr[0][1])
            temp_df["gross_punt_yards"] = int(play_arr[0][1])
            temp_df["net_punt_yards"] = int(play_arr[0][1])
            # temp_df["punt_returner_player_name"] = play_arr[0][3]

        # Penalties, pt. I
        elif (
            "penalty" in event_text.lower() and
            "(" in event_text.lower()
        ):
            # This is handled later.
            pass
        elif (
            "penalty" in event_text.lower()
        ):
            # This is handled later.
            pass
        # Timeouts:
        elif "injury timeout" in event_text.lower():
            # We don't need to handle this.
            pass
        elif "timeout" in event_text.lower():
            temp_df["is_timeout"] = True
            play_arr = re.findall(
                r"Timeout ([\#0-9a-zA-Z\s]+)",
                event_text
            )
            # temp_timeout_team = play_arr[0]
            # if away_team_name in temp_timeout_team:
            #     temp_df["timeout_team"] = away_team_id
            # elif home_team_name in temp_timeout_team:
            #     temp_df["timeout_team"] = home_team_id
            # else:
            #     raise ValueError(
            #         "Could not determine what the timeout team is " +
            #         f"for this play: `{event_text}`."
            #     )
            # temp_df["timeout_team"] = temp_timeout_team
            temp_df["timeout_team"] = play_arr[0]
        # 2PC
        elif "pass attempt failed" in event_text.lower():
            temp_df["is_two_point_attempt"] = True

            if is_no_play is True:
                pass
            elif possession_team_arr[i] == away_team_id:
                away_score_post += 2
            elif possession_team_arr[i] == home_team_id:
                home_score_post += 2

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass attempt [FAILED|failed]+",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0]
            temp_df["two_point_conv_result"] = "failure"
        elif "pass attempt successful" in event_text.lower():
            temp_df["is_two_point_attempt"] = True

            if is_no_play is True:
                pass
            elif possession_team_arr[i] == away_team_id:
                away_score_post += 2
            elif possession_team_arr[i] == home_team_id:
                home_score_post += 2

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass attempt [SUCCESSFUL|successful]+",
                event_text
            )
            temp_df["receiver_player_name"] = play_arr[0]
            temp_df["two_point_conv_result"] = "success"
        elif "pass attempt to" in event_text.lower() and "good" in event_text.lower():
            temp_df["is_two_point_attempt"] = True

            if is_no_play is True:
                pass
            elif possession_team_arr[i] == away_team_id:
                away_score_post += 2
            elif possession_team_arr[i] == home_team_id:
                home_score_post += 2

            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) pass attempt to ([a-zA-Z\.\,\'\s\-]+) [GOOD|good]+",
                event_text
            )
            temp_df["passer_player_name"] = play_arr[0][0]
            temp_df["receiver_player_name"] = play_arr[0][1]
            temp_df["two_point_conv_result"] = "success"
        # housekeeping
        elif "ball on" in event_text.lower():
            pass
        elif (
            event_text.lower() == "1st and 1." or
            event_text.lower() == "1st and 2." or
            event_text.lower() == "1st and 3." or
            event_text.lower() == "1st and 4." or
            event_text.lower() == "1st and 5." or
            event_text.lower() == "1st and 6." or
            event_text.lower() == "1st and 7." or
            event_text.lower() == "1st and 8." or
            event_text.lower() == "1st and 9." or
            event_text.lower() == "1st and 10." or
            event_text.lower() == "1st and 11." or
            event_text.lower() == "1st and 12." or
            event_text.lower() == "1st and 13." or
            event_text.lower() == "1st and 14." or
            event_text.lower() == "1st and 15." or
            event_text.lower() == "1st and 16." or
            event_text.lower() == "1st and 17." or
            event_text.lower() == "1st and 18." or
            event_text.lower() == "1st and 19." or
            event_text.lower() == "1st and 20."
        ):
            # There are plays found in game ID 5367381
            # where this is the entire play description.
            # Obviously, there's nothing to parse, so let's
            # skip it.
            logging.info(
                f"Unusual play `{event_text}`."
            )
        elif (
            event_text.lower() == "2nd and 1." or
            event_text.lower() == "2nd and 2." or
            event_text.lower() == "2nd and 3." or
            event_text.lower() == "2nd and 4." or
            event_text.lower() == "2nd and 5." or
            event_text.lower() == "2nd and 6." or
            event_text.lower() == "2nd and 7." or
            event_text.lower() == "2nd and 8." or
            event_text.lower() == "2nd and 9." or
            event_text.lower() == "2nd and 10." or
            event_text.lower() == "2nd and 11." or
            event_text.lower() == "2nd and 12." or
            event_text.lower() == "2nd and 13." or
            event_text.lower() == "2nd and 14." or
            event_text.lower() == "2nd and 15." or
            event_text.lower() == "2nd and 16." or
            event_text.lower() == "2nd and 17." or
            event_text.lower() == "2nd and 18." or
            event_text.lower() == "2nd and 19." or
            event_text.lower() == "2nd and 20."
        ):
            # There are plays like those found in game ID 1994440
            # where this is the entire play description.
            # Obviously, there's nothing to parse, so let's
            # skip it.
            logging.info(
                f"Unusual play `{event_text}`."
            )
        elif (
            event_text.lower() == "3rd and 1." or
            event_text.lower() == "3rd and 2." or
            event_text.lower() == "3rd and 3." or
            event_text.lower() == "3rd and 4." or
            event_text.lower() == "3rd and 5." or
            event_text.lower() == "3rd and 6." or
            event_text.lower() == "3rd and 7." or
            event_text.lower() == "3rd and 8." or
            event_text.lower() == "3rd and 9." or
            event_text.lower() == "3rd and 10." or
            event_text.lower() == "3rd and 11." or
            event_text.lower() == "3rd and 12." or
            event_text.lower() == "3rd and 13." or
            event_text.lower() == "3rd and 14." or
            event_text.lower() == "3rd and 15." or
            event_text.lower() == "3rd and 16." or
            event_text.lower() == "3rd and 17." or
            event_text.lower() == "3rd and 18." or
            event_text.lower() == "3rd and 19." or
            event_text.lower() == "3rd and 20."
        ):
            # There are plays like those found in game ID 1994440
            # where this is the entire play description.
            # Obviously, there's nothing to parse, so let's
            # skip it.
            logging.info(
                f"Unusual play `{event_text}`."
            )
        elif (
            event_text.lower() == "4th and 1." or
            event_text.lower() == "4th and 2." or
            event_text.lower() == "4th and 3." or
            event_text.lower() == "4th and 4." or
            event_text.lower() == "4th and 5." or
            event_text.lower() == "4th and 6." or
            event_text.lower() == "4th and 7." or
            event_text.lower() == "4th and 8." or
            event_text.lower() == "4th and 9." or
            event_text.lower() == "4th and 10." or
            event_text.lower() == "4th and 11." or
            event_text.lower() == "4th and 12." or
            event_text.lower() == "4th and 13." or
            event_text.lower() == "4th and 14." or
            event_text.lower() == "4th and 15." or
            event_text.lower() == "4th and 16." or
            event_text.lower() == "4th and 17." or
            event_text.lower() == "4th and 18." or
            event_text.lower() == "4th and 19." or
            event_text.lower() == "4th and 20."
        ):
            # There are plays like those found in game ID 1994440
            # where this is the entire play description.
            # Obviously, there's nothing to parse, so let's
            # skip it.
            logging.info(
                f"Unusual play `{event_text}`."
            )
        elif "clock" in event_text.lower() and len(event_text) == 12:
            # This is to handle a play found in game ID 5367381
            # where the entire play is:
            # `Clock 00:38.`
            # This will also handle plays like this so long as its
            # formatted as `Clock MM:SS.`
            logging.info(
                f"Unusual play `{event_text}`."
            )
        elif "won the toss and" in event_text.lower():
            temp_df["is_coin_toss"] = True
            play_arr = re.findall(
                r"([a-zA-Z\.\,\'\s\-]+) won the toss and ([a-zA-Z ]+)\.",
                event_text
            )
            temp_df["coin_toss_winner"] = play_arr[0][0]
            temp_df["coin_toss_decision"] = play_arr[0][1]
        elif "at qb for" in event_text.lower():
            # If we end up at this point, we don't need to parse the play,
            # we just need to verify that there isn't more to this play than
            # "hey some guy subbed in at QB".
            play_arr = re.findall(
                r"^([a-zA-Z\.\,\'\s\-]+) at QB for ([ A-Za-z0-9\.]+)\.?$",
                event_text
            )

            test_str = play_arr[0][0]
        else:
            raise SyntaxError(
                f"Unhandled play: `{event_text}`"
            )

        if len(tacklers_arr) > 0 and ";" in tacklers_arr:
            temp_df[[
                "assist_tackle_1_player_name",
                "assist_tackle_2_player_name"
            ]] = tacklers_arr.split(";")
        elif len(tacklers_arr) > 0:
            temp_df["solo_tackle_1_player_name"] = tacklers_arr

        if len(sack_players_arr) > 0 and ";" in sack_players_arr:
            temp_df[[
                "half_sack_1_player_name",
                "half_sack_2_player_name"
            ]] = sack_players_arr.split(";")
        elif len(sack_players_arr) > 0:
            temp_df["sack_player_name"] = sack_players_arr

        # Penalties, pt. II
        if (
            "penalty" in event_text.lower() and
            "declined" in event_text.lower()
        ):
            temp_df["is_penalty"] = True
            play_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9]+) ([0-9a-zA-Z\s]+) declined",
                event_text
            )
            temp_df["penalty_team"] = play_arr[0][0]
            temp_df["penalty_type"] = play_arr[0][1]
        elif (
            "penalty" in event_text.lower() and
            "(" in event_text.lower()
        ):
            temp_df["is_penalty"] = True
            play_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9]+) ([a-zA-Z\s\:]+) " +
                r"\(([a-zA-Z\.\,\'\s\-]+)\) ([\-0-9]+) yard[s]? " +
                r"from ([A-Z0-9]+) to ([A-Z0-9]+)",
                event_text
            )
            if len(play_arr) == 0 and "(" not in event_text.lower():
                play_arr = re.findall(
                    r"PENALTY ([a-zA-Z0-9]+) ([a-zA-Z\s\:]+) ([\-0-9]+) yard[s]?",
                    event_text
                )
                temp_df["penalty_team"] = play_arr[0][0]
                temp_df["penalty_type"] = play_arr[0][1]
                # temp_df["penalty_player_name"] = play_arr[0][2]
                temp_df["penalty_yards"] = int(play_arr[0][2])
            elif len(play_arr) == 0 and "(" in event_text.lower() and "yard" not in event_text.lower():
                play_arr = re.findall(
                    r"PENALTY ([a-zA-Z0-9]+) ([a-zA-Z\s\:]+) \(([a-zA-Z\.\,\'\s\-]+)\)",
                    event_text
                )
                temp_df["penalty_team"] = play_arr[0][0]
                temp_df["penalty_type"] = play_arr[0][1]
                temp_df["penalty_player_name"] = play_arr[0][2]
            elif len(play_arr) == 0 and "(" in event_text.lower():
                play_arr = re.findall(
                    r"PENALTY ([a-zA-Z0-9]+) ([a-zA-Z\s\:]+) \(([a-zA-Z\.\,\'\s\-]+)\) ([\-0-9]+) yard[s]?",
                    event_text
                )
                if len(play_arr) == 0:
                    play_arr = re.findall(
                        r"PENALTY ([a-zA-Z0-9]+) ([a-zA-Z\s\:]+) ([\-0-9]+) yard[s]?",
                        event_text
                    )
                    temp_df["penalty_team"] = play_arr[0][0]
                    temp_df["penalty_type"] = play_arr[0][1]
                    # temp_df["penalty_player_name"] = play_arr[0][2]
                    temp_df["penalty_yards"] = int(play_arr[0][2])

                else:
                    temp_df["penalty_team"] = play_arr[0][0]
                    temp_df["penalty_type"] = play_arr[0][1]
                    temp_df["penalty_player_name"] = play_arr[0][2]
                    temp_df["penalty_yards"] = int(play_arr[0][3])
            else:
                temp_df["penalty_team"] = play_arr[0][0]
                temp_df["penalty_type"] = play_arr[0][1]
                temp_df["penalty_player_name"] = play_arr[0][2]
                temp_df["penalty_yards"] = int(play_arr[0][3])
        elif (
            "penalty" in event_text.lower() and
            "off-setting" in event_text.lower()
        ):
            temp_df["is_penalty"] = True
        elif (
            "penalty" in event_text.lower()
        ):
            temp_df["is_penalty"] = True
            play_arr = re.findall(
                r"PENALTY ([a-zA-Z0-9]+) ([a-zA-Z\s\:]+) ([\-0-9]+) yard[s]? to the ([A-Z0-9]+)",
                event_text
            )
            temp_df["penalty_team"] = play_arr[0][0]
            temp_df["penalty_type"] = play_arr[0][1]
            temp_df["penalty_yards"] = int(play_arr[0][2])

        temp_df["away_score_post"] = away_score_post
        temp_df["home_score_post"] = home_score_post
        pbp_df_arr.append(temp_df)

    pbp_df = pd.concat(pbp_df_arr, ignore_index=True)

    return pbp_df
