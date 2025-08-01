import polars as pl

# J1 League events schema - based on StatsBomb structure
# This is similar to the open_data events schema but may have some differences
J1_LEAGUE_EVENTS_SCHEMA = {
    "id": pl.String,
    "index": pl.Int64,
    "period": pl.Int64,
    "timestamp": pl.String,
    "minute": pl.Int64,
    "second": pl.Int64,
    "possession": pl.Int64,
    "duration": pl.String,
    "related_events": pl.List(pl.String),
    "location": pl.List(pl.Float64),
    "type_id": pl.Int64,
    "type_name": pl.String,
    "possession_team_id": pl.Int64,
    "possession_team_name": pl.String,
    "play_pattern_id": pl.Int64,
    "play_pattern_name": pl.String,
    "team_id": pl.Int64,
    "team_name": pl.String,
    "player_id": pl.Int64,
    "player_name": pl.String,
    "position_id": pl.Int64,
    "position_name": pl.String,
    
    # Tactics information
    "tactics_formation": pl.Int64,
    "tactics_lineup_jersey_number": pl.Int64,
    "tactics_lineup_player_id": pl.Int64,
    "tactics_lineup_player_name": pl.String,
    "tactics_lineup_position_id": pl.Int64,
    "tactics_lineup_position_name": pl.String,
    
    # Pass information
    "pass_recipient_id": pl.Int64,
    "pass_recipient_name": pl.String,
    "pass_length": pl.String,
    "pass_angle": pl.String,
    "pass_height_id": pl.Int64,
    "pass_height_name": pl.String,
    "pass_end_location": pl.List(pl.String),
    "pass_body_part_id": pl.Int64,
    "pass_body_part_name": pl.String,
    "pass_type_id": pl.Int64,
    "pass_type_name": pl.String,
    "pass_outcome_id": pl.Int64,
    "pass_outcome_name": pl.String,
    "pass_technique_id": pl.Int64,
    "pass_technique_name": pl.String,
    "pass_assisted_shot_id": pl.String,
    "pass_shot_assist": pl.Boolean,
    "pass_goal_assist": pl.Boolean,
    "pass_through_ball": pl.Boolean,
    "pass_cross": pl.Boolean,
    "pass_switch": pl.Boolean,
    "pass_aerial_won": pl.Boolean,
    "pass_backheel": pl.Boolean,
    "pass_deflected": pl.Boolean,
    "pass_inswinging": pl.Boolean,
    "pass_outswinging": pl.Boolean,
    "pass_cut_back": pl.Boolean,
    "pass_no_touch": pl.Boolean,
    "pass_straight": pl.Boolean,
    "pass_miscommunication": pl.Boolean,
    
    # Shot information
    "shot_statsbomb_xg": pl.String,
    "shot_end_location": pl.List(pl.String),
    "shot_key_pass_id": pl.String,
    "shot_body_part_id": pl.Int64,
    "shot_body_part_name": pl.String,
    "shot_technique_id": pl.Int64,
    "shot_technique_name": pl.String,
    "shot_type_id": pl.Int64,
    "shot_type_name": pl.String,
    "shot_outcome_id": pl.Int64,
    "shot_outcome_name": pl.String,
    "shot_first_time": pl.Boolean,
    "shot_one_on_one": pl.Boolean,
    "shot_deflected": pl.Boolean,
    "shot_aerial_won": pl.Boolean,
    "shot_open_goal": pl.Boolean,
    "shot_redirect": pl.Boolean,
    "shot_freeze_frame_location": pl.List(pl.String),
    "shot_freeze_frame_teammate": pl.Boolean,
    "shot_freeze_frame_player_id": pl.Int64,
    "shot_freeze_frame_player_name": pl.String,
    "shot_freeze_frame_position_id": pl.Int64,
    "shot_freeze_frame_position_name": pl.String,
    
    # Goalkeeper information
    "goalkeeper_end_location": pl.List(pl.String),
    "goalkeeper_position_id": pl.Int64,
    "goalkeeper_position_name": pl.String,
    "goalkeeper_type_id": pl.Int64,
    "goalkeeper_type_name": pl.String,
    "goalkeeper_outcome_id": pl.Int64,
    "goalkeeper_outcome_name": pl.String,
    "goalkeeper_technique_id": pl.Int64,
    "goalkeeper_technique_name": pl.String,
    "goalkeeper_body_part_id": pl.Int64,
    "goalkeeper_body_part_name": pl.String,
    "goalkeeper_punched_out": pl.Boolean,
    
    # Dribble information
    "dribble_outcome_id": pl.Int64,
    "dribble_outcome_name": pl.String,
    "dribble_nutmeg": pl.Boolean,
    "dribble_overrun": pl.Boolean,
    "dribble_no_touch": pl.Boolean,
    
    # Duel information
    "duel_type_id": pl.Int64,
    "duel_type_name": pl.String,
    "duel_outcome_id": pl.Int64,
    "duel_outcome_name": pl.String,
    
    # Foul information
    "foul_committed_advantage": pl.Boolean,
    "foul_won_advantage": pl.Boolean,
    "foul_committed_offensive": pl.Boolean,
    "foul_won_defensive": pl.Boolean,
    "foul_committed_type_id": pl.Int64,
    "foul_committed_type_name": pl.String,
    "foul_committed_card_id": pl.Int64,
    "foul_committed_card_name": pl.String,
    
    # Other event information
    "under_pressure": pl.Boolean,
    "counterpress": pl.Boolean,
    "carry_end_location": pl.List(pl.String),
    "ball_receipt_outcome_id": pl.Int64,
    "ball_receipt_outcome_name": pl.String,
    "ball_recovery_recovery_failure": pl.Boolean,
    "ball_recovery_offensive": pl.Boolean,
    "interception_outcome_id": pl.Int64,
    "interception_outcome_name": pl.String,
    "clearance_left_foot": pl.Boolean,
    "clearance_right_foot": pl.Boolean,
    "clearance_head": pl.Boolean,
    "clearance_other": pl.Boolean,
    "clearance_body_part_id": pl.Int64,
    "clearance_body_part_name": pl.String,
    "clearance_aerial_won": pl.Boolean,
    "block_deflection": pl.Boolean,
    "block_offensive": pl.Boolean,
    "block_save_block": pl.Boolean,
    "substitution_outcome_id": pl.Int64,
    "substitution_outcome_name": pl.String,
    "substitution_replacement_id": pl.Int64,
    "substitution_replacement_name": pl.String,
    "injury_stoppage_in_chain": pl.Boolean,
    "bad_behaviour_card_id": pl.Int64,
    "bad_behaviour_card_name": pl.String,
    "miscontrol_aerial_won": pl.Boolean,
    "50_50_outcome_id": pl.Int64,
    "50_50_outcome_name": pl.String,
    "off_camera": pl.Boolean,
    "out": pl.Boolean,
    
    # Coordinate information
    "x": pl.String,
    "y": pl.String,
    "end_x": pl.String,
    "end_y": pl.String,
    
    # Possession stats
    "possession_event_count": pl.String,
    "possession_pass_count": pl.String,
    "possession_player_count": pl.String,
    "possession_duration": pl.String,
    "total_xG": pl.String,
} 