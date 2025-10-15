import numpy as np
import pandas as pd
from typing import Optional, Tuple

from DB_connection import DB_connection
from metrics import compute_lean_score, compute_proportions, compute_shanon_entropy, compute_rank_aware_lean_score, compute_rank_aware_proportions

db_connection = DB_connection()



def get_sessions_by_profile(profile_name):
    query = f"""
              SELECT s.session_id
              FROM sessions s
              JOIN profiles p ON s.profile_id = p.profile_id
              WHERE p.profile_name = '{profile_name}'
              AND s.status = 'completed'
            """
    results = db_connection.select(query)
    return results["session_id"].tolist()

### GET GLOBAL DATA WITHOUT DYNAMICS ###
def get_videos_watched_in_session(session_id):
    query = f"""
              SELECT *
              FROM annoted_videos v
              JOIN recommendation_log rl ON v.video_id = rl.video_id
              WHERE rl.session_id = '{session_id}'
            """
    results = db_connection.select(query)
    return results

def get_context_videos(session_id):
    query = f"""
            SELECT DISTINCT rl.source_video_id AS video_id, rl.depth ,v.llm_label
            FROM recommendation_log rl
            INNER JOIN annoted_videos v ON v.video_youtube_id = rl.source_video_id
            WHERE session_id ='{session_id}'
            AND was_during_context = 1
            ORDER BY rl.depth ASC
          """
    results = db_connection.select(query)
    return results

def get_recommended_videos_during_context_phase(session_id):
    query = f"""
            SELECT rl.recommended_video_id AS video_id, rl.recommendation_rank, v.llm_label
            FROM recommendation_log rl
            INNER JOIN annoted_videos v ON v.video_youtube_id = rl.recommended_video_id
            WHERE session_id ='{session_id}'
            AND was_during_context = 1
          """
    results = db_connection.select(query)
    return results

def get_chosen_videos_during_navigation_phase(session_id):
    query = f"""
            SELECT rl.recommended_video_id AS video_id, v.llm_label
            FROM recommendation_log rl
            INNER JOIN annoted_videos v ON v.video_youtube_id = rl.recommended_video_id
            WHERE session_id ='{session_id}'
            AND was_during_context = 0
            AND was_selected = 1
          """
    results = db_connection.select(query)
    return results

def get_recommended_videos_during_navigation_phase(session_id):
    query = f"""
            SELECT rl.recommended_video_id AS video_id, rl.recommendation_rank, v.llm_label
            FROM recommendation_log rl
            INNER JOIN annoted_videos v ON v.video_youtube_id = rl.recommended_video_id
            WHERE session_id ='{session_id}'
            AND was_during_context = 0
          """
    results = db_connection.select(query)
    return results

### GET EVOLUTIONARY DATA ###

## SELECTED ITEMS ##
def selected_items_lean_and_div_evolution_per_session(session_id: str, window_size: Optional[int] = None,) -> list:

    context_videos = get_context_videos(session_id)
    context_videos_shuffled = context_videos.sample(frac=1)
    nbr_context_videos = len(context_videos)

    query = f"""
            SELECT rl.recommended_video_id AS video_id, rl.depth , v.llm_label
            FROM recommendation_log rl
            INNER JOIN annoted_videos v ON v.video_youtube_id = rl.recommended_video_id
            WHERE session_id ='{session_id}'
            AND was_during_context = 0
            AND was_selected = 1
            ORDER BY rl.depth ASC
          """
    chosen_videos = db_connection.select(query)

    all_videos = pd.concat([context_videos_shuffled,chosen_videos], ignore_index=True)

    lean_scores = []
    diversity_scores = []
    for i in range(nbr_context_videos+1, len(all_videos)+1):
        if window_size:
            start_index = max(0, i - window_size)
            lean_scores.append(compute_lean_score(all_videos.iloc[start_index:i]))
            diversity_scores.append(compute_shanon_entropy(compute_proportions(all_videos.iloc[start_index:i])))
        else:
            lean_scores.append(compute_lean_score(all_videos.iloc[:i]))
            diversity_scores.append(compute_shanon_entropy(compute_proportions(all_videos.iloc[:i])))

    return lean_scores,diversity_scores



## RECOMMENDED ITEMS ##
def _lean_score(df: pd.DataFrame, rank_aware: bool) -> float:
    if rank_aware:
        # ensure a recommendation_rank exists; if missing, make a simple 1..n
        if "recommendation_rank" not in df.columns:
            df = df.copy()
            df["recommendation_rank"] = np.arange(1, len(df) + 1)
        return compute_rank_aware_lean_score(df)
    else:
        return compute_lean_score(df)

def _proportions(df: pd.DataFrame, rank_aware: bool) -> dict:
    if rank_aware:
        if "recommendation_rank" not in df.columns:
            df = df.copy()
            df["recommendation_rank"] = np.arange(1, len(df) + 1)
        return compute_rank_aware_proportions(df)
    else:
        return compute_proportions(df)

# --- main functions ---

def recommended_items_lean_and_div_evolution_per_session(
    session_id: str,
    window_size: Optional[int] = None,
    rank_aware: bool = False
) -> Tuple[list, list]:
    # Escape single quotes for safety when using f-strings in SQL
    session_id_esc = session_id.replace("'", "''")

    # 1) Context once + reproducible shuffle
    context_videos = get_recommended_videos_during_context_phase(session_id)
    context_videos_shuffled = context_videos.sample(frac=1, ignore_index=True)

    # 2) Bounds once
    nbr_context_videos = db_connection.select_single_value(
        f"""
        SELECT MAX(depth) AS max_depth
        FROM recommendation_log
        WHERE session_id = '{session_id_esc}'
          AND was_during_context = 1
        """
    )
    max_depth = db_connection.select_single_value(
        f"""
        SELECT MAX(depth) AS max_depth
        FROM recommendation_log
        WHERE session_id = '{session_id_esc}'
        """
    )

    if nbr_context_videos is None or max_depth is None:
        return [], []

    # 3) All non-context recs once, ordered by depth
    recs_df = db_connection.select(
        f"""
        SELECT
            rl.recommended_video_id AS video_id,
            rl.depth,
            rl.recommendation_rank,
            v.llm_label
        FROM recommendation_log rl
        INNER JOIN annoted_videos v
          ON v.video_youtube_id = rl.recommended_video_id
        WHERE rl.session_id = '{session_id_esc}'
          AND rl.was_during_context = 0
        ORDER BY rl.depth ASC
        """
    )

    # 4) If no recs, just score context for each step (same score repeated)
    steps = int(max_depth) - int(nbr_context_videos) + 1
    if len(recs_df) == 0:
        base_view = context_videos_shuffled
        if window_size:
            start_idx = max(0, len(base_view) - window_size)
            view = base_view.iloc[start_idx:]
        else:
            view = base_view

        lean = _lean_score(view, rank_aware)
        div = compute_shanon_entropy(_proportions(view, rank_aware))
        return [lean] * steps, [div] * steps

    # 5) Precompute cum-count per depth (how many rec rows have depth <= d)
    counts = (
        recs_df["depth"]
        .value_counts()
        .rename_axis("depth")
        .reset_index(name="cnt")
        .sort_values("depth")
    )
    counts["cum_cnt"] = counts["cnt"].cumsum()

    depth_min = int(counts["depth"].min())
    depth_max = int(counts["depth"].max())

    full = (
        pd.DataFrame({"depth": range(depth_min, depth_max + 1)})
        .merge(counts[["depth", "cum_cnt"]], on="depth", how="left")
        .sort_values("depth")
    )
    full["cum_cnt"] = full["cum_cnt"].ffill().fillna(0).astype(int)

    def cum_rows_for_depth(d: int) -> int:
        d = int(d)
        if d < depth_min:
            return 0
        if d > depth_max:
            return int(full["cum_cnt"].iloc[-1])
        return int(full.loc[full["depth"] == d, "cum_cnt"].iloc[0])

    # 6) Build base and emulate "re-append ≤ depth block each step"
    base = pd.concat([context_videos_shuffled, recs_df], ignore_index=True)
    C = len(context_videos_shuffled)
    rec_prefix_indices = list(range(C, C + len(recs_df)))

    lean_scores: list[float] = []
    div_scores: list[float] = []
    current_indices: list[int] = list(range(C))  # start with context only

    for d in range(int(nbr_context_videos), int(max_depth) + 1):
        r = cum_rows_for_depth(d)
        if r > 0:
            # duplicate-append to match original behavior
            current_indices.extend(rec_prefix_indices[:r])

        view = base.iloc[current_indices]
        if window_size:
            start_idx = max(0, len(view) - window_size)
            view_slice = view.iloc[start_idx:]
        else:
            view_slice = view

        lean_scores.append(_lean_score(view_slice, rank_aware))
        div_scores.append(
            compute_shanon_entropy(_proportions(view_slice, rank_aware))
        )

    return lean_scores, div_scores


