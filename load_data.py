from config import choose_expe

import json
import pandas as pd

from DB_connection import DB_connection


db_connection = DB_connection()

def load_if_not_exists(df, table_name):
    query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    result = db_connection.select(query)
    db_connection.save_df(df, table_name)

    if result.empty:
        print(f"Table '{table_name}' created and data loaded.")
    else:
        print(f"Table '{table_name}' already exists. Skipping load.")



def process_session_table(session_df: pd.DataFrame) -> pd.DataFrame:
    length = len(session_df)

    session_names = [None]*length
    profile_ids   = [None]*length
    persona_mix   = [None]*length
    context_names = [None]*length
    persona_sequences = [None]*length

    for i in range(length):
        try:
            raw = session_df.loc[i, "experiment_config"]
            # handle both str JSON and already-parsed dicts
            expe = json.loads(raw)["experiment"] if isinstance(raw, str) else raw["experiment"]

            mode = expe.get("mode")
            if mode == "single_persona":
                session_names[i] = f"single_persona_{expe['profile_id']}_{expe['context_name']}"
                profile_ids[i]   = expe['profile_id']
                context_names[i] = expe['context_name']
                # persona_mix/persona_sequence stay None

            elif mode == "mixed_persona":
                weights_string = "".join(
                    f"profile_{mix['profile_id']}_weight_{int(mix['weight']*100)}_"
                    for mix in expe['persona_mix']
                )
                session_names[i] = f"mixed_persona_{weights_string}{expe['context_name']}"
                # store JSON-serialised string so SQLite can handle it
                persona_mix[i]   = json.dumps(expe['persona_mix'], ensure_ascii=False)
                context_names[i] = expe['context_name']

            elif mode == "random_choice":
                session_names[i] = f"random_choice_{expe['context_name']}"
                profile_ids[i]   = expe.get('profile_id')
                context_names[i] = expe['context_name']

        except Exception:
            # keep defaults (None) if parsing fails
            pass
    out = session_df.copy()
    out["session_name"]          = session_names
    out["profile_id_extracted"]  = profile_ids
    out["persona_mix"]           = persona_mix
    out["context_name"]          = context_names
    out["persona_sequence"]      = persona_sequences

    return out

    
            


if __name__ == "__main__":
    if choose_expe == "expe_1":

        persona_filter_logs    = pd.read_csv("./data/expe_1/csv_files/persona_filter_logs.csv")
        profile_video_contexts = pd.read_csv("./data/expe_1/csv_files/profile_video_contexts.csv")
        profiles               = pd.read_csv("./data/expe_1/csv_files/profiles.csv")
        recommendation_log     = pd.read_csv("./data/expe_1/csv_files/recommendation_log.csv")
        sessions               = pd.read_csv("./data/expe_1/csv_files/sessions.csv")
        videos                 = pd.read_csv("./data/expe_1/csv_files/videos.csv")

        load_if_not_exists(persona_filter_logs, "persona_filter_logs")
        load_if_not_exists(profile_video_contexts, "profile_video_contexts")
        load_if_not_exists(profiles, "profiles")
        load_if_not_exists(recommendation_log, "recommendation_log")
        load_if_not_exists(sessions, "sessions")
        load_if_not_exists(videos, "videos")


    elif choose_expe == "expe_2":
        experiment_contexts    = pd.read_csv("./data/expe_2/csv_files/experiment_contexts.csv")
        persona_filter_logs    = pd.read_csv("./data/expe_2/csv_files/persona_filter_logs.csv")
        profile_video_contexts = pd.read_csv("./data/expe_2/csv_files/profile_video_contexts.csv")
        profiles               = pd.read_csv("./data/expe_2/csv_files/profiles.csv")
        recommendation_log     = pd.read_csv("./data/expe_2/csv_files/recommendation_log.csv")
        sessions               = pd.read_csv("./data/expe_2/csv_files/sessions.csv")
        videos                 = pd.read_csv("./data/expe_2/csv_files/videos.csv")


        load_if_not_exists(experiment_contexts, "experiment_contexts")
        load_if_not_exists(persona_filter_logs, "persona_filter_logs")
        load_if_not_exists(profile_video_contexts, "profile_video_contexts")
        load_if_not_exists(profiles, "profiles")
        load_if_not_exists(recommendation_log, "recommendation_log")
        load_if_not_exists(process_session_table(sessions), "sessions")
        load_if_not_exists(videos, "videos")

db_connection.close()
#print(sessions.head())
