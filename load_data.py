import pandas as pd

from DB_connection import DB_connection


db_connection = DB_connection()

persona_filter_logs    = pd.read_csv("./data/csv_files/persona_filter_logs.csv")
profile_video_contexts = pd.read_csv("./data/csv_files/profile_video_contexts.csv")
profiles               = pd.read_csv("./data/csv_files/profiles.csv")
recommendation_log     = pd.read_csv("./data/csv_files/recommendation_log.csv")
sessions               = pd.read_csv("./data/csv_files/sessions.csv")
videos                 = pd.read_csv("./data/csv_files/videos.csv")

def load_if_not_exists(df, table_name):
    query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    result = db_connection.select(query)
    if result.empty:
        db_connection.save_df(df, table_name)
        print(f"Table '{table_name}' created and data loaded.")
    else:
        print(f"Table '{table_name}' already exists. Skipping load.")


load_if_not_exists(persona_filter_logs, "persona_filter_logs")
load_if_not_exists(profile_video_contexts, "profile_video_contexts")
load_if_not_exists(profiles, "profiles")
load_if_not_exists(recommendation_log, "recommendation_log")
load_if_not_exists(sessions, "sessions")
load_if_not_exists(videos, "videos")


#print(sessions.head())
