from DB_connection import DB_connection

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


def get_videos_watched_in_session(session_id):
    query = f"""
              SELECT *
              FROM annoted_videos v
              JOIN recommendation_log rl ON v.video_id = rl.video_id
              WHERE rl.session_id = '{session_id}'
            """
    results = db_connection.select(query)

    return results






print(get_videos_watched_in_session("987d7d32-e463-4c46-a057-64a22cc5f9db").head())



