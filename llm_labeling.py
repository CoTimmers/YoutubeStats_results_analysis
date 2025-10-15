import os
from dotenv import load_dotenv
import json
import time
import pandas as pd
from typing import  Optional
from typing import Literal
from pydantic import BaseModel
from pydantic import BaseModel, Field, ValidationError
from langchain_openai import ChatOpenAI
from typing import Optional, List, Callable, Tuple

from config import stances, provider
from DB_connection import DB_connection




db_connection = DB_connection()

# -------------------
# Load environment
# -------------------
load_dotenv()
#provider = (os.getenv("LLM_PROVIDER") or "").lower().strip()

AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_MODEL = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4.1")

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY") or os.getenv("OPEN_ROUTER_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "mistralai/mistral-small-3.2-24b-instruct")

if provider == "azure":
    model = AZURE_OPENAI_MODEL
elif provider == "openrouter":
    model = OPENROUTER_MODEL

stances_string = "".join(f"- {s}\n" for s in stances)
stances_json_vec = json.dumps(stances, ensure_ascii=False)



def merge_videos_tables():
    try:
        query = f"SELECT * FROM  annoted_videos"
        annoted_videos = db_connection.select(query)

        query = f"SELECT * FROM videos"
        videos = db_connection.select(query)
        
        columns = videos.columns.tolist()
        merged_df = pd.merge(videos, annoted_videos, on=columns, how="outer")

        db_connection.save_df(merged_df, "annoted_videos")

    except:
        print("Creating annoted_videos table")
        query ="SELECT * FROM videos"
        videos = db_connection.select(query)
        videos["llm_label"] = None
        videos["llm_justification"] = None
        db_connection.save_df(videos, "annoted_videos")
        merge_videos_tables()


class VideoClassification(BaseModel):
    video_id: str = Field(..., description="YouTube video id")
    label: str = Field(..., description=f"One of: {', '.join(stances)}")
    justification: str

class BatchClassification(BaseModel):
    items: List[VideoClassification]

    



def get_langchain_llm(provider: str, model: Optional[str] = None) -> ChatOpenAI:
    """Return a LangChain ChatOpenAI object configured for Azure or OpenRouter."""
    if provider == "azure":
        if not model:
            model = AZURE_OPENAI_MODEL
        return ChatOpenAI(
            api_key=AZURE_OPENAI_KEY,
            api_version="2024-02-15-preview",
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            model=model,
            temperature=0.0, # for deterministic output
        )

    elif provider == "openrouter":
        if not model:
            model = OPENROUTER_MODEL
        return ChatOpenAI(
            api_key=OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1",
            model=model,
            temperature=0.0, # for deterministic output
        )
    else:
        raise ValueError(f"Unsupported LLM_PROVIDER: {provider}")
    

def build_prompt(videos_df: pd.DataFrame , include_transcripts: bool) -> str:

    videos_blocks = f""
    for index, row in videos_df.iterrows():
        video_id = row.get('video_youtube_id', '') or ''
        video_title = row.get('title', '') or ''
        channel_name = row.get('channel_name', '') or ''
        description = (row.get('description', '') or '')[:1000]  # trim generously
        transcript_text = (row.get('transcript', '') or '')[:5000] if include_transcripts else ''

        transcript_block = (
            f"\nTranscript : {transcript_text}\n"
            if include_transcripts and transcript_text else
            "\nTranscript : [omitted]\n"
        )
        videos_blocks += (
            f"\nVideo ID: {video_id}\n"
            f"Title: {video_title}\n"
            f"Channel: {channel_name}\n"
            f"Description: {description}\n"
            f"{transcript_block}"
            "----\n"
        )
    
    prompt = f"""
                Task: Classify the following YouTube video into exactly one of the categories below.
                {stances_string}
                The classification should consider ideological stance, emotional tone, and narrative alignment.
                Use the video title, channel name, and description. Use the transcript only if provided.
                Be precise and objective; do not infer opinions unless clearly expressed.

                {videos_blocks}

                If the video is off-topic or irrelevant, label it as "Unrelated".
                If the video lacks sufficient information, label it as "Undefined".

                Label : Choose from the JSON array {stances_json_vec}.
                Justification : Briefly explain why you chose this label, referring to specific cues.
                Return only the structured result as per the schema.
                """.strip()
    
    return prompt

def query_llm(prompt, llm_structured) -> BatchClassification:
    return llm_structured.invoke(prompt)




def annotate_by_batch(llm_structured, batch_size: int = 10, include_transcripts: bool = True ):


    videos_df = db_connection.select(f"SELECT * FROM annoted_videos WHERE llm_label IS NULL LIMIT {batch_size} ")
    print(len(videos_df), "videos to annotate")
    prompt = build_prompt(videos_df, include_transcripts)
    response = query_llm(prompt, llm_structured)

    for item in response.items:

        query = f"""
            UPDATE annoted_videos
            SET llm_label = ?, 
            llm_justification = ?
            WHERE video_youtube_id = ?
        """    
        cursor = db_connection.connection.cursor()
        cursor.execute(query,(item.label, item.justification, item.video_id))
    db_connection.connection.commit()

    return videos_df


if __name__ == "__main__":

    merge_videos_tables()

    llm = get_langchain_llm(provider, model)
    llm_structured = llm.with_structured_output(BatchClassification)

    continue_bool = True
    previous_videos_df = pd.DataFrame()
    while continue_bool:
        try:
            videos_df = annotate_by_batch(llm_structured, batch_size=10, include_transcripts=True)
        except:
            print("Error querying LLM, retrying in 10 seconds...")

        if videos_df.reset_index(drop=True).equals(previous_videos_df.reset_index(drop=True)):
            continue_bool = False
        previous_videos_df = videos_df
        if len(videos_df) == 0:
            continue_bool = False












    










