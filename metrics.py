import pandas as pd
import numpy as np


mapping = {
    "Pro-Israel": -1,
    "Pro-Palestine": 1,
    "Neutral": 0,
    "Unrelated": 0,
    "Undefined": 0
}

def compute_lean_score(df: pd.DataFrame) -> float:
    df = df.copy()
    df["score"] = df["llm_label"].map(mapping)
    return df["score"].mean()


def compute_rank_aware_lean_score(df: pd.DataFrame) -> float:
    df = df.copy()
    df["score"] = df["llm_label"].map(mapping)
    df["weight"] = 1 / np.log2(df["recommendation_rank"]+1)
    return (df["score"] * df["weight"]).sum() / df["weight"].sum()
        


def compute_proportions(df: pd.DataFrame) -> dict:
    proportions = df["llm_label"].value_counts(normalize=True).to_dict()
    return proportions


def compute_rank_aware_proportions(df: pd.DataFrame) -> dict:
    df = df.copy()
    df["weight"] = 1 / np.log2(df["recommendation_rank"]+1)
    weighted_counts = df.groupby("llm_label")["weight"].sum()
    total_weight = weighted_counts.sum()
    proportions = (weighted_counts / total_weight).to_dict()
    return proportions


def compute_shanon_entropy(proportions: list) -> float:
    """Compute the Shannon entropy given a list of proportions."""
    proportions = np.array(list(proportions.values()))
    proportions = proportions[proportions > 0]  # Remove zero proportions to avoid log(0)
    return -np.sum(proportions * np.log2(proportions))/np.log2(5)


