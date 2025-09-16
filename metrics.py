import pandas as pd
import numpy as np

def compute_lean_score(df: pd.DataFrame) -> float:
    mapping = {
        "Pro-Israel": -1,
        "Pro-Palestine": 1,
        "Neutral": 0,
        "Unrelated": 0,
        "Undefined": 0
    }
    df = df.copy()
    df["score"] = df["llm_label"].map(mapping)
    return df["score"].mean()


def compute_proportions(df: pd.DataFrame) -> dict:
    total = len(df)
    proportions = df["llm_label"].value_counts(normalize=True).to_dict()
    return [proportions/total for proportions in proportions.values()]


def compute_shanon_entropy(proportions: list) -> float:
    """Compute the Shannon entropy given a list of proportions."""
    proportions = np.array(proportions)
    proportions = proportions[proportions > 0]  # Remove zero proportions to avoid log(0)
    return -np.sum(proportions * np.log2(proportions))