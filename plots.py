import math
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches

from typing import Optional
from scipy import stats



def plot_with_flags(data: Optional[list] = None, title: str = "", labels: list = None, with_confidence_interval:bool=False, plot_mean:bool=False):
    fig, ax = plt.subplots(figsize=(12, 5))

    alpha = 0.1  # Transparency level

    # -----------------
    # PALESTINIAN FLAG (y in [0,1])
    # -----------------
    if data is not None:
        if data[0] is not None:
            if data[0].ndim == 1:
                nbr_videos_seen=len(data[0])
            else:
                nbr_videos_seen=len(data[0][0])
    #nbr_videos_seen=len(data[0]) if data is not None else 50

    nbr_videos_seen=nbr_videos_seen-1
    
    ax.add_patch(patches.Polygon([[0,1],[nbr_videos_seen,1],[nbr_videos_seen,2/3],[nbr_videos_seen/3,2/3]], facecolor="black", alpha=alpha))
    ax.add_patch(patches.Rectangle((0, 1/3), 50, 1/3, facecolor="white", alpha=alpha))
    ax.add_patch(patches.Polygon([[0,0],[nbr_videos_seen/3,1/3],[nbr_videos_seen,1/3],[nbr_videos_seen,0]], facecolor="green", alpha=alpha))
    triangle = patches.Polygon([[0, 0], [0, 1], [25, 0.5]], closed=True, facecolor="red", alpha=alpha)
    ax.add_patch(triangle)

    # -----------------
    # ISRAELI FLAG (y in [0,-1])
    # -----------------
    ax.add_patch(patches.Rectangle((0, -1), 50, 1, facecolor="white", edgecolor="black", alpha=alpha))

    # Blue stripes
    ax.add_patch(patches.Rectangle((0, -0.2), 50, 0.2, facecolor="blue", alpha=alpha))
    ax.add_patch(patches.Rectangle((0, -1), 50, 0.2, facecolor="blue", alpha=alpha))

    # Function: equilateral triangle points

    m_x = nbr_videos_seen / 2
    m_y = -0.5
    size = 0.2

    scale_factor = 2

    ax.add_patch(patches.Polygon([[m_x, m_y + size],[(m_x + math.sqrt(3)/2* size)+scale_factor ,m_y-size/2],[m_x - math.sqrt(3)/2* size -scale_factor,m_y-size/2]], facecolor="none",  edgecolor="blue", linewidth=4,alpha=alpha))
    ax.add_patch(patches.Polygon([[(m_x -math.sqrt(3)/2* size)-scale_factor, m_y + size/2],[m_x +math.sqrt(3)/2* size+scale_factor, m_y + size/2],[m_x, m_y - size]], facecolor="none", edgecolor="blue", linewidth=4, alpha=alpha))

    nbr_videos_seen=nbr_videos_seen+1  # to account for 0 indexing
    
    if labels is None:
        labels = [f"Series {i+1}" for i in range(len(data))]

    
    if data is not None:
        algo_lean = np.array([0]*nbr_videos_seen)
        if with_confidence_interval:
            if data[0].ndim == 1:
                print("Please provide full data for confidence interval plotting, processing without confidence interval!")
                for vec,label in zip(data,labels):
                    plt.plot(vec, linewidth=4,label = label)
            else:
                for mat,label in zip(data,labels):
                    x = np.arange(mat.shape[1])
                    mean_lean = np.mean(mat, axis=0)
                    algo_lean = algo_lean + mean_lean
                    confidence_interval = stats.sem(mat, axis=0) * 1.96
                    plt.plot(mean_lean,label=label)
                    plt.fill_between(x, mean_lean - confidence_interval, 
                            mean_lean + confidence_interval,  alpha=0.2)
        else:
            if data[0].ndim != 1:
                for mat,label in zip(data,labels):
                    x = np.arange(mat.shape[1])
                    mean_lean = np.mean(mat, axis=0)
                    confidence_interval = stats.sem(mat, axis=0) * 1.96
                    plt.plot(mean_lean, linewidth =4, label=label)
            else:
                for vec,label in zip(data,labels):
                    plt.plot(vec, linewidth=4,label = label)

        if plot_mean:
            algo_lean = algo_lean/len(data)
            plt.plot(algo_lean,linewidth=2, label="Mean lean", color="black")


    # -----------------
    # Formatting
    # -----------------
    ax.set_xlim(0, 50)
    ax.set_ylim(-1, 1)
    ax.set_xlabel("Step ($n$)", fontsize=15)
    ax.set_ylabel("Lean", fontsize=15)
    ax.set_title(title, fontsize=20)
    ax.legend(fontsize=12)

    plt.show()

    

def plot_proportions_evolution(proportions_context:dict, propotions_experiment:dict, title:str =None):
    # Take the union of keys (sorted for consistency)
    all_keys = sorted(set(proportions_context.keys()) | set(propotions_experiment.keys()))

    # Align values (use 0 if a key is missing in one dict)
    values1 = [proportions_context.get(k, 0) for k in all_keys]
    values2 = [propotions_experiment.get(k, 0) for k in all_keys]

    # X positions
    x = np.arange(len(all_keys))
    width = 0.35

    # Plot
    plt.bar(x - width/2, values1, width, label="During context", color="skyblue", edgecolor="black")
    plt.bar(x + width/2, values2, width, label="During experiment", color="orange", edgecolor="black")

    plt.xticks(x, all_keys)
    plt.ylabel("Proportion")
    plt.title(title)
    plt.legend()
    plt.show()


def plot_diversity_evolution(data:list, title:str="",labels: list = None, with_confidence_interval:bool=False):
    fig, ax = plt.subplots(figsize=(12, 5))
    if with_confidence_interval is not None:
        for mat,label in zip(data,labels):
            x = np.arange(mat.shape[1])
            mean_diversity = np.mean(mat, axis=0)
            confidence_interval = stats.sem(mat, axis=0) * 1.96 
            plt.plot(mean_diversity,label=label)
            plt.fill_between(x, mean_diversity - confidence_interval, 
                    mean_diversity + confidence_interval,  alpha=0.2)

        
    else:
        if labels is None:
            labels = [f"Series {i+1}" for i in range(len(data))]

        for vec,label in zip(data,labels):
            plt.plot(vec,label=label)
    ax.set_xlabel("Number of videos seen", fontsize=15)
    ax.set_ylabel("Diversity score", fontsize=15)
    ax.set_title(title, fontsize=20)
    ax.legend(fontsize=12)

    plt.show()




