#!/usr/bin/env python
"""
Visualize the Airflow DAG using matplotlib.
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
import numpy as np


def visualize_dag():
    tasks = [
        ("extract_data", "Extract\n(Baca CSV → Staging)"),
        ("validate_data", "Validate\n(Null + Duplicate Check)"),
        ("transform_data", "Transform\n(Agregasi + Bonus)"),
        ("load_data", "Load\n(SQLite + Visualisasi)"),
    ]
    
    edges = [
        ("extract_data", "validate_data"),
        ("validate_data", "transform_data"),
        ("transform_data", "load_data"),
    ]
    
    fig, ax = plt.subplots(1, 1, figsize=(14, 5))
    fig.patch.set_facecolor("#0b0d10")
    ax.set_facecolor("#14181d")
    ax.set_xlim(0, 14)
    ax.set_ylim(-1, 5)
    ax.axis("off")
    
    y_center = 2
    x_positions = np.linspace(1.5, 12.5, len(tasks))
    node_width = 2.2
    node_height = 1.5
    
    for i, (task_id, label) in enumerate(tasks):
        x = x_positions[i]
        
        box = FancyBboxPatch(
            (x - node_width/2, y_center - node_height/2),
            node_width, node_height,
            boxstyle="round,pad=0.1,rounding_size=0.15",
            facecolor="#1e252d",
            edgecolor="#c5cdc8",
            linewidth=2,
            mutation_scale=15,
        )
        ax.add_patch(box)
        
        ax.text(
            x, y_center + 0.15,
            task_id,
            ha="center", va="center",
            fontsize=11, fontweight="bold", color="#ececea",
            family="monospace"
        )
        ax.text(
            x, y_center - 0.35,
            label,
            ha="center", va="center",
            fontsize=9, color="#8b9298",
            family="sans-serif"
        )
    
    for i, (src, dst) in enumerate(edges):
        src_idx = next(j for j, (t, _) in enumerate(tasks) if t == src)
        dst_idx = next(j for j, (t, _) in enumerate(tasks) if t == dst)
        
        x_start = x_positions[src_idx] + node_width/2
        x_end = x_positions[dst_idx] - node_width/2
        y_start = y_center
        y_end = y_center
        
        arrow = FancyArrowPatch(
            (x_start, y_start), (x_end, y_end),
            arrowstyle="->,head_width=8,head_length=10",
            mutation_scale=25,
            linewidth=2.5,
            color="#7d9b86",
            connectionstyle="arc3,rad=0",
            zorder=10,
        )
        ax.add_patch(arrow)
    
    title = ax.text(
        7, 4.2,
        "DAG: etl_sederhana_572392",
        ha="center", va="center",
        fontsize=16, fontweight="bold", color="#ececea"
    )
    
    subtitle = ax.text(
        7, 3.7,
        "Sample Superstore ETL Pipeline — extract → validate → transform → load",
        ha="center", va="center",
        fontsize=11, color="#8b9298"
    )
    
    legend_elements = [
        mpatches.Patch(facecolor="#1e252d", edgecolor="#c5cdc8", label="Task"),
        mpatches.Patch(facecolor="#7d9b86", label="Dependency"),
    ]
    ax.legend(
        handles=legend_elements,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.08),
        ncol=2,
        frameon=False,
        fontsize=10,
        labelcolor="#8b9298",
    )
    
    plt.tight_layout()
    output_path = "/Users/andreasanditya/Downloads/572392_Andreas_AirflowETL/code/dag_visualization.png"
    fig.savefig(output_path, facecolor=fig.get_facecolor(), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"DAG visualization saved to: {output_path}")


if __name__ == "__main__":
    visualize_dag()