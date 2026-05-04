from collections import Counter
from pathlib import Path
import random

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import yaml

from util.guess_file_type import classify_workflow


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_FOLDER = PROJECT_ROOT / "results" / "rq3"


def analyze_dataset(dataset_path, workflows_path):
    text_lines = []
    random.seed(69)
    df = pd.read_csv(dataset_path)
    df_unique = df.drop_duplicates(subset=["workflow_global_id"])
    file_hash_by_repo = df_unique.groupby(
        "repository")["file_hash"].apply(list)
    result = {}
    for repository_name in file_hash_by_repo.index:
        file_hashes = file_hash_by_repo[repository_name]
        repo_results = []
        for file_hash in file_hashes:
            workflow_path = workflows_path / repository_name / f"{file_hash}"
            if workflow_path.exists():
                try:
                    with open(workflow_path, "r", encoding="utf-8") as f:
                        data = yaml.safe_load(f)
                    out = classify_workflow(data)
                    repo_results.append(
                        {
                            "file_hash": file_hash,
                            "wf_name": out.get("name"),
                            "labels": out.get("labels", []),
                            "matched_keywords": out.get("matched_keywords", {}),
                        }
                    )
                except Exception:
                    continue
        result[repository_name] = repo_results

    label_counts = {}
    for repo, workflows in result.items():
        single, multi, unknown = 0, 0, 0
        for wf in workflows:
            labels = wf.get("labels", [])
            if len(labels) == 1:
                single += 1
            elif len(labels) > 1:
                multi += 1
            else:
                unknown += 1
        label_counts[repo] = {
            "single": single,
            "multi": multi,
            "unknown": unknown,
        }

    text_lines.append("label_counts:")
    text_lines.append(repr(label_counts))

    total_single = sum(counts["single"] for counts in label_counts.values())
    total_multi = sum(counts["multi"] for counts in label_counts.values())

    multi_label_combinations = Counter()
    for repo, workflows in result.items():
        for wf in workflows:
            labels = wf.get("labels", [])
            if len(labels) > 1:
                multi_label_combinations[tuple(sorted(labels))] += 1
    text_lines.append("multi_label_combinations.most_common(10):")
    text_lines.append(repr(multi_label_combinations.most_common(10)))

    all_labels = ["build", "test", "deploy",
                  "lint", "security", "release", "eval"]
    matrix = pd.DataFrame(0, index=all_labels, columns=all_labels)
    all_workflows = [wf for workflows in result.values() for wf in workflows]
    for wf in all_workflows:
        labels = [l for l in wf.get("labels", []) if l in all_labels]
        for l1 in labels:
            for l2 in labels:
                if l1 != l2:
                    matrix.loc[l1, l2] += 1

    single_label_counts = Counter()
    for repo, workflows in result.items():
        for wf in workflows:
            labels = wf.get("labels", [])
            if len(labels) == 1:
                single_label_counts[labels[0]] += 1
    text_lines.append("single_label_counts.most_common():")
    text_lines.append(repr(single_label_counts.most_common()))

    return {
        "text_lines": text_lines,
        "total_single": total_single,
        "total_multi": total_multi,
        "matrix": matrix,
    }


def main() -> None:
    RESULTS_FOLDER.mkdir(parents=True, exist_ok=True)

    configs = [
        {
            "label": "A",
            "dataset_path": PROJECT_ROOT / "gigawork" / "dataset_with_ids.csv",
            "workflows_path": PROJECT_ROOT / "gigawork" / "all_workflows",
        },
        {
            "label": "N.A.",
            "dataset_path": PROJECT_ROOT / "gigawork_non_agentic_results" / "dataset_with_ids.csv",
            "workflows_path": PROJECT_ROOT / "gigawork_non_agentic_results" / "all_workflows",
        },
    ]

    results = {}
    for cfg in configs:
        data = analyze_dataset(cfg["dataset_path"], cfg["workflows_path"])
        results[cfg["label"]] = data
        (RESULTS_FOLDER / f"rq3_results_({cfg['label']}).txt").write_text(
            "\n".join(data["text_lines"]),
            encoding="utf-8",
        )

    pie_color_map = {
        "Single Label": "#4C72B0",
        "Multi Label": "#DD8452",
    }

    for label, data in results.items():
        labels = ["Single Label", "Multi Label"]
        sizes = [data["total_single"], data["total_multi"]]
        colors = [pie_color_map[lbl] for lbl in labels]
        plt.figure(figsize=(8, 8))
        plt.pie(sizes, labels=labels, autopct="%1.1f%%",
                startangle=140, colors=colors)
        plt.title(f"Distribution of Workflow Labels ({label})")
        plt.axis("equal")
        plt.savefig(RESULTS_FOLDER / f"workflow_labels_distribution_({label}).png",
                    dpi=150, bbox_inches="tight")

        mask = np.tril(np.ones_like(data["matrix"], dtype=bool), k=0)
        plt.figure(figsize=(8, 6))
        sns.heatmap(data["matrix"], annot=True,
                    fmt="d", cmap="Blues", mask=mask)
        plt.title(f"Label Co-occurrence Matrix (Upper Triangle) ({label})")
        plt.xlabel("Label")
        plt.ylabel("Label")
        plt.tight_layout()
        plt.savefig(RESULTS_FOLDER / f"label_cooccurrence_matrix_({label}).png",
                    dpi=150, bbox_inches="tight")


if __name__ == "__main__":
    main()
