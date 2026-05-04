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
DATASET_PATH = PROJECT_ROOT / "gigawork_non_agentic_results" / "dataset_with_ids.csv"
BASE_GIGAWORK_PATH = PROJECT_ROOT / \
    "gigawork_non_agentic_results" / "all_workflows"
RESULTS_FOLDER = PROJECT_ROOT / "results" / "rq3_non_agentic"


def main() -> None:
    # RQ3: Quali pattern ricorrenti caratterizzano i workflow CI/CD nei progetti
    # AI-agent open source su GitHub?
    # NB: controllare quanti falsi positivi rimangono nell'estrazione delle categorie

    RESULTS_FOLDER.mkdir(parents=True, exist_ok=True)
    text_lines = []

    random.seed(69)

    df = pd.read_csv(DATASET_PATH)

    # carico i file hash univoci
    df_unique = df.drop_duplicates(subset=["workflow_global_id"])

    file_hash_by_repo = df_unique.groupby(
        "repository")["file_hash"].apply(list)

    result = {}

    for repository_name in file_hash_by_repo.index:
        file_hashes = file_hash_by_repo[repository_name]
        repo_results = []
        for file_hash in file_hashes:
            workflow_path = BASE_GIGAWORK_PATH / \
                repository_name / f"{file_hash}"
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

    # e inutile analizzare i workflow con label unknown perche sembrano rappresentare tutti
    # workflow legati a github, quindi chiusura stale pr, commenti, etc etc.

    # conto quanti workflow file fanno soltanto una cosa e quanti invece fanno piu cose
    # (es. test + build) e quanti sono unknown
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

    # somma totale di single, multi e unknown
    total_single = sum(counts["single"] for counts in label_counts.values())
    total_multi = sum(counts["multi"] for counts in label_counts.values())
    labels = ["Single Label", "Multi Label"]
    sizes = [total_single, total_multi]
    plt.figure(figsize=(8, 8))
    plt.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=140)
    plt.title("Distribution of Workflow Labels")
    plt.axis("equal")
    plt.savefig(RESULTS_FOLDER / "workflow_labels_distribution.png",
                dpi=150, bbox_inches="tight")
    plt.show()

    # quali sono le label piu comuni nei workflow multi-label?
    # (es. test + build, test + deploy, ecc.) e quali sono le combinazioni di label piu comuni?
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

    mask = np.tril(np.ones_like(matrix, dtype=bool),
                   k=0)  # k=0 include diagonale

    plt.figure(figsize=(8, 6))
    sns.heatmap(matrix, annot=True, fmt="d", cmap="Blues", mask=mask)
    plt.title("Label Co-occurrence Matrix (Upper Triangle)")
    plt.xlabel("Label")
    plt.ylabel("Label")
    plt.tight_layout()
    plt.savefig(RESULTS_FOLDER / "label_cooccurrence_matrix.png",
                dpi=150, bbox_inches="tight")
    plt.show()

    # le label single piu comuni quali sono?
    single_label_counts = Counter()
    for repo, workflows in result.items():
        for wf in workflows:
            labels = wf.get("labels", [])
            if len(labels) == 1:
                single_label_counts[labels[0]] += 1
    text_lines.append("single_label_counts.most_common():")
    text_lines.append(repr(single_label_counts.most_common()))

    (RESULTS_FOLDER / "rq3_results.txt").write_text(
        "\n".join(text_lines),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
