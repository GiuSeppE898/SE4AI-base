from collections import Counter
from pathlib import Path
import random

import pandas as pd
import yaml
import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASET_PATH = PROJECT_ROOT / "gigawork_non_agentic_results" / "dataset_with_ids.csv"
BASE_GIGAWORK_PATH = PROJECT_ROOT / \
    "gigawork_non_agentic_results" / "all_workflows"
RESULTS_FOLDER = PROJECT_ROOT / "results" / "rq1_non_agentic"


def plot_pie_chart(counter, title, output_path=None):
    labels, values = zip(*counter.most_common(10))
    plt.figure(figsize=(8, 8))
    plt.pie(values, labels=labels, autopct="%1.1f%%", startangle=140)
    plt.title(title)
    plt.axis("equal")
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
    # plt.show()


def main() -> None:
    # RQ1: Quali sono i building blocks piu comuni nei workflows degli agenti AI?
    # suggerimento del prof: I task/action piu usati (uses: actions/checkout@v3,
    # uses: actions/setup-python@v4, ecc.) — capire quali sono le building block
    # piu comuni in questi progetti

    random.seed(69)

    RESULTS_FOLDER.mkdir(parents=True, exist_ok=True)
    text_lines = []

    df = pd.read_csv(DATASET_PATH)

    repositories = df.groupby("repository")

    # per ogni repository prendiamo tutti i file hash associati allo stesso file hash,
    # se il file e stato eliminato prendiamo l'ultimo.
    tmp = df.dropna(subset=["repository", "workflow_global_id"]).copy()

    # timestamp per prendere l'ultima modifica effettuata al file
    tmp["event_ts"] = pd.to_numeric(tmp["committed_date"], errors="coerce")
    tmp["event_ts"] = tmp["event_ts"].fillna(
        pd.to_numeric(tmp["authored_date"], errors="coerce"))
    tmp["file_h"] = tmp["file_hash"].fillna(tmp["previous_file_hash"])

    last_file = (
        tmp.sort_values(["repository", "workflow_global_id", "event_ts"])
        .groupby(["repository", "workflow_global_id"], as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )

    repo_summary = (
        last_file.groupby("repository", as_index=False)
        .agg(
            files=("file_h", lambda s: [x for x in s.dropna().unique()]),
        )
        .reset_index(drop=True)
    )

    text_lines.append("repo_summary.head(5):")
    text_lines.append(repo_summary.head(5).to_string(index=False))

    stats_per_repo = {}
    total_uses_counter = Counter()
    total_uses_counter_unique = Counter()
    most_used_languages = Counter()
    most_used_languages_version = Counter()

    for repo in repo_summary["repository"].unique():
        files = repo_summary[repo_summary["repository"]
                             == repo]["files"].values[0]
        repo_actions = Counter()

        for file in files:
            path = BASE_GIGAWORK_PATH / repo / file
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                    if isinstance(data, dict) and "jobs" in data:
                        for job in data["jobs"].values():
                            if isinstance(job, dict) and "steps" in job:
                                for step in job["steps"]:
                                    if "uses" in step:
                                        action = step["uses"]
                                        if action.startswith("actions/setup"):
                                            language = action.split(
                                                "actions/setup-")[1]
                                            language_no_v = language.split(
                                                "@")[0]
                                            most_used_languages[language_no_v] += 1
                                            most_used_languages_version[language] += 1

                                        unique_action = step["uses"].split(
                                            "@")[0]
                                        repo_actions[unique_action] += 1
                                        total_uses_counter[action] += 1
                                        total_uses_counter_unique[unique_action] += 1
            except Exception:
                continue

        stats_per_repo[repo] = dict(repo_actions)

    text_lines.append("total_uses_counter.most_common():")
    text_lines.append(repr(total_uses_counter.most_common()))
    text_lines.append("total_uses_counter_unique.most_common():")
    text_lines.append(repr(total_uses_counter_unique.most_common()))
    text_lines.append("most_used_languages.most_common():")
    text_lines.append(repr(most_used_languages.most_common()))
    text_lines.append("stats_per_repo:")
    text_lines.append(repr(stats_per_repo))

    # grafico a torta per total_uses_counter_unique e total_uses_counter con almeno 10 utilizzi
    plot_pie_chart(
        total_uses_counter_unique,
        "Top 10 Unique Actions Used in GitHub Workflows",
        RESULTS_FOLDER / "top_10_unique_actions.png",
    )
    plot_pie_chart(
        total_uses_counter,
        "Top 10 Actions Used in GitHub Workflows (with versions)",
        RESULTS_FOLDER / "top_10_actions_versions.png",
    )

    plot_pie_chart(
        most_used_languages,
        "Top 10 Unique Languages Used in GitHub AI Repos",
        RESULTS_FOLDER / "top_10_languages.png",
    )
    plot_pie_chart(
        most_used_languages_version,
        "Top 10 Languages Used in GitHub AI Repos (with versions)",
        RESULTS_FOLDER / "top_10_languages_versions.png",
    )

    (RESULTS_FOLDER / "rq1_results.txt").write_text(
        "\n".join(text_lines),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
