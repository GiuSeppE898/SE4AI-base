from collections import Counter
from pathlib import Path
import random

import pandas as pd
import yaml
import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_FOLDER = PROJECT_ROOT / "results" / "rq1"


def build_color_map(labels):
    cmap = plt.get_cmap("tab20")
    colors = [cmap(i % cmap.N) for i in range(len(labels))]
    return dict(zip(labels, colors))


def plot_pie_chart(counter, title, output_path=None, color_map=None):
    if not counter:
        return
    labels, values = zip(*counter.most_common(10))
    plt.figure(figsize=(8, 8))
    colors = [color_map.get(label) for label in labels] if color_map else None
    plt.pie(values, labels=labels, autopct="%1.1f%%",
            startangle=140, colors=colors)
    plt.title(title)
    plt.axis("equal")
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")


def extract_runners(job: dict) -> list[str]:
    runs_on = job.get("runs-on", "")
    strategy = job.get("strategy", {}) or {}
    matrix = strategy.get("matrix", {}) if isinstance(strategy, dict) else {}
    matrix = matrix if isinstance(matrix, dict) else {}
    os_list = matrix.get("os", [])
    os_list = os_list if isinstance(os_list, list) else []

    if isinstance(runs_on, str):
        if "matrix.os" in runs_on and os_list:
            return [str(o).strip() for o in os_list if str(o).strip()]
        return [runs_on.strip()] if runs_on.strip() else []
    if isinstance(runs_on, list):
        return [str(r).strip() for r in runs_on if str(r).strip()]
    return []


def run_dataset(label, dataset_path, workflows_path):
    random.seed(69)
    text_lines = []
    df = pd.read_csv(dataset_path)

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
    total_runner_counter = Counter()
    total_runner_counter_unique = Counter()

    for repo in repo_summary["repository"].unique():
        files = repo_summary[repo_summary["repository"]
                             == repo]["files"].values[0]
        repo_actions = Counter()
        repo_runners = Counter()

        for file in files:
            path = workflows_path / repo / file
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
                            runners = extract_runners(job)
                            if runners:
                                repo_runners.update(runners)
                                total_runner_counter.update(runners)
                                total_runner_counter_unique.update(
                                    set(runners))
            except Exception:
                continue

        stats_per_repo[repo] = dict(repo_actions)

    text_lines.append("total_uses_counter.most_common():")
    text_lines.append(repr(total_uses_counter.most_common()))
    text_lines.append("total_uses_counter_unique.most_common():")
    text_lines.append(repr(total_uses_counter_unique.most_common()))
    text_lines.append("most_used_languages.most_common():")
    text_lines.append(repr(most_used_languages.most_common()))
    text_lines.append("total_runner_counter.most_common():")
    text_lines.append(repr(total_runner_counter.most_common()))
    text_lines.append("total_runner_counter_unique.most_common():")
    text_lines.append(repr(total_runner_counter_unique.most_common()))
    text_lines.append("stats_per_repo:")
    text_lines.append(repr(stats_per_repo))

    return {
        "label": label,
        "text_lines": text_lines,
        "total_uses_counter": total_uses_counter,
        "total_uses_counter_unique": total_uses_counter_unique,
        "most_used_languages": most_used_languages,
        "most_used_languages_version": most_used_languages_version,
        "total_runner_counter": total_runner_counter,
        "total_runner_counter_unique": total_runner_counter_unique,
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
        data = run_dataset(
            cfg["label"], cfg["dataset_path"], cfg["workflows_path"])
        results[cfg["label"]] = data
        (RESULTS_FOLDER / f"rq1_results_({cfg['label']}).txt").write_text(
            "\n".join(data["text_lines"]),
            encoding="utf-8",
        )

    def top_labels(counter):
        return [k for k, _ in counter.most_common(10)]

    unique_action_labels = sorted(
        set(
            top_labels(results["A"]["total_uses_counter_unique"]) +
            top_labels(results["N.A."]["total_uses_counter_unique"])
        )
    )
    unique_action_colors = build_color_map(unique_action_labels)

    action_version_labels = sorted(
        set(
            top_labels(results["A"]["total_uses_counter"]) +
            top_labels(results["N.A."]["total_uses_counter"])
        )
    )
    action_version_colors = build_color_map(action_version_labels)

    language_labels = sorted(
        set(
            top_labels(results["A"]["most_used_languages"]) +
            top_labels(results["N.A."]["most_used_languages"])
        )
    )
    language_colors = build_color_map(language_labels)

    language_version_labels = sorted(
        set(
            top_labels(results["A"]["most_used_languages_version"]) +
            top_labels(results["N.A."]["most_used_languages_version"])
        )
    )
    language_version_colors = build_color_map(language_version_labels)

    runner_labels = sorted(
        set(
            top_labels(results["A"]["total_runner_counter"]) +
            top_labels(results["N.A."]["total_runner_counter"])
        )
    )
    runner_colors = build_color_map(runner_labels)

    runner_unique_labels = sorted(
        set(
            top_labels(results["A"]["total_runner_counter_unique"]) +
            top_labels(results["N.A."]["total_runner_counter_unique"])
        )
    )
    runner_unique_colors = build_color_map(runner_unique_labels)

    for label, data in results.items():
        plot_pie_chart(
            data["total_uses_counter_unique"],
            f"Top 10 Unique Actions Used in GitHub Workflows ({label})",
            RESULTS_FOLDER / f"top_10_unique_actions_({label}).png",
            unique_action_colors,
        )
        plot_pie_chart(
            data["total_uses_counter"],
            f"Top 10 Actions Used in GitHub Workflows (with versions) ({label})",
            RESULTS_FOLDER / f"top_10_actions_versions_({label}).png",
            action_version_colors,
        )
        plot_pie_chart(
            data["most_used_languages"],
            f"Top 10 Unique Languages Used in GitHub AI Repos ({label})",
            RESULTS_FOLDER / f"top_10_languages_({label}).png",
            language_colors,
        )
        plot_pie_chart(
            data["most_used_languages_version"],
            f"Top 10 Languages Used in GitHub AI Repos (with versions) ({label})",
            RESULTS_FOLDER / f"top_10_languages_versions_({label}).png",
            language_version_colors,
        )
        plot_pie_chart(
            data["total_runner_counter"],
            f"Top 10 Runners Used in GitHub AI Repos ({label})",
            RESULTS_FOLDER / f"top_10_runners_({label}).png",
            runner_colors,
        )
        plot_pie_chart(
            data["total_runner_counter_unique"],
            f"Top 10 Unique Runners Used in GitHub AI Repos (with versions) ({label})",
            RESULTS_FOLDER / f"top_10_unique_runners_({label}).png",
            runner_unique_colors,
        )


if __name__ == "__main__":
    main()
