from collections import Counter, defaultdict
from pathlib import Path
import re

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_FOLDER = PROJECT_ROOT / "results" / "rq2"


def get_on_section(workflow_dict):
    """Ritorna il contenuto della chiave `on`, gestendo anche il parsing YAML 1.1 (`on` -> True)."""
    if not isinstance(workflow_dict, dict):
        return None

    if "on" in workflow_dict:
        return workflow_dict["on"]

    # In PyYAML, la chiave `on` non quotata puo essere interpretata come boolean True.
    if True in workflow_dict:
        return workflow_dict[True]

    for k, v in workflow_dict.items():
        if isinstance(k, str) and k.strip().lower() == "on":
            return v

    return None


def extract_triggers(on_value):
    """Estrae i trigger principali dal campo `on` di una workflow GitHub Actions."""
    if on_value is None:
        return []

    if isinstance(on_value, str):
        return [on_value.strip()] if on_value.strip() else []

    if isinstance(on_value, list):
        triggers = []
        for item in on_value:
            if isinstance(item, str) and item.strip():
                triggers.append(item.strip())
            elif isinstance(item, dict):
                triggers.extend([str(k).strip()
                                for k in item.keys() if str(k).strip()])
        return sorted(set(triggers))

    if isinstance(on_value, dict):
        return sorted({str(k).strip() for k in on_value.keys() if str(k).strip()})

    return []


def extract_job_keywords(workflow_dict):
    """Estrae le chiavi top-level della sezione jobs (es. tests, build, deploy)."""
    if not isinstance(workflow_dict, dict):
        return []

    jobs_section = workflow_dict.get("jobs")
    if isinstance(jobs_section, dict):
        return [str(k).strip() for k in jobs_section.keys() if str(k).strip()]

    return []


def classify_job_name(job_name: str, extended_vocab: bool = False) -> str:
    s = job_name.strip().lower().replace("_", "-")
    tokens = set(re.findall(r"[a-z0-9]+", s))

    def has_any(*words):
        return any(w in tokens for w in words)

    if has_any("deploy", "release", "publish", "ship", "rollout"):
        return "cd_release"
    if has_any("test", "tests", "integration", "e2e", "unit", "smoke"):
        return "ci_testing"
    if has_any("build", "compile", "package", "docker", "image"):
        return "ci_build"
    if has_any("lint", "format", "fmt", "style", "typecheck", "scan", "security", "sast"):
        return "quality_security"
    if has_any("stale", "cleanup", "clean", "cache", "close", "triage", "housekeeping"):
        return "repo_maintenance"
    if has_any("analyze", "analysis", "benchmark", "metrics", "report", "profile"):
        return "analysis_monitoring"
    if has_any("cron", "schedule", "sync", "backup", "rotate", "refresh"):
        return "scheduled_ops"

    if not extended_vocab:
        return "other"

    if has_any(
            "claude",
            "autopr",
            "auto",
            "fix",
            "run",
            "agent",
            "copilot",
            "llm",
            "gpt",
            "ai",
            "dogfood",
            "devin",
            "prompt",
            "assistant",
    ):
        return "ai_native"

    if has_any("pytest", "unittest", "unittests", "integrationtest", "integrationtests"):
        return "ci_testing"

    if has_any(
            "check",
            "checks",
            "pre",
            "commit",
            "codespell",
            "spell",
            "size",
            "conflict",
            "conflicts",
            "mypy",
            "pyright",
            "type",
            "format",
            "verify",
            "version",
            "changed",
            "files",
    ):
        return "quality_security"

    if has_any("prerelease", "artifact", "publish", "release"):
        return "cd_release"

    if has_any("migrate", "migration", "cleanup", "housekeeping"):
        return "repo_maintenance"

    if has_any("notify", "notification", "slack", "teams", "mail", "email", "webhook"):
        return "scheduled_ops"

    if has_any("main", "changes"):
        return "analysis_monitoring"

    return "other"


def group_jobs_generic(job_counter: dict, extended_vocab: bool = False) -> dict:
    grouped = defaultdict(lambda: {"total": 0, "items": {}})
    for job, count in sorted(job_counter.items()):
        g = classify_job_name(job, extended_vocab)
        grouped[g]["total"] += int(count)
        grouped[g]["items"][job] = int(count)
    return dict(sorted(grouped.items(), key=lambda x: (-x[1]["total"], x[0])))


def plot_job_category_pie(
        grouped_data,
        title="Distribuzione categorie job",
        legend_title="Categorie",
        figsize=(10, 8),
        startangle=140,
        output_path=None,
        color_map=None,
):
    labels = list(grouped_data.keys())
    values = [v["total"] for v in grouped_data.values()]
    total = sum(values)

    fig, ax = plt.subplots(figsize=figsize)

    colors = [color_map.get(label) for label in labels] if color_map else None
    wedges, _, _ = ax.pie(
        values,
        startangle=startangle,
        autopct=lambda p: f"{p:.1f}%",
        pctdistance=0.75,
        wedgeprops={"edgecolor": "white", "linewidth": 1},
        colors=colors,
    )

    legend_labels = [
        f"{lbl} (n={val}, {val / total * 100:.1f}%)" for lbl, val in zip(labels, values)
    ]
    ax.legend(
        wedges,
        legend_labels,
        title=legend_title,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        frameon=False,
    )

    ax.set_title(title)
    ax.axis("equal")
    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
    # plt.show()


def bar_chart(ax, labels, values, color):
    bars = ax.barh(labels[::-1], values[::-1], color=color,
                   edgecolor="white", linewidth=0.6)
    ax.set_xlabel("Conteggio", fontsize=10)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="y", labelsize=9)
    for bar, val in zip(bars, values[::-1]):
        ax.text(
            bar.get_width() + max(values) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            str(val),
            va="center",
            ha="left",
            fontsize=8,
            color="#333",
        )
    ax.set_xlim(0, max(values) * 1.12)


def build_color_map(labels):
    cmap = plt.get_cmap("tab20")
    colors = [cmap(i % cmap.N) for i in range(len(labels))]
    return dict(zip(labels, colors))


def analyze_dataset(dataset_path, workflows_path):
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

    stats_per_repo = {}
    total_trigger_counter = Counter()
    total_trigger_counter_unique = Counter()

    for repo in repo_summary["repository"].unique():
        files = repo_summary.loc[repo_summary["repository"]
                                 == repo, "files"].values[0]
        repo_triggers = Counter()

        for file in files:
            path = workflows_path / repo / file
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
            except Exception:
                continue

            on_section = get_on_section(data)
            triggers = extract_triggers(on_section)

            if not triggers:
                continue

            repo_triggers.update(triggers)
            total_trigger_counter.update(triggers)
            total_trigger_counter_unique.update(set(triggers))

        stats_per_repo[repo] = dict(repo_triggers)

    text_lines.append("Top trigger totali:")
    text_lines.append(repr(total_trigger_counter.most_common(20)))
    text_lines.append("Top trigger unici per file:")
    text_lines.append(repr(total_trigger_counter_unique.most_common(20)))

    # calcolo in percentuale quanto e diffusa ogni trigger rispetto al totale
    total_files_with_trigger = sum(total_trigger_counter_unique.values())

    trigger_percentage_df = (
        pd.DataFrame(
            [
                {
                    "trigger": trigger,
                    "percentage": (
                        (count / total_files_with_trigger) * 100
                        if total_files_with_trigger
                        else 0.0
                    ),
                }
                for trigger, count in total_trigger_counter_unique.items()
            ]
        )
        .sort_values(["percentage", "trigger"])
        .reset_index(drop=True)
    )

    text_lines.append("trigger_percentage_df:")
    text_lines.append(trigger_percentage_df.to_string(index=False))

    trigger_stats = defaultdict(
        lambda: {"total": 0, "jobs_counter": Counter()})

    for repo in repo_summary["repository"].unique():
        files = repo_summary.loc[repo_summary["repository"]
                                 == repo, "files"].values[0]

        for file in files:
            path = workflows_path / repo / file

            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
            except Exception:
                continue

            if not isinstance(data, dict):
                continue

            triggers = extract_triggers(get_on_section(data))
            if not triggers:
                continue

            job_keywords = extract_job_keywords(data)

            # Conta una volta per trigger per file workflow
            for trigger in set(triggers):
                trigger_stats[trigger]["total"] += 1
                trigger_stats[trigger]["jobs_counter"].update(job_keywords)

    result = {
        trigger: {
            "total": values["total"],
            "most_associated_keywords": dict(values["jobs_counter"].most_common(20)),
        }
        for trigger, values in sorted(
            trigger_stats.items(),
            key=lambda item: item[1]["total"],
            reverse=True,
        )
    }

    grouped_by_trigger = {}
    if "schedule" in result and "most_associated_keywords" in result["schedule"]:
        text_lines.append("schedule most_associated_keywords:")
        text_lines.append(repr(result["schedule"]["most_associated_keywords"]))
        generic_grouped = group_jobs_generic(
            result["schedule"]["most_associated_keywords"])
        text_lines.append("schedule grouped:")
        text_lines.append(repr(generic_grouped))
        grouped_by_trigger["schedule"] = generic_grouped

    if "push" in result and "most_associated_keywords" in result["push"]:
        push_grouped = group_jobs_generic(
            result["push"]["most_associated_keywords"],
            extended_vocab=True,
        )
        text_lines.append("push grouped:")
        text_lines.append(repr(push_grouped))
        grouped_by_trigger["push"] = push_grouped

    if "pull_request" in result and "most_associated_keywords" in result["pull_request"]:
        pr_grouped = group_jobs_generic(
            result["pull_request"]["most_associated_keywords"],
            extended_vocab=True,
        )
        text_lines.append("pull_request grouped:")
        text_lines.append(repr(pr_grouped))
        grouped_by_trigger["pull_request"] = pr_grouped

    if "workflow_dispatch" in result and "most_associated_keywords" in result["workflow_dispatch"]:
        wdispatch_grouped = group_jobs_generic(
            result["workflow_dispatch"]["most_associated_keywords"],
            extended_vocab=True,
        )
        text_lines.append("workflow_dispatch grouped:")
        text_lines.append(repr(wdispatch_grouped))
        grouped_by_trigger["workflow_dispatch"] = wdispatch_grouped

    return {
        "text_lines": text_lines,
        "total_trigger_counter_unique": total_trigger_counter_unique,
        "trigger_percentage_df": trigger_percentage_df,
        "grouped_by_trigger": grouped_by_trigger,
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
        (RESULTS_FOLDER / f"rq2_results_({cfg['label']}).txt").write_text(
            "\n".join(data["text_lines"]),
            encoding="utf-8",
        )

    TOP_N = 20
    for label, data in results.items():
        if not data["total_trigger_counter_unique"]:
            continue
        top_total = data["total_trigger_counter_unique"].most_common(TOP_N)
        labels_total, values_total = zip(*top_total)
        fig, ax = plt.subplots(figsize=(16, 6))
        fig.suptitle(
            f"GitHub Actions - top {TOP_N} triggers (sull'ultima versione di ogni file) ({label})",
            fontsize=15,
            fontweight="bold",
            y=1.02,
        )
        bar_chart(
            ax,
            labels_total,
            values_total,
            "#4C72B0",
        )
        plt.tight_layout()
        fig.savefig(RESULTS_FOLDER / f"triggers_distribution_({label}).png",
                    dpi=150, bbox_inches="tight")

    triggers = ["schedule", "push", "pull_request", "workflow_dispatch"]
    for trigger in triggers:
        combined_labels = set()
        for data in results.values():
            grouped = data["grouped_by_trigger"].get(trigger)
            if grouped:
                combined_labels.update(grouped.keys())
        if not combined_labels:
            continue
        color_map = build_color_map(sorted(combined_labels))
        for label, data in results.items():
            grouped = data["grouped_by_trigger"].get(trigger)
            if not grouped:
                continue
            plot_job_category_pie(
                grouped,
                title=f"Distribuzione categorie job per trigger {trigger} ({label})",
                output_path=RESULTS_FOLDER /
                f"{trigger}_job_categories_({label}).png",
                color_map=color_map,
            )


if __name__ == "__main__":
    main()
