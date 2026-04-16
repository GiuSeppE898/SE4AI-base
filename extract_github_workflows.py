#!/usr/bin/env python3
import csv
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE_REPO_DIR = ROOT / "awesome-ai-agents"
README_PATH = SOURCE_REPO_DIR / "README.md"
OUTPUT_DIR = ROOT / "output"
WORKFLOWS_DIR = OUTPUT_DIR / "workflows"
REPORT_CSV = OUTPUT_DIR / "workflows_extraction_report.csv"
REPOS_CSV = OUTPUT_DIR / "repos_from_awesome_ai_agents.csv"
SUMMARY_JSON = OUTPUT_DIR / "extraction_summary.json"
TMP_DIR = ROOT / ".tmp_clones"
TMP_REPORTS_DIR = ROOT / ".tmp_reports"

REPO_PATTERN = re.compile(r"https://github.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)")
INVALID_REPO_OWNERS = {"orgs", "features"}


def extract_repositories(readme_text):
    repos = sorted(set(REPO_PATTERN.findall(readme_text)))
    cleaned = []
    for repo in repos:
        if repo.endswith("/"):
            repo = repo[:-1]
        if repo.lower() == "e2b-dev/awesome-ai-agents":
            continue
        owner = repo.split("/", 1)[0].lower()
        if owner in INVALID_REPO_OWNERS:
            continue
        cleaned.append(repo)
    return sorted(set(cleaned))


def safe_repo_dir_name(repo_full_name):
    return repo_full_name.replace("/", "__")


def ensure_clean_dir(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def write_repos_csv(repos):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with REPOS_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["repo"])
        for repo in repos:
            writer.writerow([repo])


def clone_and_extract_workflows(repo):
    """
    Extract workflows from a repository using gigawork.
    Returns metadata about the extraction including workflow count and file locations.
    """
    safe_name = safe_repo_dir_name(repo)
    clone_dir = TMP_DIR / safe_name
    workflows_output_dir = WORKFLOWS_DIR / safe_name
    gigawork_report = TMP_REPORTS_DIR / f"{safe_name}.csv"

    # Clean directories
    if clone_dir.exists():
        shutil.rmtree(clone_dir)
    if workflows_output_dir.exists():
        shutil.rmtree(workflows_output_dir)
    
    workflows_output_dir.mkdir(parents=True, exist_ok=True)
    TMP_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    clone_url = f"https://github.com/{repo}.git"
    
    # Run gigawork
    cmd = [
        "gigawork",
        clone_url,
        "-w", str(workflows_output_dir),
        "-o", str(gigawork_report),
        "-s", str(clone_dir),
        "-n", repo,
        "--no-headers",
    ]
    
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=300,
        )
        
        if result.returncode != 0:
            return {
                "repo": repo,
                "status": "gigawork_failed",
                "error": (result.stderr or result.stdout)[:500],
                "workflow_count": 0,
                "files": [],
            }
        
        # Count extracted workflow files
        if not workflows_output_dir.exists():
            return {
                "repo": repo,
                "status": "no_workflows",
                "error": "",
                "workflow_count": 0,
                "files": [],
            }
        
        # Collect workflow files
        # gigawork salva i file senza estensione, con nome = commit hash
        copied_files = []
        for p in sorted(workflows_output_dir.rglob("*")):
            if not p.is_file():
                continue
            
            copied_files.append(
                {
                    "workflow_file": str(p.relative_to(workflows_output_dir)).replace("\\", "/"),
                    "destination_path": str(p.relative_to(ROOT)).replace("\\", "/"),
                    "size_bytes": p.stat().st_size,
                }
            )
        
        status = "ok" if copied_files else "no_workflow_files"
        return {
            "repo": repo,
            "status": status,
            "error": "",
            "workflow_count": len(copied_files),
            "files": copied_files,
            "gigawork_report": str(gigawork_report) if gigawork_report.exists() else None,
        }
    
    except subprocess.TimeoutExpired:
        return {
            "repo": repo,
            "status": "timeout",
            "error": "Extraction timeout (>5 min)",
            "workflow_count": 0,
            "files": [],
        }
    except Exception as e:
        return {
            "repo": repo,
            "status": "error",
            "error": str(e)[:500],
            "workflow_count": 0,
            "files": [],
        }


def write_report_csv(results):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with REPORT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "repo",
                "status",
                "workflow_count",
                "workflow_file",
                "destination_path",
                "size_bytes",
                "error",
            ]
        )

        for row in results:
            if row["files"]:
                for wf in row["files"]:
                    writer.writerow(
                        [
                            row["repo"],
                            row["status"],
                            row["workflow_count"],
                            wf["workflow_file"],
                            wf["destination_path"],
                            wf["size_bytes"],
                            row["error"],
                        ]
                    )
            else:
                writer.writerow(
                    [
                        row["repo"],
                        row["status"],
                        row["workflow_count"],
                        "",
                        "",
                        "",
                        row["error"],
                    ]
                )


def write_summary(results):
    total_repos = len(results)
    repos_ok = sum(1 for r in results if r["status"] == "ok")
    repos_no_workflow = sum(1 for r in results if r["status"] in {"no_workflows", "no_workflow_yaml"})
    repos_failed = sum(1 for r in results if r["status"] in {"clone_failed", "sparse_checkout_failed"})
    total_workflows = sum(r["workflow_count"] for r in results)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": str(README_PATH.relative_to(ROOT)),
        "totals": {
            "repos": total_repos,
            "repos_with_workflows": repos_ok,
            "repos_without_workflows": repos_no_workflow,
            "repos_failed": repos_failed,
            "workflow_files": total_workflows,
        },
        "artifacts": {
            "repos_csv": str(REPOS_CSV.relative_to(ROOT)),
            "report_csv": str(REPORT_CSV.relative_to(ROOT)),
            "workflows_dir": str(WORKFLOWS_DIR.relative_to(ROOT)),
        },
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with SUMMARY_JSON.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def main():
    if not README_PATH.exists():
        raise SystemExit(f"README non trovato: {README_PATH}")

    text = README_PATH.read_text(encoding="utf-8")
    repos = extract_repositories(text)

    ensure_clean_dir(OUTPUT_DIR)
    ensure_clean_dir(WORKFLOWS_DIR)
    ensure_clean_dir(TMP_DIR)
    ensure_clean_dir(TMP_REPORTS_DIR)

    write_repos_csv(repos)

    results = []
    for idx, repo in enumerate(repos, start=1):
        print(f"[{idx}/{len(repos)}] Estrazione workflows da {repo} (con gigawork)")
        result = clone_and_extract_workflows(repo)
        results.append(result)

    write_report_csv(results)
    write_summary(results)

    # Cleanup
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    shutil.rmtree(TMP_REPORTS_DIR, ignore_errors=True)

    total_workflows = sum(r["workflow_count"] for r in results)
    print("\nEstrazione completata (gigawork)")
    print(f"Repository processati: {len(results)}")
    print(f"Workflow files estratti: {total_workflows}")
    print(f"Report CSV: {REPORT_CSV}")
    print(f"Summary JSON: {SUMMARY_JSON}")


if __name__ == "__main__":
    main()
