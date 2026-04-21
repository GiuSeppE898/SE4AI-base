from github import Github
import json
import time
import requests
import pandas as pd

# ── Configurazione ──────────────────────────────────────────────────────────
TOKEN = "..."
DATASET_PATH = "C:\\dev\\SE4AI-base\\gigawork\\dataset.csv"
OUTPUT_DIR = "C:\\dev\\SE4AI-base\\github_api_results"

# ── Inizializzazione ─────────────────────────────────────────────────────────
print("[INFO] Inizializzazione client GitHub...")
g = Github(TOKEN)

http = requests.Session()
http.headers.update({
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
})

redirect_cache = {}


def resolve_owner_repo(owner_repo: str) -> str:
    if owner_repo in redirect_cache:
        return redirect_cache[owner_repo]

    url = f"https://api.github.com/repos/{owner_repo}"
    try:
        r = http.get(url, allow_redirects=True, timeout=20)
        if r.status_code == 200:
            full_name = r.json().get("full_name", owner_repo)
            redirect_cache[owner_repo] = full_name
            if full_name != owner_repo:
                print(f"  [REDIRECT] {owner_repo} → {full_name}")
            return full_name
    except Exception as e:
        print(f"  [WARN] Impossibile risolvere {owner_repo}: {e}")

    redirect_cache[owner_repo] = owner_repo
    return owner_repo


# ── Caricamento dataset ──────────────────────────────────────────────────────
print(f"[INFO] Caricamento dataset da: {DATASET_PATH}")
df = pd.read_csv(DATASET_PATH)
print(f"[INFO] Righe totali nel dataset: {len(df)}")

work = (
    df[["repository", "commit_hash"]]
    .dropna()
    .drop_duplicates()
)
print(
    f"[INFO] Coppie (repository, commit_hash) uniche da elaborare: {len(work)}")

# ── Raccolta workflow runs ────────────────────────────────────────────────────
results = []
errors = []

repos_list = list(work.groupby("repository"))
total_repos = len(repos_list)

for repo_idx, (repository_key, group) in enumerate(repos_list, start=1):
    original_owner_repo = repository_key.replace("__", "/", 1)
    print(f"\n[REPO {repo_idx}/{total_repos}] {original_owner_repo}")

    resolved_owner_repo = resolve_owner_repo(original_owner_repo)

    try:
        repo = g.get_repo(resolved_owner_repo)
        commits = group["commit_hash"].tolist()
        print(f"  [INFO] Commit da analizzare: {len(commits)}")

        for commit_idx, sha in enumerate(commits, start=1):
            print(
                f"  [COMMIT {commit_idx}/{len(commits)}] {sha[:12]}...", end=" ", flush=True)
            try:
                runs = repo.get_workflow_runs(head_sha=sha)
                time.sleep(1)

                run_count = 0
                for run in runs:
                    raw = run.raw_data
                    raw["dataset_repository"] = repository_key
                    raw["dataset_commit_hash"] = sha
                    raw["original_owner_repo"] = original_owner_repo
                    raw["resolved_owner_repo"] = resolved_owner_repo
                    results.append(raw)
                    run_count += 1

                print(f"→ {run_count} run trovate")

            except Exception as e:
                print(f"→ ERRORE: {e}")
                errors.append({
                    "repository":          repository_key,
                    "original_owner_repo": original_owner_repo,
                    "resolved_owner_repo": resolved_owner_repo,
                    "commit_hash":         sha,
                    "error":               str(e),
                })

    except Exception as e:
        print(f"  [ERRORE REPO] {e}")
        errors.append({
            "repository":          repository_key,
            "original_owner_repo": original_owner_repo,
            "resolved_owner_repo": resolved_owner_repo,
            "commit_hash":         None,
            "error":               str(e),
        })

print(f"\n[DONE] Workflow runs raccolte: {len(results)}")
print(f"[DONE] Errori:                 {len(errors)}")

# ── Salvataggio risultati ────────────────────────────────────────────────────
out_runs = f"{OUTPUT_DIR}\\workflow_runs_full_data.json"
out_errors = f"{OUTPUT_DIR}\\workflow_runs_errors.json"

print(f"\n[INFO] Salvataggio risultati in: {out_runs}")
with open(out_runs, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print(f"[INFO] Salvataggio errori in:    {out_errors}")
with open(out_errors, "w", encoding="utf-8") as f:
    json.dump(errors, f, indent=2, ensure_ascii=False)

print("[INFO] Completato.")
