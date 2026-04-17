import os
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests

# link del readme con la lista delle repo
GITHUB_README_URL = "https://raw.githubusercontent.com/e2b-dev/awesome-ai-agents/main/README.md"
MAX_WORKERS = 5


def clone_repository(repo_url, destination_folder):
    if os.path.exists(destination_folder):
        return f"SKIP già presente: {destination_folder}"

    try:
        subprocess.run(["git", "clone", repo_url, destination_folder], check=True)
        return f"OK clonata: {repo_url}"
    except subprocess.CalledProcessError as e:
        return f"ERR clone {repo_url} -> {e}"


def extract_open_source_section(readme_content: str) -> str:
    start_marker = "# Open-source projects"
    end_marker = "# Closed-source projects and companies"

    start = readme_content.find(start_marker)
    if start == -1:
        return ""

    end = readme_content.find(end_marker, start)
    if end == -1:
        end = len(readme_content)

    return readme_content[start:end]


def normalize_github_repo_url(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.netloc.lower() not in {"github.com", "www.github.com"}:
        return None

    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        return None

    owner, repo = parts[0], parts[1]

    # Esclude pagine non-repository
    excluded_first_parts = {"orgs", "features", "marketplace", "topics", "collections", "apps", "settings"}
    if owner in excluded_first_parts:
        return None

    # Normalizza "repo.git"
    repo = repo.replace(".git", "")
    if not repo:
        return None

    return f"https://github.com/{owner}/{repo}.git"


def extract_github_repo_urls(readme_content: str) -> list[str]:
    open_source_section = extract_open_source_section(readme_content)
    if not open_source_section:
        return []

    markdown_links = re.findall(r"\[[^\]]+\]\((https?://[^)\s]+)\)", open_source_section)

    repo_urls = []
    seen = set()

    for link in markdown_links:
        normalized = normalize_github_repo_url(link)
        if normalized and normalized not in seen:
            seen.add(normalized)
            repo_urls.append(normalized)

    return repo_urls


def repo_destination_folder(repo_url: str) -> str:
    parsed = urlparse(repo_url)
    parts = [p for p in parsed.path.split("/") if p]
    owner = parts[0]
    repo = parts[1].replace(".git", "")
    # evita collisioni tra owner diversi con stesso nome repo
    return os.path.join("repositories", f"{owner}__{repo}")


def main():
    os.makedirs("repositories", exist_ok=True)

    response = requests.get(GITHUB_README_URL, timeout=30)
    if response.status_code != 200:
        print(f"Errore durante il download del README: {response.status_code}")
        return

    readme_content = response.text
    repo_urls = extract_github_repo_urls(readme_content)

    print(f"Repository trovate: {len(repo_urls)}")

    jobs = []
    seen_urls = set()
    seen_destinations = set()

    for repo_url in repo_urls:
        if repo_url in seen_urls:
            continue
        destination_folder = repo_destination_folder(repo_url)
        if destination_folder in seen_destinations:
            continue
        seen_urls.add(repo_url)
        seen_destinations.add(destination_folder)
        jobs.append((repo_url, destination_folder))

    print(f"Job unici da clonare: {len(jobs)}")
    print(f"Thread usati: {MAX_WORKERS}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_repo = {
            executor.submit(clone_repository, repo_url, destination_folder): repo_url
            for repo_url, destination_folder in jobs
        }

        for future in as_completed(future_to_repo):
            print(future.result())


if __name__ == "__main__":
    main()