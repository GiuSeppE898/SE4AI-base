"""
Questa funzione serve a fare un "educated" guess su un file yaml per capire se è di test, di lint, di build o di deploy 
"""

import yaml
from typing import Any

CLASSIFICATION_RULES = {
    "lint": [
        # Tool specifici
        "flake8", "pylint", "eslint", "ruff", "black", "mypy",
        "prettier", "stylelint", "super-linter", "tflint", "golangci",
        "rubocop", "checkstyle", "ktlint", "swiftlint", "shellcheck",
        "hadolint", "markdownlint", "yamllint", "isort", "pycodestyle",
        "pydocstyle", "commitlint", "actionlint", "secretlint", "detect-secrets",
        # Comandi run
        "yarn lint", "npm run lint", "npm run-script lint",
        "pnpm lint", "make lint", "run lint",
        # Pre-commit e type checking
        "pre-commit", "check types", "type check",
        # Generato pyi (type stubs)
        "check-generated-pyi", "stubgen",
    ],
    "test": [
        # Framework di test
        "pytest", "jest", "unittest", "vitest", "mocha", "jasmine",
        "cargo test", "go test", "npm test", "yarn test", "pnpm test",
        "rspec", "minitest", "phpunit", "testng",
        # E2E / browser
        "cypress", "playwright", "selenium",
        # Coverage
        "coverage", "codecov", "coveralls",
        # Altri
        "junit", "xunit", "robot framework", "nose2", "hypothesis",
    ],
    "build": [
        # Container
        "docker build", "docker/build-push-action",
        # JS/TS
        "npm run build", "yarn build", "pnpm build", "vite build", "webpack",
        # Artifact upload (segnale di build completata)
        "actions/upload-artifact",
        # Linguaggi compilati
        "gradle", "maven", "mvn", "bazel",
        "cargo build", "go build", "dotnet build",
        "swift build", "gem build",
        # RIMOSSI: "pip install", "poetry install", "make", "cmake", "compile"
        # perché sono setup, non build vera
    ],
    "deploy": [
        # Kubernetes / Helm
        "kubectl", "helm",
        # Cloud providers
        "aws-actions", "google-github-actions", "azure/",
        "gcloud", "heroku", "vercel", "netlify", "render",
        "firebase deploy", "fly deploy",
        # IaC
        "terraform apply", "ansible",
        # Docker push (deploy verso registry)
        "docker push",
        # SSH deploy
        "appleboy/ssh-action", "capistrano",
        # Serverless
        "serverless deploy",
    ],
    "security": [
        # SAST / scanning
        "snyk", "trivy", "codeql", "semgrep", "grype",
        "github/codeql-action", "anchore/scan-action",
        # Dependency audit
        "npm audit", "yarn audit", "safety", "owasp",
        "dependency-review", "actions/dependency-review",
        # Secret scanning
        "gitleaks", "trufflehog",
        # Supply chain
        "ossf/scorecard", "scorecard-action",
    ],
    "eval": [
        # Valutazione modelli / agenti (specifico AI)
        "benchmark", "benchmarks",
        "model eval", "agent eval",
        "evaluate",         # solo se non troppo generico nel tuo dataset
        # Performance / load testing
        "load test", "performance test",
        "locust", "k6", "hyperfine", "ab test",
        # RIMOSSI: "score", "metric", "accuracy" — troppo generici
    ],
    "release": [
        # GitHub release
        "softprops/action-gh-release", "actions/create-release",
        "gh release create", "goreleaser",
        # Versioning automatico
        "semantic-release", "release-please", "bump2version",
        # Publish su registry (azioni specifiche di publish)
        "twine upload",          # PyPI
        "npm publish",           # npm
        "gem push",              # RubyGems
        # RIMOSSI: "release" nudo, "publish" nudo, "changelog" — troppo generici
        # "bump version" — troppo generico
    ],
}


def _extract_text_values(data: Any) -> list[str]:
    """
    Visita ricorsivamente il dict/lista YAML e raccoglie
    tutti i valori stringa in una lista piatta.
    """
    texts = []
    if isinstance(data, dict):
        for k, v in data.items():
            # include anche le chiavi (job id, ecc.)
            texts.append(str(k))
            texts.extend(_extract_text_values(v))
    elif isinstance(data, list):
        for item in data:
            texts.extend(_extract_text_values(item))
    elif isinstance(data, str):
        texts.append(data)
    return texts


def classify_workflow(yaml_data: dict) -> dict:
    """
    Classifica un workflow GitHub Actions dato il contenuto
    già parsato con yaml.safe_load().

    Args:
        yaml_data: dict ottenuto da yaml.safe_load(file)

    Returns:
        dict con:
          - "labels": lista di categorie trovate (multi-label)
          - "scores": dict categoria -> numero di match trovati
          - "matched_keywords": dict categoria -> keyword che hanno fatto match
    """
    all_texts = _extract_text_values(yaml_data)
    full_text = " ".join(all_texts).lower()

    workflow_name = yaml_data.get("name", "").lower()
    name_boost = {
        "lint": ["lint", "linting", "format"],
        "test": ["test", "testing", "spec"],
        "build": ["build", "compile", "package"],
        "deploy": ["deploy", "deployment", "release cd"],
        "security": ["security", "scan", "audit", "codeql"],
        "eval": ["eval", "benchmark", "evaluate"],
        "release": ["release", "publish", "changelog"],
    }

    scores = {}
    matched_keywords = {}

    for category, keywords in CLASSIFICATION_RULES.items():
        matches = [kw for kw in keywords if kw.lower() in full_text]
        name_matches = [w for w in name_boost[category] if w in workflow_name]
        scores[category] = len(matches) + len(name_matches) * 2
        matched_keywords[category] = matches + \
            [f"[name] {w}" for w in name_matches]

    labels = [cat for cat, score in scores.items() if score > 0]
    if not labels:
        labels = ["unknown"]

    return {
        "name": yaml_data.get("name", "Unknown"),
        "labels": labels,
        "scores": scores,
        "matched_keywords": matched_keywords,
    }
