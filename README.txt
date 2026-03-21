CI/CD workflow extraction for awesome-ai-agents

What this project does
- Reads repository links from awesome-ai-agents/README.md.
- Clones each repository with sparse checkout.
- Extracts only .github/workflows/*.yml and .yaml files.
- Produces output datasets for downstream analysis.

How to run
1) Ensure git and python3 are installed.
2) Run: python3 extract_github_workflows.py

Generated artifacts
- output/repos_from_awesome_ai_agents.csv
- output/workflows_extraction_report.csv
- output/extraction_summary.json
- output/workflows/<owner__repo>/*.yml|*.yaml
