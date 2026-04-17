import os
import subprocess
import pandas as pd

REPO_FOLDER = "/Users/vito/Desktop/SE4AI-base/repositories"
BASE_OUTPUT_DIR = "gigawork"
ALL_WORKFLOWS_DIR = os.path.join(BASE_OUTPUT_DIR, "all_workflows")

os.makedirs(ALL_WORKFLOWS_DIR, exist_ok=True)

repo_list = sorted(
    [name for name in os.listdir(REPO_FOLDER) if os.path.isdir(os.path.join(REPO_FOLDER, name))]
)

all_data = []

for repo in repo_list:
    full_repo_path = os.path.join(REPO_FOLDER, repo)
    repo_name = os.path.basename(repo)
    
    repo_specific_dir = os.path.join(ALL_WORKFLOWS_DIR, repo_name)
    os.makedirs(repo_specific_dir, exist_ok=True)
    
    output_single = os.path.join(repo_specific_dir, f"{repo_name}.csv")
    
    try:
        subprocess.run([
            "gigawork", full_repo_path,
            "-n", repo_name,
            "-o", output_single,
            "-w", repo_specific_dir
        ], check=True, capture_output=True)
        
        if os.path.exists(output_single) and os.path.getsize(output_single) > 0:
            all_data.append(pd.read_csv(output_single))
            
    except Exception as e:
        print(f"Errore su {repo_name}: {e}")

if all_data:
    master_df = pd.concat(all_data, ignore_index=True)
    master_df.to_csv(os.path.join(BASE_OUTPUT_DIR, "dataset.csv"), index=False)