from pathlib import Path
import os
import random

import pandas as pd

from util.file_name_chain import add_workflow_global_id_from_csv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPO_FOLDER = PROJECT_ROOT / "non_agentic_repositories"
GIGAWORK_OUTPUT_DIRECTORY = PROJECT_ROOT / "gigawork_non_agentic_results"

# imposto il seed di random per le analisi
random.seed(69)

# mappo i tipi di aggiunta in human readable
# A for added, M for modified, D for deleted
CHANGE_TYPE_MAP = {
    "A": "Aggiunto",
    "M": "Modificato",
    "D": "Eliminato",
    "R": "Rinominato",
}


def main() -> None:
    # Esplorazione metriche
    # carico tutti i file yaml e li associo al nome della repo
    repo_names = sorted(
        [
            name
            for name in os.listdir(REPO_FOLDER)
            if os.path.isdir(os.path.join(REPO_FOLDER, name))
        ]
    )

    print(sorted([f"{x.split('__')[1]}" for x in repo_names]))
    print(len(repo_names))

    # GigaWork
    # gigawork nel suo output.csv ha le seguenti colonne:
    # * repository
    # * commit_hash
    # * author_name
    # * author_email
    # * committer_name
    # * committer_email
    # * committed_date
    # * authored_date
    # * file_path
    # * previous_file_path
    # * file_hash
    # * previous_file_hash
    # * git_change_type
    # * valid_yaml
    # * probably_workflow
    # * valid_workflow
    #
    # una volta eseguito il run proviamo ad esplorare il dataset che ha cacciato fuori

    # controllo quali workflow non sono vuoti e pulisco un po' la variabile repo_names
    empty_workflows = []

    for repo in repo_names:
        repo_dir = os.path.join(
            GIGAWORK_OUTPUT_DIRECTORY, "all_workflows", repo)
        if os.path.isdir(repo_dir):
            files = os.listdir(repo_dir)
            if not files:
                empty_workflows.append(repo)
                repo_names.remove(repo)

    print(
        f"In queste repositories non sono stati trovati workflow: {empty_workflows}")
    print(f"Numero di repository attive: {len(repo_names)}")

    # carico il dataset creato da gigawork
    dataset_path = GIGAWORK_OUTPUT_DIRECTORY / "dataset.csv"
    df = pd.read_csv(dataset_path)

    print(df.head())

    # per sicurezza controllo se ci sono repository nel dataset che non sono in repo_names
    # (dovrebbero essere tutte in repo_names)
    dataset_repos = set(df["repository"].unique())
    if not dataset_repos.issubset(set(repo_names)):
        print("c'e stato un errore nell'eliminazione delle repo")

    # ordino repo_names in ordine alfabetico per evitare che random si comporti in modo strano
    repo_names = sorted(repo_names)

    # quanti file yaml sono broken?
    print(df["valid_yaml"].value_counts())

    # come prima analisi controllo quanti workflow UNIVOCI ci sono per repository
    # baso l'univocita sulla colonna file_path
    workflow_counts = df.groupby("repository")["file_path"].nunique()
    print(workflow_counts, end="\n\n")

    # media per repository
    average_workflows = workflow_counts.mean()
    print(
        f"Numero medio di workflow univoci per repository: {average_workflows:.2f}")

    # la repository con piu workflow univoci
    repo_piu_workflows = workflow_counts.sort_values(ascending=False).head(1)
    print(
        "Repository con piu workflow univoci:",
        repo_piu_workflows.index[0],
        "con",
        repo_piu_workflows.values[0],
        "workflow univoci",
    )

    # prendo una repository a caso e la analizzo
    # random_repo = random.choice(repo_names)
    # prendo una repo rappresentativa
    random_repo = "joaomdmoura__crewai"

    print(f"Repository analizzata: {random_repo.split('__')[1]}")

    # quali sono i workflow univoci di questa repository?
    # e quali operazioni sono stati fatti su di esso e in quale numero?
    random_repo_workflows = df[df["repository"] == random_repo].copy()

    file_counts = (
        random_repo_workflows.groupby("file_path")
        .size()
        .rename("nr di volte")
    )

    repeated_files = file_counts[file_counts > 1].index

    if len(repeated_files) == 0:
        print("Nessun file_path ripetuto in questa repository.")
    else:
        pivot_changes = (
            random_repo_workflows[random_repo_workflows["file_path"].isin(
                repeated_files)]
            .pivot_table(
                index="file_path",
                columns="git_change_type",
                aggfunc="size",
                fill_value=0,
            )
            .reset_index()
        )

        pivot_changes = pivot_changes.rename(
            columns={c: CHANGE_TYPE_MAP.get(c, c)
                     for c in pivot_changes.columns},
        )

        pivot_changes["nr di volte"] = pivot_changes["file_path"].map(
            file_counts)
        pivot_changes = pivot_changes.sort_values(
            "nr di volte", ascending=False)

        print("Breakdown modifiche per file_path ripetuti (pivot):")
        print(pivot_changes.to_string(index=False))

    # ma ha senso usare file_path come "chiave primaria" per identificare un workflow nel tempo?
    # potrebbe essere che i workflow cambiano nome o vengono rinominati o cancellati spesso, controllo.
    # TODO: CONTROLLARE SE CI SONO PARECCHI PREVIOUS_FILE_PATH DIVERSI DA FILE_PATH,
    # se sono un nr significativo allora fare un algo per risalire alla catena anche quando cambiano nome
    # magari usando i filehash e prev file hash, poi dargli un identificativo univoco a livello globale

    _temp_df = add_workflow_global_id_from_csv(
        input_csv_path=GIGAWORK_OUTPUT_DIRECTORY / "dataset.csv",
        output_csv_path=GIGAWORK_OUTPUT_DIRECTORY / "dataset_with_ids.csv",
    )

    # conto adesso qual'e la media di workflow univoci per repository usando il nuovo dataset
    workflow_counts = _temp_df.groupby(
        "repository")["workflow_global_id"].nunique()
    print(workflow_counts, end="\n\n")

    # media per repository
    average_workflows = workflow_counts.mean()
    print(
        f"Numero medio di workflow univoci per repository: {average_workflows:.2f}")

    # la repository con piu workflow univoci
    repo_piu_workflows = workflow_counts.sort_values(ascending=False).head(1)
    print(
        "Repository con piu workflow univoci:",
        repo_piu_workflows.index[0],
        "con",
        repo_piu_workflows.values[0],
        "workflow univoci",
    )

    # dato che i dati adesso sono piu puliti, prendo una repository a caso e la analizzo di nuovo
    df = _temp_df.copy()  # copio il nuovo dataset nella variabile df per usare sempre lui

    # prendo una repository a caso e la analizzo
    # random_repo = random.choice(repo_names)
    # prendo una repo rappresentativa
    random_repo = "joaomdmoura__crewai"

    def print_repo_summary(repo_name: str) -> None:
        """
        Stampa una tabella per una singola repository con:
        - workflow_global_id
        - file_path (lista dei path osservati per quell'ID)
        - conteggio per tipo modifica (colonne: Aggiunto, Modificato, Eliminato, Rinominato)

        Requisiti:
        - usa CHANGE_TYPE_MAP per mappare i git_change_type
        - analizza solo la repository passata in input
        """
        repo_df = df[df["repository"] == repo_name].copy()

        if repo_df.empty:
            print(f"Nessun dato trovato per repository: {repo_name}")
            return

        # mapping human-readable
        repo_df["git_change_type_mapped"] = (
            repo_df["git_change_type"].map(
                CHANGE_TYPE_MAP).fillna(repo_df["git_change_type"])
        )

        # lista path per workflow_global_id
        paths_df = (
            repo_df.groupby("workflow_global_id", as_index=False)
            .agg(
                file_path=(
                    "file_path",
                    lambda s: sorted(
                        set(x for x in s.dropna() if str(x).strip())),
                )
            )
        )

        # conteggi per tipo modifica per workflow_global_id
        counts_df = (
            repo_df.groupby(["workflow_global_id", "git_change_type_mapped"])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )

        # ordine colonne preferito
        preferred_order = ["Aggiunto", "Modificato", "Eliminato", "Rinominato"]
        existing_preferred = [
            c for c in preferred_order if c in counts_df.columns]
        other_cols = [
            c
            for c in counts_df.columns
            if c not in (["workflow_global_id"] + existing_preferred)
        ]

        counts_df = counts_df[["workflow_global_id"] +
                              existing_preferred + other_cols]

        # merge finale
        summary = paths_df.merge(
            counts_df, on="workflow_global_id", how="left").fillna(0)

        print(
            f"Repository analizzata: {repo_name.split('__')[1] if '__' in repo_name else repo_name}"
        )
        print(summary.to_string(index=False))

    print_repo_summary(random_repo)

    # conto quante linee sono valid_workflow, quanti sono probably_workflow e quanti valid_yaml
    valid_workflow_tr = df["valid_workflow"].where(
        df["valid_workflow"] == True).count()
    print(
        "Linee con valid_workflow = True:",
        valid_workflow_tr,
        "False =",
        len(df) - valid_workflow_tr,
        "Totale:",
        len(df),
    )

    probably_workflow = df["probably_workflow"].where(
        df["probably_workflow"] == True).count()
    print(
        "Linee con probably_workflow = True:",
        probably_workflow,
        "False =",
        len(df) - probably_workflow,
        "Totale:",
        len(df),
    )

    valid_yaml = df["valid_yaml"].where(df["valid_yaml"] == True).count()
    print(
        "Linee con valid_yaml = True:",
        valid_yaml,
        "False =",
        len(df) - valid_yaml,
        "Totale:",
        len(df),
    )

    # ogni singolo file con id univoco conto quante volte e stato valid_workflow,
    # probably_workflow e valid_yaml
    invalid_workflow_per_id = df["valid_workflow"].eq(
        False).groupby(df["workflow_global_id"]).sum()
    probably_workflow_per_id = (
        df["probably_workflow"].eq(True).groupby(
            df["workflow_global_id"]).sum()
    )
    valid_yaml_per_id = df["valid_yaml"].eq(
        True).groupby(df["workflow_global_id"]).sum()

    print("media not_valid_workflow per workflow_global_id:",
          invalid_workflow_per_id.mean())
    print("media probably_workflow per workflow_global_id:",
          probably_workflow_per_id.mean())
    print("media valid_yaml per workflow_global_id:", valid_yaml_per_id.mean())

    # valid_workflow e una dipendenza funzionale di valid_yaml?
    # valid_yaml = True -> valid_workflow = True sempre ?
    matrice_2x2 = (
        pd.crosstab(df["valid_workflow"], df["valid_yaml"], dropna=False)
        .reindex(index=[False, True], columns=[False, True], fill_value=0)
    )

    matrice_2x2.index.name = "valid_workflow"
    matrice_2x2.columns.name = "valid_yaml"

    print(matrice_2x2)

    # df valid_yaml -> valid_workflow
    # possiamo dedurre che non sempre un file yaml valido indica un workflow valido,
    # da tenere conto per l'analisi approfondita.


if __name__ == "__main__":
    main()
