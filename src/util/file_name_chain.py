from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Dict, Set, List

import pandas as pd

__all__ = [
    "add_workflow_global_id",
    "add_workflow_global_id_from_csv",
]


@dataclass
class DSU:
    """
    Disjoint Set Union (Union-Find) con:
    - Path compression
    - Union by rank
    """
    parent: Dict[str, str] = field(default_factory=dict)
    rank: Dict[str, int] = field(default_factory=dict)

    def add(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: str) -> str:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


def _token(repo: str, file_hash: str) -> str:
    return f"H|{repo}|{file_hash}"


def add_workflow_global_id(
    df: pd.DataFrame,
    id_col: str = "workflow_global_id",
) -> pd.DataFrame:
    """
    Aggiunge una colonna ID stabile per workflow, resistente a rename/move.

    Logica:
    1) Nodo = (repository, hash)
    2) Arco = previous_file_hash <-> file_hash
    3) Ogni componente connessa = stessa identità storica workflow
    4) ID deterministico = sha1(nodi_ordinati_della_componente)

    Colonne richieste:
    - repository, file_hash, previous_file_hash, file_path, previous_file_path
    """
    required = {"repository", "file_hash", "previous_file_hash", "file_path", "previous_file_path"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Colonne mancanti: {sorted(missing)}")

    out = df.copy()

    # Normalizzazione una sola volta
    norm_cols = ["repository", "file_hash", "previous_file_hash", "file_path", "previous_file_path"]
    for c in norm_cols:
        out[c] = out[c].fillna("").astype(str).str.strip()

    dsu = DSU()

    # 1) Costruzione grafo con Union-Find
    for row in out.itertuples(index=False):
        repo = row.repository
        fh = row.file_hash
        ph = row.previous_file_hash

        t_fh = _token(repo, fh) if fh else ""
        t_ph = _token(repo, ph) if ph else ""

        if t_fh:
            dsu.add(t_fh)
        if t_ph:
            dsu.add(t_ph)
        if t_fh and t_ph:
            dsu.union(t_fh, t_ph)

    # 2) root -> nodi componente
    comp_nodes: Dict[str, Set[str]] = {}
    for node in dsu.parent.keys():
        root = dsu.find(node)
        comp_nodes.setdefault(root, set()).add(node)

    # 3) root -> id deterministico
    root_to_gid: Dict[str, str] = {}
    for root, nodes in comp_nodes.items():
        signature = "||".join(sorted(nodes))
        root_to_gid[root] = "wf_" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:20]

    # 4) Assegnazione ID per riga
    ids: List[str] = []
    for row in out.itertuples(index=False):
        repo = row.repository
        fh = row.file_hash
        ph = row.previous_file_hash

        tok = _token(repo, fh) if fh else (_token(repo, ph) if ph else "")
        if tok:
            ids.append(root_to_gid[dsu.find(tok)])
        else:
            # fallback deterministico solo path-chain
            sig = f"P|{repo}|{row.previous_file_path}->{row.file_path}"
            ids.append("wf_" + hashlib.sha1(sig.encode("utf-8")).hexdigest()[:20])

    out[id_col] = ids
    return out


def add_workflow_global_id_from_csv(
    input_csv_path: str,
    output_csv_path: str,
    id_col: str = "workflow_global_id",
) -> pd.DataFrame:
    """
    Carica CSV, aggiunge workflow_global_id, salva CSV, ritorna DataFrame.
    """
    df = pd.read_csv(input_csv_path)
    out = add_workflow_global_id(df, id_col=id_col)
    out.to_csv(output_csv_path, index=False)
    return out