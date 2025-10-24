import logging
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import FactorAnalysis
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from sqlalchemy import MetaData, Table
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from etl.load_to_postgres import get_engine  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TARGET_COLUMNS = [
    "cliente_id",
    "cliente_nome",
    "cliente_perfil",
    "agencia_nome",
    "carteira_nome",
    "linha",
    "risco_inicial",
    "cob_garantia",
    "mod_bacen",
    "atraso",
    "valor_contrato",
    "saldo_atual",
]

RISK_ORDER = [
    "AA",
    "A",
    "B",
    "BB",
    "C",
    "CC",
    "D",
    "DD",
    "E",
    "EE",
    "F",
    "G",
]
RISK_MAP: Dict[str, int] = {name: idx for idx, name in enumerate(RISK_ORDER)}

OUTPUT_DIR = ROOT_DIR / "analysis" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def slugify(value: str) -> str:
    filtered = [c.lower() if c.isalnum() else "_" for c in value]
    slug = "".join(filtered)
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_") or "perfil"


def load_source_dataframe(engine) -> pd.DataFrame:
    query = f"""
        SELECT {', '.join(TARGET_COLUMNS)}
        FROM clientes_carteira
        WHERE cliente_perfil IS NOT NULL
    """
    logger.info("Fetching data from clientes_carteira")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    logger.info("Fetched %d rows", len(df))
    return df


def prepare_segment(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["risco_inicial"] = work["risco_inicial"].str.upper().str.strip()
    work["risco_inicial_score"] = work["risco_inicial"].map(RISK_MAP)
    work["cob_garantia"] = pd.to_numeric(work["cob_garantia"], errors="coerce")
    work["atraso"] = pd.to_numeric(work["atraso"], errors="coerce")
    work["valor_contrato"] = pd.to_numeric(work["valor_contrato"], errors="coerce")
    work["saldo_atual"] = pd.to_numeric(work["saldo_atual"], errors="coerce")
    work["mod_bacen"] = work["mod_bacen"].fillna("DESCONHECIDO").astype(str).str.upper()
    # drop rows missing the core numeric drivers
    work = work.dropna(subset=["risco_inicial_score", "cob_garantia", "atraso"])
    if work.empty:
        return work
    work["risco_inicial_score"] = work["risco_inicial_score"].astype(int)
    work["cob_garantia"] = work["cob_garantia"].astype(float)
    work["atraso"] = work["atraso"].astype(float)
    work["valor_contrato"] = work["valor_contrato"].fillna(work["valor_contrato"].median())
    work["saldo_atual"] = work["saldo_atual"].fillna(work["saldo_atual"].median())
    one_hot = pd.get_dummies(work["mod_bacen"], prefix="mod", dtype=float)
    feature_cols = ["risco_inicial_score", "cob_garantia", "atraso", "valor_contrato", "saldo_atual"]
    # Não duplicar as colunas - apenas adicionar one-hot
    return pd.concat([work.reset_index(drop=True), one_hot.reset_index(drop=True)], axis=1)


def choose_factor_components(feature_frame: pd.DataFrame) -> int:
    n_features = feature_frame.shape[1]
    n_samples = feature_frame.shape[0]
    max_components = max(1, min(5, n_features, n_samples - 1))
    return min(3, max_components)


def run_factor_analysis(feature_frame: pd.DataFrame) -> Dict[str, np.ndarray]:
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(feature_frame)
    n_components = choose_factor_components(feature_frame)
    fa = FactorAnalysis(n_components=n_components, random_state=42)
    factors = fa.fit_transform(X_scaled)
    loadings = fa.components_
    return {
        "scaled": X_scaled,
        "factors": factors,
        "loadings": loadings,
        "scaler": scaler,
        "n_components": n_components,
    }


def pick_cluster_count(factors: np.ndarray) -> tuple[int, float]:
    max_k = min(6, factors.shape[0] - 1)
    if max_k < 2:
        return 1, 0.0
    best_k = 1
    best_score = 0.0
    for k in range(2, max_k + 1):
        km = KMeans(n_clusters=k, n_init=20, random_state=42)
        labels = km.fit_predict(factors)
        if len(set(labels)) < 2:
            continue
        score = silhouette_score(factors, labels)
        if score > best_score:
            best_score = score
            best_k = k
    return best_k, best_score


def cluster_factors(factors: np.ndarray, n_clusters: int) -> Dict[str, Any]:
    if n_clusters <= 1:
        labels = np.zeros(factors.shape[0], dtype=int)
        centers = factors.mean(axis=0, keepdims=True)
        return {"labels": labels, "centers": centers, "silhouette": 0.0}
    km = KMeans(n_clusters=n_clusters, n_init=50, random_state=42)
    labels = km.fit_predict(factors)
    silhouette = 0.0
    if len(set(labels)) > 1:
        silhouette = float(silhouette_score(factors, labels))
    return {"labels": labels, "centers": km.cluster_centers_, "silhouette": silhouette}


def summarize_clusters(df: pd.DataFrame, labels: np.ndarray) -> pd.DataFrame:
    df = df.copy()
    df["cluster"] = labels
    
    # Abordagem mais robusta: construir dicionário linha por linha
    summary_data = []
    for cluster_id in sorted(df["cluster"].unique()):
        cluster_df = df[df["cluster"] == cluster_id]
        
        # Calcular médias como valores escalares
        summary_data.append({
            "cluster": int(cluster_id),
            "total_clientes": int(cluster_df["cliente_id"].nunique()),
            "risco_inicial_medio": round(cluster_df["risco_inicial_score"].mean(), 2),
            "cobertura_media": round(cluster_df["cob_garantia"].mean(), 2),
            "atraso_medio": round(cluster_df["atraso"].mean(), 2),
            "valor_contrato_medio": round(cluster_df["valor_contrato"].mean(), 2),
            "saldo_atual_medio": round(cluster_df["saldo_atual"].mean(), 2),
        })
    
    summary = pd.DataFrame(summary_data)
    return summary


def persist_cluster_run(
    engine,
    perfil: str,
    prepared: pd.DataFrame,
    summary: pd.DataFrame,
    metrics: Dict[str, Any],
    factor_cols: List[str],
) -> uuid.UUID:
    run_id = uuid.uuid4()
    metadata = MetaData()
    cluster_run_table = Table("cluster_run", metadata, autoload_with=engine)
    cluster_run_clientes_table = Table("cluster_run_clientes", metadata, autoload_with=engine)
    cluster_run_resumo_table = Table("cluster_run_resumo", metadata, autoload_with=engine)

    feature_columns = metrics.get("feature_columns", [])
    parametros_payload = {
        "feature_columns": list(feature_columns),
        "n_components": int(metrics.get("n_components", 0)),
        "n_clusters": int(metrics.get("n_clusters", 0)),
    }
    metricas_payload = {
        "silhouette": float(metrics.get("silhouette", 0.0) or 0.0),
        "search_silhouette": float(metrics.get("search_silhouette", 0.0) or 0.0),
    }

    client_cols = [
        "cliente_id",
        "cliente_perfil",
        "agencia_nome",
        "carteira_nome",
        "linha",
        "cluster",
        "risco_inicial",
        "risco_inicial_score",
    ]
    factor_cols_present = [col for col in factor_cols if col in prepared.columns]
    cliente_records = []
    for row in prepared[client_cols + factor_cols_present].to_dict(orient="records"):
        factor_scores = {
            col: float(row[col])
            for col in factor_cols_present
            if col in row and pd.notna(row[col])
        }
        cliente_records.append(
            {
                "run_id": run_id,
                "cliente_id": row.get("cliente_id"),
                "cliente_perfil": row.get("cliente_perfil"),
                "agencia_nome": row.get("agencia_nome"),
                "carteira_nome": row.get("carteira_nome"),
                "linha": row.get("linha"),
                "cluster": int(row.get("cluster", 0)),
                "risco_inicial": row.get("risco_inicial"),
                "risco_inicial_score": (
                    int(row.get("risco_inicial_score"))
                    if row.get("risco_inicial_score") is not None
                    and not pd.isna(row.get("risco_inicial_score"))
                    else None
                ),
                "factors": factor_scores,
            }
        )

    summary_records = []
    for row in summary.to_dict(orient="records"):
        summary_records.append(
            {
                "run_id": run_id,
                "cluster": int(row["cluster"]),
                "total_clientes": int(row["total_clientes"]),
                "risco_inicial_medio": float(row["risco_inicial_medio"]),
                "cobertura_media": float(row["cobertura_media"]),
                "atraso_medio": float(row["atraso_medio"]),
                "valor_contrato_medio": float(row["valor_contrato_medio"]),
                "saldo_atual_medio": float(row["saldo_atual_medio"]),
            }
        )

    run_row = {
        "run_id": run_id,
        "perfil": perfil,
        "algoritmo": "factor_analysis_kmeans_v1",
        "parametros": parametros_payload,
        "metricas": metricas_payload,
    }

    with engine.begin() as conn:
        conn.execute(cluster_run_table.insert(), run_row)
        if cliente_records:
            conn.execute(cluster_run_clientes_table.insert(), cliente_records)
        if summary_records:
            conn.execute(cluster_run_resumo_table.insert(), summary_records)

    return run_id


def ensure_minimum_samples(df: pd.DataFrame, threshold: int = 25) -> bool:
    return len(df) >= threshold


def build_segment_outputs(
    perfil: str,
    df_segment: pd.DataFrame,
    engine = None,
) -> Optional[pd.DataFrame]:
    if not ensure_minimum_samples(df_segment):
        logger.warning("Skipping perfil %s (only %d registros)", perfil, len(df_segment))
        return None
    prepared = prepare_segment(df_segment)
    if prepared.empty or prepared.shape[0] < 5:
        logger.warning("Perfil %s sem dados suficientes após limpeza", perfil)
        return None
    feature_cols = [
        col
        for col in prepared.columns
        if (col.startswith("mod_") and col != "mod_bacen")
        or col in {
        "risco_inicial_score",
        "cob_garantia",
        "atraso",
        "valor_contrato",
        "saldo_atual",
    }
    ]
    factors_payload = run_factor_analysis(prepared[feature_cols])
    factors = factors_payload["factors"]
    k, search_silhouette = pick_cluster_count(factors)
    cluster_payload = cluster_factors(factors, k)
    labels = cluster_payload["labels"]
    prepared["cluster"] = labels
    for idx in range(factors_payload["n_components"]):
        prepared[f"factor_{idx+1}"] = factors[:, idx]
    perfil_slug = slugify(perfil)
    detailed_path = OUTPUT_DIR / f"perfil_{perfil_slug}_clusters.csv"
    summary_path = OUTPUT_DIR / f"perfil_{perfil_slug}_cluster_summary.csv"
    prepared_output_cols = [
        "cliente_id",
        "cliente_nome",
        "cliente_perfil",
        "agencia_nome",
        "carteira_nome",
        "linha",
        "cluster",
    ] + [f"factor_{idx+1}" for idx in range(factors_payload["n_components"])]
    prepared[prepared_output_cols].to_csv(detailed_path, index=False)
    summary = summarize_clusters(prepared, labels)
    summary.to_csv(summary_path, index=False)
    metrics = {
        "feature_columns": feature_cols,
        "n_components": factors_payload["n_components"],
        "n_clusters": k,
        "silhouette": cluster_payload.get("silhouette", 0.0),
        "search_silhouette": search_silhouette,
    }
    if engine is not None:
        factor_cols = [f"factor_{idx+1}" for idx in range(factors_payload["n_components"])]
        run_id = persist_cluster_run(engine, perfil, prepared, summary, metrics, factor_cols)
        logger.info(
            "Perfil %s -> %d clusters (run_id=%s, silhouette=%.3f)",
            perfil,
            summary.shape[0],
            run_id,
            metrics["silhouette"],
        )
    else:
        logger.info("Perfil %s -> %d clusters (não persistido)", perfil, summary.shape[0])
    return prepared[prepared_output_cols]


def aggregate_views(consolidated: pd.DataFrame, dimension: str, filename: str) -> None:
    view = (
        consolidated.groupby([dimension, "cluster"])
        .agg(total_clientes=("cliente_id", "nunique"))
        .reset_index()
        .sort_values(by="total_clientes", ascending=False)
    )
    view.to_csv(OUTPUT_DIR / filename, index=False)


def main() -> None:
    engine = get_engine()
    df = load_source_dataframe(engine)
    if df.empty:
        logger.error("Tabela clientes_carteira não retornou dados")
        return
    consolidated_rows: List[pd.DataFrame] = []
    for perfil, grupo in df.groupby("cliente_perfil"):
        result = build_segment_outputs(perfil, grupo, engine=engine)
        if result is not None:
            consolidated_rows.append(result)
    if not consolidated_rows:
        logger.warning("Nenhum perfil gerou clusters")
        return
    consolidated = pd.concat(consolidated_rows, ignore_index=True)
    aggregate_views(consolidated, "agencia_nome", "clusters_por_agencia.csv")
    aggregate_views(consolidated, "carteira_nome", "clusters_por_carteira.csv")
    aggregate_views(consolidated, "linha", "clusters_por_linha.csv")
    logger.info("Clusterização concluída. Resultados em %s", OUTPUT_DIR)


if __name__ == "__main__":
    main()
