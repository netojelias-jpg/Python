import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from etl.load_to_postgres import get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


def get_all_runs() -> pd.DataFrame:
    """Obtém todos os runs de clusterização ordenados por data."""
    engine = get_engine()
    query = """
        SELECT 
            run_id,
            perfil,
            algoritmo,
            parametros->>'n_clusters' as n_clusters,
            parametros->>'n_components' as n_components,
            metricas->>'silhouette' as silhouette,
            metricas->>'search_silhouette' as search_silhouette,
            run_at
        FROM cluster_run
        ORDER BY perfil, run_at DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # Converter tipos
    df['n_clusters'] = pd.to_numeric(df['n_clusters'], errors='coerce').fillna(0).astype(int)
    df['n_components'] = pd.to_numeric(df['n_components'], errors='coerce').fillna(0).astype(int)
    df['silhouette'] = pd.to_numeric(df['silhouette'], errors='coerce').fillna(0.0)
    df['search_silhouette'] = pd.to_numeric(df['search_silhouette'], errors='coerce').fillna(0.0)
    
    return df


def get_cluster_summary(run_id: str) -> pd.DataFrame:
    """Obtém resumo de todos os clusters de um run específico."""
    engine = get_engine()
    query = """
        SELECT 
            cluster,
            total_clientes,
            risco_inicial_medio,
            cobertura_media,
            atraso_medio,
            valor_contrato_medio,
            saldo_atual_medio
        FROM cluster_run_resumo
        WHERE run_id = :run_id
        ORDER BY cluster
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"run_id": run_id})
    return df


def get_cluster_clients(run_id: str, cluster_id: int) -> pd.DataFrame:
    """Obtém todos os clientes de um cluster específico."""
    engine = get_engine()
    query = """
        SELECT 
            cliente_id,
            cliente_perfil,
            agencia_nome,
            carteira_nome,
            linha,
            cluster,
            risco_inicial,
            risco_inicial_score,
            factors
        FROM cluster_run_clientes
        WHERE run_id = :run_id AND cluster = :cluster_id
        ORDER BY cliente_id
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"run_id": run_id, "cluster_id": cluster_id})
    return df


def explain_cluster(summary_row: Dict) -> str:
    """Gera uma explicação textual das características do cluster."""
    explanations = []
    
    # Análise de risco
    risco = summary_row['risco_inicial_medio']
    if risco <= 2:
        explanations.append("**Risco Baixo** (rating médio AA-B)")
    elif risco <= 4:
        explanations.append("**Risco Moderado** (rating médio BB-C)")
    elif risco <= 7:
        explanations.append("**Risco Elevado** (rating médio CC-D)")
    else:
        explanations.append("**Risco Muito Alto** (rating médio E-G)")
    
    # Análise de cobertura
    cobertura = summary_row['cobertura_media']
    if cobertura >= 150:
        explanations.append("Excelente cobertura de garantias")
    elif cobertura >= 100:
        explanations.append("Cobertura adequada de garantias")
    else:
        explanations.append("Cobertura insuficiente de garantias")
    
    # Análise de atraso
    atraso = summary_row['atraso_medio']
    if atraso == 0:
        explanations.append("Sem atrasos (adimplente)")
    elif atraso <= 30:
        explanations.append("Atrasos leves (até 30 dias)")
    elif atraso <= 90:
        explanations.append("Atrasos moderados (30-90 dias)")
    else:
        explanations.append("Atrasos graves (>90 dias)")
    
    # Análise de ticket
    ticket = summary_row['valor_contrato_medio']
    if ticket >= 100000:
        explanations.append("Contratos de alto valor")
    elif ticket >= 30000:
        explanations.append("Contratos de médio valor")
    else:
        explanations.append("Contratos de baixo valor")
    
    return " • ".join(explanations)


def get_cluster_characteristics(run_id: str, cluster_id: int) -> Dict:
    """Analisa as características principais do cluster através dos fatores."""
    engine = get_engine()
    query = """
        SELECT factors
        FROM cluster_run_clientes
        WHERE run_id = :run_id AND cluster = :cluster_id
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"run_id": run_id, "cluster_id": cluster_id})
    
    if df.empty:
        return {}
    
    # Extrair médias dos fatores
    all_factors = []
    for factors_json in df['factors']:
        if factors_json and isinstance(factors_json, dict):
            all_factors.append(factors_json)
    
    if not all_factors:
        return {}
    
    # Calcular médias dos fatores
    factor_means = {}
    for factor_dict in all_factors:
        for key, value in factor_dict.items():
            if key not in factor_means:
                factor_means[key] = []
            factor_means[key].append(value)
    
    result = {k: round(sum(v) / len(v), 3) for k, v in factor_means.items()}
    return result


@app.route('/')
def index():
    """Página principal com lista de perfis e runs."""
    try:
        runs_df = get_all_runs()
        
        # Agrupar por perfil, pegando o run mais recente
        latest_runs = runs_df.groupby('perfil').first().reset_index()
        latest_runs = latest_runs.sort_values('silhouette', ascending=False)
        
        perfis_data = []
        for _, row in latest_runs.iterrows():
            perfis_data.append({
                'perfil': row['perfil'],
                'run_id': row['run_id'],
                'n_clusters': int(row['n_clusters']),
                'silhouette': float(row['silhouette']),
                'run_at': row['run_at'].strftime('%d/%m/%Y %H:%M') if pd.notna(row['run_at']) else 'N/A'
            })
        
        return render_template('clusters_index.html', perfis=perfis_data)
    except Exception as e:
        logger.error(f"Erro ao carregar página principal: {e}", exc_info=True)
        return f"<h1>Erro ao carregar dados</h1><p>{str(e)}</p>", 500


@app.route('/perfil/<run_id>')
def perfil_detail(run_id: str):
    """Página de detalhes de um perfil com seus clusters."""
    try:
        # Buscar informações do run
        runs_df = get_all_runs()
        run_info = runs_df[runs_df['run_id'] == run_id]
        
        if run_info.empty:
            return "<h1>Run não encontrado</h1>", 404
        
        run_info = run_info.iloc[0]
        
        # Buscar resumo dos clusters
        summary_df = get_cluster_summary(run_id)
        
        clusters_data = []
        for _, row in summary_df.iterrows():
            cluster_data = {
                'cluster_id': int(row['cluster']),
                'total_clientes': int(row['total_clientes']),
                'risco_inicial_medio': round(float(row['risco_inicial_medio']), 2),
                'cobertura_media': round(float(row['cobertura_media']), 2),
                'atraso_medio': round(float(row['atraso_medio']), 2),
                'valor_contrato_medio': round(float(row['valor_contrato_medio']), 2),
                'saldo_atual_medio': round(float(row['saldo_atual_medio']), 2),
                'explicacao': explain_cluster(row.to_dict())
            }
            
            # Adicionar características dos fatores
            factors = get_cluster_characteristics(run_id, int(row['cluster']))
            cluster_data['factors'] = factors
            
            clusters_data.append(cluster_data)
        
        perfil_data = {
            'perfil': run_info['perfil'],
            'run_id': run_id,
            'n_clusters': int(run_info['n_clusters']),
            'n_components': int(run_info['n_components']),
            'silhouette': round(float(run_info['silhouette']), 4),
            'run_at': run_info['run_at'].strftime('%d/%m/%Y %H:%M:%S') if pd.notna(run_info['run_at']) else 'N/A'
        }
        
        return render_template('perfil_detail.html', perfil=perfil_data, clusters=clusters_data)
    except Exception as e:
        logger.error(f"Erro ao carregar detalhes do perfil: {e}", exc_info=True)
        return f"<h1>Erro ao carregar dados</h1><p>{str(e)}</p>", 500


@app.route('/cluster/<run_id>/<int:cluster_id>')
def cluster_clients(run_id: str, cluster_id: int):
    """Página com lista de clientes de um cluster específico."""
    try:
        # Buscar informações do run
        runs_df = get_all_runs()
        run_info = runs_df[runs_df['run_id'] == run_id]
        
        if run_info.empty:
            return "<h1>Run não encontrado</h1>", 404
        
        run_info = run_info.iloc[0]
        
        # Buscar resumo do cluster
        summary_df = get_cluster_summary(run_id)
        cluster_summary = summary_df[summary_df['cluster'] == cluster_id]
        
        if cluster_summary.empty:
            return "<h1>Cluster não encontrado</h1>", 404
        
        cluster_summary = cluster_summary.iloc[0]
        
        # Buscar clientes do cluster
        clients_df = get_cluster_clients(run_id, cluster_id)
        
        clients_data = []
        for _, row in clients_df.iterrows():
            factors = row['factors'] if isinstance(row['factors'], dict) else {}
            clients_data.append({
                'cliente_id': row['cliente_id'],
                'agencia_nome': row['agencia_nome'],
                'carteira_nome': row['carteira_nome'],
                'linha': row['linha'],
                'risco_inicial': row['risco_inicial'],
                'risco_inicial_score': int(row['risco_inicial_score']) if pd.notna(row['risco_inicial_score']) else 'N/A',
                'factors': factors
            })
        
        cluster_data = {
            'cluster_id': cluster_id,
            'perfil': run_info['perfil'],
            'run_id': run_id,
            'total_clientes': int(cluster_summary['total_clientes']),
            'risco_inicial_medio': round(float(cluster_summary['risco_inicial_medio']), 2),
            'cobertura_media': round(float(cluster_summary['cobertura_media']), 2),
            'atraso_medio': round(float(cluster_summary['atraso_medio']), 2),
            'valor_contrato_medio': round(float(cluster_summary['valor_contrato_medio']), 2),
            'saldo_atual_medio': round(float(cluster_summary['saldo_atual_medio']), 2),
            'explicacao': explain_cluster(cluster_summary.to_dict())
        }
        
        return render_template('cluster_clients.html', cluster=cluster_data, clients=clients_data)
    except Exception as e:
        logger.error(f"Erro ao carregar clientes do cluster: {e}", exc_info=True)
        return f"<h1>Erro ao carregar dados</h1><p>{str(e)}</p>", 500


@app.route('/api/runs')
def api_runs():
    """API endpoint para obter todos os runs."""
    try:
        runs_df = get_all_runs()
        return jsonify(runs_df.to_dict(orient='records'))
    except Exception as e:
        logger.error(f"Erro na API de runs: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/cluster_summary/<run_id>')
def api_cluster_summary(run_id: str):
    """API endpoint para obter resumo dos clusters de um run."""
    try:
        summary_df = get_cluster_summary(run_id)
        return jsonify(summary_df.to_dict(orient='records'))
    except Exception as e:
        logger.error(f"Erro na API de cluster summary: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
