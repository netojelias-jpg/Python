from dotenv import load_dotenv
load_dotenv()
from etl.load_to_postgres import get_engine
import pandas as pd

# Ver resumo de todos os runs
print("\n=== TODOS OS CLUSTER RUNS ===")
df_runs = pd.read_sql("""
    SELECT 
        perfil, 
        parametros->>'n_clusters' as clusters,
        metricas->>'silhouette' as silhouette,
        run_at
    FROM cluster_run 
    ORDER BY run_at DESC
""", get_engine())
print(df_runs.to_string(index=False))

# Ver resumo detalhado do ALTA RENDA PF
print("\n\n=== CLUSTER RESUMO - ALTA RENDA PF ===")
df_resumo = pd.read_sql("""
    SELECT 
        cluster, 
        total_clientes, 
        risco_inicial_medio, 
        cobertura_media, 
        atraso_medio,
        valor_contrato_medio,
        saldo_atual_medio
    FROM cluster_run_resumo 
    WHERE run_id = 'cacb92cc-475b-4773-9d0c-7ebffd45741a' 
    ORDER BY cluster
""", get_engine())
print(df_resumo.to_string(index=False))

# Contar clientes por cluster
print("\n\n=== TOTAL DE CLIENTES POR RUN ===")
df_count = pd.read_sql("""
    SELECT 
        cr.perfil,
        COUNT(*) as total_clientes_run
    FROM cluster_run cr
    JOIN cluster_run_clientes crc ON cr.run_id = crc.run_id
    GROUP BY cr.perfil, cr.run_id
    ORDER BY cr.perfil
""", get_engine())
print(df_count.to_string(index=False))
