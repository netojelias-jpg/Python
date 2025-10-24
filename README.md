
Projeto: Dashboard "Análise de Carteira" e pipeline para análise de carteira de clientes

Arquivos criados nesta etapa:
- sql/create_table_clientes_carteira.sql -> script CREATE TABLE para Postgres
- sql/create_table_categorias.sql -> script CREATE TABLE para categoria
- sql/create_table_cluster_run.sql -> tabelas para versionamento das execuções de clustering
- etl/load_to_postgres.py -> script Python para carregar .xlsx e fazer upsert na tabela
- etl/load_categorias.py -> ETL para categorias
- etl/requirements.txt -> dependências Python
- scripts/run_sql_migrations.py -> executor sequencial dos scripts SQL em `sql/`

- Visão geral - profissões de alta cardinalidade

Algumas colunas do seu .xlsx (por ex. `profissao`) têm cardinalidade alta (muitas profissões diferentes — até 500). Para melhorar performance do banco e simplificar análises, o ETL cria uma dimensão chamada `profissao_dim` com os nomes únicos de profissões e vincula cada registro em `clientes_carteira` pela coluna `profissao_id`.

Vantagens:
- reduz armazenamento repetido de strings longas;
- facilita agregações e filtros por profissão (usando integer ids);
- mantém flexibilidade para armazenar nomes originais em `profissao_dim`.

Fluxo resumido de importação (ETL):
1. Importa .xlsx em pandas e normaliza colunas.
2. Extrai nomes de profissões únicos e escreve em `profissao_dim` (INSERT ... ON CONFLICT DO NOTHING).
3. Busca ids correspondentes e cria coluna `profissao_id` no DataFrame.
4. Upsert na tabela `clientes_carteira` (INSERT ... ON CONFLICT DO UPDATE).

Nome do dashboard: Análise de Carteira

Pré-requisitos
- PostgreSQL acessível
- Python 3.8+
- Variáveis de ambiente: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

Configurar variáveis PG
-----------------------
Crie um arquivo `.env` na raiz do projeto (baseado em `.env.example`) ou exporte as variáveis no PowerShell antes de rodar os scripts:

```powershell
Copy-Item .env.example .env
# edite o arquivo .env com seu host, database, usuário e senha
```

Instalação (PowerShell)
```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r .\etl\requirements.txt
```

Criar tabela no Postgres
```powershell
# a partir do psql ou cliente SQL execute o conteúdo de sql/create_table_clientes_carteira.sql
psql -h $env:PGHOST -p $env:PGPORT -U $env:PGUSER -d $env:PGDATABASE -f .\sql\create_table_clientes_carteira.sql
```

Aplicar todos os scripts SQL de uma vez
```powershell
python .\scripts\run_sql_migrations.py
```

Rodar ETL (PowerShell)
As variáveis podem ser carregadas do `.env` automaticamente; se preferir exportar manualmente use `set` conforme o exemplo abaixo.

```powershell
$env:PGHOST="localhost"; $env:PGPORT="5432"; $env:PGDATABASE="meudb"; $env:PGUSER="meuuser"; $env:PGPASSWORD="minhasenha"
python .\etl\load_to_postgres.py "C:\caminho\para\seu_arquivo.xlsx"
```

Visualizar últimas operações no navegador
-----------------------------------------
```powershell
# variáveis lidas do .env automaticamente; exporte apenas se quiser sobrescrever
python .\web\app.py
```

Em seguida acesse http://127.0.0.1:5000/ e você verá uma tabela com `cliente_id`, `cliente_nome` e `data_operacao` ordenada pelas operações mais recentes (LIMIT 10). O app usa Flask e SQLAlchemy, portanto reutiliza as mesmas variáveis de ambiente do ETL.

Segmentação por perfil (Fatores + Clusters)
-------------------------------------------
```powershell
# usa as credenciais do .env automaticamente
python .\analysis\segmentation.py
```

O script gera, para cada `cliente_perfil`, uma análise fatorial seguida de clustering (k-means com escolha automática de `k`) usando principalmente `risco_inicial`, `cob_garantia`, `mod_bacen` e `atraso`, além de `valor_contrato` e `saldo_atual` como contexto financeiro. Saídas:
- `analysis/output/perfil_<perfil>_clusters.csv`: atribuição de cluster e scores fatoriais por cliente.
- `analysis/output/perfil_<perfil>_cluster_summary.csv`: métricas agregadas por cluster.
- Visões cruzadas (`clusters_por_agencia.csv`, `clusters_por_carteira.csv`, `clusters_por_linha.csv`).

Cada execução também é registrada no Postgres:
- `cluster_run`: metadados da rodada (perfil, parâmetros, métricas de silhouette).
- `cluster_run_clientes`: cluster corrente por cliente, incluindo scores fatoriais em JSON.
- `cluster_run_resumo`: indicadores agregados por cluster.

Use esses snapshots para acompanhar a evolução histórica dos clusters e alimentar relatórios.

Ajuste os limiares, número de componentes e features diretamente em `analysis/segmentation.py` conforme novas necessidades.

Importar categorias (PowerShell)
```powershell
# garante a existência da tabela categoria
psql -h $env:PGHOST -p $env:PGPORT -U $env:PGUSER -d $env:PGDATABASE -f .\sql\create_table_categorias.sql

# carrega o arquivo categorias.xlsx
python .\etl\load_categorias.py "C:\caminho\para\categorias.xlsx" --truncate
```

Próximos passos sugeridos
- Testar a ingestão com um subconjunto do .xlsx
- Ajustar tipos/constraints no SQL conforme sua realidade (por ex: marcar `cliente_documento` UNIQUE)
- Implementar logs mais robustos e testes unitários para o ETL
- Criar notebook de exploração e pipeline de features para o algoritmo de risco
- Implementar dashboard com Streamlit/Metabase/Superset conectado ao Postgres

Importante: NÃO criar outro banco de dados
---------------------------------------
Você instruiu para não criar outro banco de dados. Todas as instruções acima assumem que você já tem um banco Postgres existente e configurado (use as variáveis de ambiente PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD).

Se houver scripts ou exemplos para criar databases em tutoriais anteriores, IGNORE-OS ou não os execute. Use apenas os scripts SQL que alteram esquemas (por exemplo `sql/create_table_clientes_carteira.sql`) dentro do banco existente.

Se quiser, eu já preparo:
- notebook de feature engineering + treino de modelo (ex: XGBoost)
- protótipo de dashboard Streamlit com filtros e gráficos
- scripts de teste e CI
