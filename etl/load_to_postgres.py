r"""
ETL simples: leitura de um arquivo .xlsx e upsert para a tabela `clientes_carteira` no PostgreSQL.
Requisitos: pandas, sqlalchemy, psycopg2-binary, openpyxl

Uso (PowerShell):
$env:PGHOST="localhost"; $env:PGPORT="5432"; $env:PGDATABASE="meudb"; $env:PGUSER="meuuser"; $env:PGPASSWORD="minhasenha"
python .\etl\load_to_postgres.py "C:\caminho\para\arquivo.xlsx"
# também suporta CSV: python .\etl\load_to_postgres.py "C:\caminho\para\arquivo.csv"

O script faz:
- normalização dos nomes de colunas
- conversão de tipos (datas, numéricos, booleanos)
- limpeza básica (cpf/cnpj apenas dígitos)
- upsert em batch usando INSERT ... ON CONFLICT
"""
import sys
import os
import logging
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, text, bindparam
from sqlalchemy.dialects.postgresql import insert

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Mapeamento de colunas esperadas (seu .xlsx usou estes nomes)
COLUMN_MAP = {
    'cliente_id': 'cliente_id',
    'cliente_documento': 'cliente_documento',
    'cliente_nome': 'cliente_nome',
    'e_cooperado': 'e_cooperado',
    'cliente_estado_civil': 'cliente_estado_civil',
    'cliente_sexo': 'cliente_sexo',
    'cliente_nascimento': 'cliente_nascimento',
    'cliente_relacionamento': 'cliente_relacionamento',
    'risco_crl': 'risco_crl',
    'renda_bruta': 'renda_bruta',
    'agencia_id': 'agencia_id',
    'agencia_nome': 'agencia_nome',
    'carteira_nome': 'carteira_nome',
    'cliente_perfil': 'cliente_perfil',
    'profissao': 'profissao',
    'vinculo_empregaticio': 'vinculo_empregaticio',
    'AtividadeEconomica': 'atividade_economica',
    'GrupoCNAE': 'grupo_cnae',
    'CNAE': 'cnae',
    'num_contrato': 'num_contrato',
    'tipo_contrato': 'tipo_contrato',
    'mod_bacen': 'mod_bacen',
    'sub_mod_bacen': 'sub_mod_bacen',
    'linha': 'linha',
    'atraso': 'atraso',
    'risco_atual': 'risco_atual',
    'saldo_atual': 'saldo_atual',
    'saldo_provisao': 'saldo_provisao',
    'VALORGARANTIA': 'valor_garantia',
    'COBGARANTIA': 'cob_garantia',
    'DATAOPERACAO': 'data_operacao',
    'DATAVENCIMENTO': 'data_vencimento',
    'TAXA': 'taxa',
    'INDEXADOR': 'indexador',
    'VALORCONTRATO': 'valor_contrato',
    'RISCOINICIAL': 'risco_inicial'
}

TARGET_TABLE = 'clientes_carteira'
CHUNK_SIZE = 1000


def normalize_col_name(name: str) -> str:
    # keep provided mapping case-sensitive keys handled; otherwise fallback to simple normalizing
    if name in COLUMN_MAP:
        return COLUMN_MAP[name]
    n = name.strip().lower().replace(' ', '_')
    n = n.replace('-', '_')
    return n


def clean_documento(v):
    if pd.isna(v):
        return None
    s = str(v)
    import re
    digits = re.sub(r'\D', '', s)
    return digits if digits else None


def read_input_file(path: Path) -> pd.DataFrame:
    logger.info('Lendo arquivo: %s', path)
    lower = path.suffix.lower()
    if lower == '.csv':
        try:
            sample = path.read_text(encoding='utf-8', errors='ignore')[:4096]
        except Exception:
            sample = path.read_text(encoding='latin-1', errors='ignore')[:4096]
        sep = ';' if sample.count(';') > sample.count(',') else ','
        decimal = ',' if sep == ';' else '.'
        thousands = '.' if sep == ';' else None
        df = None
        for enc in ('utf-8', 'latin-1'):
            try:
                df = pd.read_csv(path, sep=sep, engine='python', decimal=decimal,
                                 thousands=thousands, encoding=enc)
                break
            except UnicodeDecodeError:
                continue
        if df is None:
            df = pd.read_csv(path, sep=sep, engine='python', decimal=decimal,
                             thousands=thousands, encoding='utf-8', errors='ignore')
    else:
        engine_name = 'xlrd' if lower == '.xls' else 'openpyxl'
        df = pd.read_excel(path, engine=engine_name)
    return df


def to_bool(v):
    if pd.isna(v):
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ('s', 'sim', 'y', 'yes', 'true', '1', 't')


def convert_df(dfin: pd.DataFrame) -> pd.DataFrame:
    df = dfin.copy()
    # normalize columns
    col_rename = {c: normalize_col_name(c) for c in df.columns}
    df.rename(columns=col_rename, inplace=True)

    # strip whitespace from string columns
    for c in df.select_dtypes(include=['object']).columns:
        df[c] = df[c].apply(lambda x: x.strip() if isinstance(x, str) else x)

    # clean documento
    if 'cliente_documento' in df.columns:
        df['cliente_documento'] = df['cliente_documento'].apply(clean_documento)

    # ensure cliente_id exists, clean and deduplicate
    if 'cliente_id' in df.columns:
        df = df[~df['cliente_id'].isna()].copy()
        df['cliente_id'] = df['cliente_id'].astype(object).apply(lambda x: str(x).strip())
        df = df[df['cliente_id'] != '']
        df = df.drop_duplicates(subset=['cliente_id'], keep='last')

    # boolean
    if 'e_cooperado' in df.columns:
        df['e_cooperado'] = df['e_cooperado'].apply(to_bool)

    # dates
    for dcol in ['cliente_nascimento', 'data_operacao', 'data_vencimento']:
        if dcol in df.columns:
            try:
                df[dcol] = pd.to_datetime(df[dcol], dayfirst=True, errors='coerce').dt.date
            except Exception:
                df[dcol] = pd.to_datetime(df[dcol], errors='coerce').dt.date

    # numeric conversions
    for ncol in ['renda_bruta', 'saldo_atual', 'saldo_provisao', 'valor_garantia', 'cob_garantia', 'taxa', 'valor_contrato']:
        if ncol in df.columns:
            df[ncol] = pd.to_numeric(df[ncol], errors='coerce')

    # atraso and numeric ints
    if 'atraso' in df.columns:
        df['atraso'] = pd.to_numeric(df['atraso'], errors='coerce').fillna(0).astype('Int64')

    return df


def get_engine():
    user = os.getenv('PGUSER')
    password = os.getenv('PGPASSWORD')
    host = os.getenv('PGHOST', 'localhost')
    port = os.getenv('PGPORT', '5432')
    db = os.getenv('PGDATABASE')
    if not all([user, password, db]):
        raise EnvironmentError('PGUSER, PGPASSWORD and PGDATABASE environment variables must be set')
    logger.info('Conectando ao Postgres host=%s port=%s db=%s user=%s', host, port, db, user)
    try:
        connect_args = {
            'host': host,
            'port': int(port) if str(port).isdigit() else port,
            'user': user,
            'password': password,
            'dbname': db
        }
        engine = create_engine('postgresql+psycopg2://', connect_args=connect_args)
        return engine
    except Exception as exc:
        # imprimir info ambiente útil sem a senha para depuração
        logger.error('Erro ao criar/conectar engine: %s', exc)
        logger.error('Variáveis de ambiente (sem senha): PGUSER=%s, PGHOST=%s, PGPORT=%s, PGDATABASE=%s',
                     user, host, port, db)
        # re-raise para manter traceback original
        raise


def ensure_profissao_dim(engine):
    """Cria a tabela dimensão de profissões se não existir."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS profissao_dim (
        id SERIAL PRIMARY KEY,
        nome TEXT UNIQUE
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def ensure_main_profissao_id(engine, table_name: str = TARGET_TABLE):
    """Adiciona a coluna profissao_id na tabela principal se não existir."""
    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS profissao_id INTEGER;"
    with engine.begin() as conn:
        conn.execute(text(alter_sql))


def upsert_profissoes(engine, nomes: List[str]):
    """Insere nomes únicos na dimensão e retorna mapping nome->id."""
    if not nomes:
        return {}
    unique_nomes = sorted({n for n in nomes if n and str(n).strip()})
    with engine.begin() as conn:
        # Inserir ignorando conflitos
        insert_sql = "INSERT INTO profissao_dim (nome) VALUES (:nome) ON CONFLICT (nome) DO NOTHING"
        conn.execute(text(insert_sql), [{'nome': n} for n in unique_nomes])
        # Buscar ids
        sel = text("SELECT id, nome FROM profissao_dim WHERE nome IN :names").bindparams(
            bindparam('names', expanding=True)
        )
        res = conn.execute(sel, {'names': unique_nomes}).mappings().all()
    mapping = {row['nome']: row['id'] for row in res}
    return mapping


def upsert_dataframe(engine, table_name: str, df: pd.DataFrame, chunk_size: int = CHUNK_SIZE):
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    cols = [c.name for c in table.columns if c.name != 'created_at' and c.name != 'updated_at']
    insert_cols = [c for c in cols if c in df.columns]

    with engine.begin() as conn:
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start+chunk_size]
            chunk_records = chunk[insert_cols].copy()
            chunk_records = chunk_records.astype(object).where(pd.notnull(chunk_records), None)
            records = chunk_records.to_dict(orient='records')
            if not records:
                continue
            stmt = insert(table).values(records)
            update_dict = {c: stmt.excluded[c] for c in insert_cols if c != 'cliente_id'}
            stmt = stmt.on_conflict_do_update(index_elements=['cliente_id'], set_=update_dict)
            conn.execute(stmt)
            logger.info(f'Upserted rows %d..%d', start, start + len(records) - 1)


def main(input_path: str):
    df = read_input_file(Path(input_path))
    logger.info('Linhas lidas: %d, colunas: %d', len(df), len(df.columns))

    dfc = convert_df(df)

    engine = get_engine()

    # garantir dimensão de profissões e coluna na tabela principal
    ensure_profissao_dim(engine)
    ensure_main_profissao_id(engine, TARGET_TABLE)

    # popular dimensão de profissões e mapear ids
    if 'profissao' in dfc.columns:
        nomes = dfc['profissao'].dropna().astype(str).unique().tolist()
        logger.info('Profissões únicas encontradas: %d', len(nomes))
        mapping = upsert_profissoes(engine, nomes)
        # mapear nomes para ids; registros sem match recebem None
        dfc['profissao_id'] = dfc['profissao'].map(mapping).astype('Int64')

    upsert_dataframe(engine, TARGET_TABLE, dfc)
    logger.info('Carga finalizada')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Uso: python load_to_postgres.py <arquivo.xlsx|arquivo.csv>')
        sys.exit(1)
    path = sys.argv[1]
    main(path)
