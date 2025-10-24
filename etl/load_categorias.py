"""Carga de categorias a partir de um arquivo Excel (.xlsx ou .xls).

Uso (PowerShell):
$env:PGHOST="localhost"; $env:PGPORT="5432"; $env:PGDATABASE="sanalise"; $env:PGUSER="postgres"; $env:PGPASSWORD="123456"
python .\etl\load_categorias.py "D:\\Meus programas\\Sicoob\\categorias.xlsx" --truncate

Dependências: pandas, SQLAlchemy, psycopg2-binary, openpyxl (já listadas em requirements).
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = [
    'e_cooperado',
    'cliente_estado_civil',
    'cliente_perfil',
    'profissao',
    'vinculo_empregaticio',
    'AtividadeEconomica',
    'GrupoCNAE',
    'CNAE',
    'tipo_contrato',
    'mod_bacen',
    'sub_mod_bacen',
    'linha',
]


COLUMN_RENAME = {
    'AtividadeEconomica': 'atividade_economica',
    'GrupoCNAE': 'grupo_cnae',
    'CNAE': 'cnae'
}


def to_bool(v):
    if pd.isna(v):
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if not s:
        return None
    return s in ('s', 'sim', 'y', 'yes', 'true', '1', 't')


def get_engine():
    user = os.getenv('PGUSER')
    password = os.getenv('PGPASSWORD')
    host = os.getenv('PGHOST', 'localhost')
    port = os.getenv('PGPORT', '5432')
    db = os.getenv('PGDATABASE')
    if not all([user, password, db]):
        raise EnvironmentError('PGUSER, PGPASSWORD e PGDATABASE precisam estar configuradas.')

    logger.info('Conectando ao Postgres host=%s port=%s db=%s user=%s', host, port, db, user)
    connect_args = {
        'host': host,
        'port': int(port) if str(port).isdigit() else port,
        'user': user,
        'password': password,
        'dbname': db
    }
    engine = create_engine('postgresql+psycopg2://', connect_args=connect_args)
    return engine


def read_excel(path: Path) -> pd.DataFrame:
    engine_name = 'openpyxl'
    if str(path).lower().endswith('.xls'):
        engine_name = 'xlrd'
    logger.info('Lendo arquivo %s com engine=%s', path, engine_name)
    df = pd.read_excel(path, engine=engine_name)
    logger.info('Linhas lidas: %d, colunas: %d', len(df), len(df.columns))
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # manter somente as colunas esperadas
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f'Colunas ausentes no arquivo: {missing}')

    df = df[EXPECTED_COLUMNS]

    rename_map = {c: COLUMN_RENAME.get(c, c) for c in df.columns}
    df.rename(columns=rename_map, inplace=True)

    # trim strings
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    df['e_cooperado'] = df['e_cooperado'].apply(to_bool)

    # substituir strings vazias por None para evitar inserção de ""
    df = df.replace({"": None})
    df = df.where(pd.notnull(df), None)

    # remover duplicatas exatas
    df = df.drop_duplicates()
    logger.info('Total de registros após limpeza/deduplicação: %d', len(df))
    return df


def ensure_categoria_table(engine):
    create_sql_path = Path(__file__).resolve().parent.parent / 'sql' / 'create_table_categorias.sql'
    if create_sql_path.exists():
        sql_text = create_sql_path.read_text(encoding='utf-8')
        with engine.begin() as conn:
            conn.execute(text(sql_text))
        logger.info('Garantido que a tabela categoria existe (script SQL executado).')
    else:
        logger.warning('Arquivo create_table_categorias.sql não encontrado; assumindo que a tabela já existe.')


def truncate_table(engine, table_name: str):
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE {table_name} RESTART IDENTITY;'))
    logger.info('Tabela %s truncada.', table_name)


def insert_dataframe(engine, df: pd.DataFrame, table_name: str, chunk_size: int = 1000):
    df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=chunk_size, method='multi')
    logger.info('Inserção concluída (%d linhas).', len(df))


def main():
    parser = argparse.ArgumentParser(description='Carrega categorias do Excel para Postgres.')
    parser.add_argument('excel_path', type=Path, help='Caminho do arquivo categorias .xlsx ou .xls')
    parser.add_argument('--table', default='categoria', help='Nome da tabela de destino (default: categoria)')
    parser.add_argument('--truncate', action='store_true', help='Truncar a tabela antes de inserir')
    args = parser.parse_args()

    df_raw = read_excel(args.excel_path)
    df_clean = clean_dataframe(df_raw)

    engine = get_engine()
    ensure_categoria_table(engine)

    if args.truncate:
        truncate_table(engine, args.table)

    insert_dataframe(engine, df_clean, args.table)


if __name__ == '__main__':
    main()
