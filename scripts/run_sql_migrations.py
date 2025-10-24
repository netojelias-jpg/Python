"""Execute todos os arquivos .sql do diretório sql/ em ordem alfabética."""
from __future__ import annotations

import logging
from pathlib import Path
import sys

from dotenv import load_dotenv
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parents[1]

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from etl.load_to_postgres import get_engine  # noqa: E402  pylint: disable=wrong-import-position

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_sql_file(engine, sql_path: Path) -> None:
    logger.info("Aplicando script SQL: %s", sql_path.name)
    sql_content = sql_path.read_text(encoding="utf-8")
    if not sql_content.strip():
        logger.info("Arquivo vazio, pulando")
        return
    with engine.begin() as conn:
        conn.exec_driver_sql(sql_content)


def main() -> None:
    sql_dir = ROOT_DIR / "sql"
    if not sql_dir.exists():
        raise FileNotFoundError(f"Diretório não encontrado: {sql_dir}")

    scripts = sorted(sql_dir.glob("*.sql"))
    if not scripts:
        logger.warning("Nenhum arquivo .sql encontrado em %s", sql_dir)
        return

    engine = get_engine()
    for script_path in scripts:
        apply_sql_file(engine, script_path)
    logger.info("Scripts executados com sucesso.")


if __name__ == "__main__":
    main()
