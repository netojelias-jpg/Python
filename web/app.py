import json
import logging
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from flask import Flask, redirect, render_template_string, url_for
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from etl.load_to_postgres import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

_BASE_QUERY = text(
    """
    SELECT
        cliente_id,
        cliente_nome,
        cliente_perfil,
        agencia_nome,
        carteira_nome,
        linha,
        risco_inicial,
        cob_garantia,
        atraso,
        valor_contrato,
        saldo_atual,
        data_operacao
    FROM clientes_carteira
    """
)

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
RISK_MAP = {name: idx for idx, name in enumerate(RISK_ORDER)}


_DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="utf-8" />
    <title>Visão Analítica - Crédito</title>
    <style>
        :root {
            color-scheme: light dark;
            font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }
        body {
            margin: 0;
            background-color: #f4f6f8;
            color: #1a1a1a;
        }
        header {
            background: linear-gradient(90deg, #0b5394, #1856a5);
            color: #fff;
            padding: 2rem 3rem;
        }
        main {
            padding: 2rem 3rem 3rem;
        }
        h1 {
            margin: 0;
            font-size: 2rem;
        }
        p.lead {
            margin-top: 0.5rem;
            opacity: 0.9;
        }
        .grid {
            display: grid;
            gap: 1.5rem;
        }
        .grid.cards {
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        }
        .grid.two {
            grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
        }
        .card {
            background: #fff;
            border-radius: 12px;
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
            padding: 1.5rem;
        }
        .card h2 {
            margin: 0;
            font-size: 0.95rem;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: #0b5394;
        }
        .card .value {
            margin-top: 0.75rem;
            font-size: 1.8rem;
            font-weight: 600;
        }
        .card small {
            display: block;
            margin-top: 0.5rem;
            color: #6b7280;
        }
        .card.error {
            border-left: 4px solid #ef4444;
        }
        .card.error p {
            margin: 0.5rem 0 0;
        }
        canvas {
            width: 100% !important;
            height: 320px !important;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
        }
        th, td {
            padding: 0.75rem 1rem;
            border-bottom: 1px solid #e5e7eb;
            text-align: left;
        }
        th {
            font-size: 0.85rem;
            letter-spacing: 0.02em;
            text-transform: uppercase;
            color: #555;
        }
        tr:nth-child(even) {
            background: #f9fafb;
        }
        .empty {
            font-style: italic;
            margin-top: 1rem;
        }
        footer {
            text-align: center;
            margin-top: 3rem;
            color: #6b7280;
            font-size: 0.85rem;
        }
        @media (max-width: 768px) {
            header, main {
                padding: 1.5rem;
            }
        }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js" integrity="sha384-uFeqdNxDm1ZSA8ueWndBG+b/8j5bgzXQL0tx41QM4ZkvTB5fnuKYVDT4FjbOoaTd" crossorigin="anonymous"></script>
</head>
<body>
    <header>
        <h1>Monitor de Carteiras</h1>
        <p class="lead">Panorama consolidado das operações de crédito e risco por carteira, agência e produto.</p>
    </header>
    <main>
        {% if error %}
            <div class="card error">
                <h2>Configuração pendente</h2>
                <p>{{ error }}</p>
            </div>
        {% endif %}
        {% if not has_data %}
            <div class="card">
                <h2>Sem dados</h2>
                <p class="value">Nenhum registro disponível para montar o dashboard.</p>
            </div>
        {% else %}
        <section class="grid cards">
            <article class="card">
                <h2>Clientes Ativos</h2>
                <p class="value">{{ kpis.total_clientes }}</p>
                <small>Contagem única de clientes com operações mapeadas.</small>
            </article>
            <article class="card">
                <h2>Contratos</h2>
                <p class="value">{{ kpis.total_contratos }}</p>
                <small>Total de registros disponíveis para análise.</small>
            </article>
            <article class="card">
                <h2>Saldo Atual</h2>
                <p class="value">{{ kpis.saldo_total }}</p>
                <small>Somatório dos saldos em aberto nas carteiras.</small>
            </article>
            <article class="card">
                <h2>Ticket Médio</h2>
                <p class="value">{{ kpis.ticket_medio }}</p>
                <small>Média por contrato das liberações registradas.</small>
            </article>
            <article class="card">
                <h2>Com Atraso</h2>
                <p class="value">{{ kpis.pct_atraso }}</p>
                <small>Percentual de contratos com atraso informado.</small>
            </article>
        </section>

        <section class="grid two" style="margin-top: 2rem;">
            <article class="card">
                <h2>Saldo por Perfil</h2>
                <canvas id="perfilChart"></canvas>
            </article>
            <article class="card">
                <h2>Top Agências por Saldo</h2>
                <canvas id="agenciaChart"></canvas>
            </article>
        </section>

        <section class="grid two" style="margin-top: 2rem;">
            <article class="card">
                <h2>Mix por Linha</h2>
                <canvas id="linhaChart"></canvas>
            </article>
            <article class="card">
                <h2>Carteiras com Maior Risco</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Carteira</th>
                            <th>Risco Médio</th>
                            <th>Saldo Total</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for item in carteira_risco %}
                        <tr>
                            <td>{{ item.carteira_nome }}</td>
                            <td>{{ item.risco_medio }}</td>
                            <td>{{ item.saldo_total }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </article>
        </section>

        <section class="card" style="margin-top: 2rem;">
            <h2>Clientes Expostos</h2>
            <table>
                <thead>
                    <tr>
                        <th>Cliente</th>
                        <th>Carteira</th>
                        <th>Agência</th>
                        <th>Saldo</th>
                        <th>Atraso</th>
                    </tr>
                </thead>
                <tbody>
                    {% for item in top_clientes %}
                    <tr>
                        <td>{{ item.cliente_nome }}</td>
                        <td>{{ item.carteira_nome }}</td>
                        <td>{{ item.agencia_nome }}</td>
                        <td>{{ item.saldo_atual }}</td>
                        <td>{{ item.atraso }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </section>
        {% endif %}
        <footer>Protótipo de dashboard - dados atualizados diretamente do banco operacional.</footer>
    </main>
    <script>
    const perfilData = {{ perfil_chart | safe }};
    const agenciaData = {{ agencia_chart | safe }};
    const linhaData = {{ linha_chart | safe }};

    if (perfilData.labels.length) {
        new Chart(document.getElementById("perfilChart"), {
            type: "bar",
            data: perfilData,
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    x: { ticks: { color: "#374151" } },
                    y: { ticks: { color: "#374151" } }
                }
            }
        });
    }

    if (agenciaData.labels.length) {
        new Chart(document.getElementById("agenciaChart"), {
            type: "bar",
            data: agenciaData,
            options: {
                indexAxis: "y",
                responsive: true,
                scales: {
                    x: { ticks: { color: "#374151" } },
                    y: { ticks: { color: "#374151" } }
                }
            }
        });
    }

    if (linhaData.labels.length) {
        new Chart(document.getElementById("linhaChart"), {
            type: "doughnut",
            data: linhaData,
            options: {
                responsive: true,
                plugins: { legend: { position: "bottom" } }
            }
        });
    }
    </script>
</body>
</html>
"""


def _format_brl(value: float) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"R$ {value:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")


def _build_palette(n: int) -> list[str]:
    base = [
        "#0b5394",
        "#1856a5",
        "#1f6dd6",
        "#0d9488",
        "#f59e0b",
        "#ef4444",
        "#6366f1",
        "#9333ea",
        "#ec4899",
        "#22c55e",
    ]
    if n <= len(base):
        return base[:n]
    reps = (n // len(base)) + 1
    return (base * reps)[:n]


def prepare_dashboard_context(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "has_data": False,
            "kpis": {},
            "perfil_chart": json.dumps({"labels": [], "datasets": []}),
            "agencia_chart": json.dumps({"labels": [], "datasets": []}),
            "linha_chart": json.dumps({"labels": [], "datasets": []}),
            "carteira_risco": [],
            "top_clientes": [],
        }

    if "risco_inicial" in df.columns:
        df["risco_inicial"] = df["risco_inicial"].astype(str).str.upper().str.strip()
        df["risco_inicial_score"] = df["risco_inicial"].map(RISK_MAP)

    numeric_cols = [
        "risco_inicial_score",
        "cob_garantia",
        "atraso",
        "valor_contrato",
        "saldo_atual",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "data_operacao" in df.columns:
        df["data_operacao"] = pd.to_datetime(df["data_operacao"], errors="coerce")

    total_contratos = int(len(df))
    total_clientes = int(df["cliente_id"].nunique())
    saldo_total_val = float(df["saldo_atual"].sum(skipna=True) or 0)
    ticket_medio_val = float(df["valor_contrato"].mean(skipna=True) or 0)
    pct_atraso_val = float(((df["atraso"].fillna(0) > 0).mean() or 0) * 100)

    perfil_agg = (
        df.groupby("cliente_perfil")
        .agg(saldo_total=("saldo_atual", "sum"), risco_medio=("risco_inicial_score", "mean"))
        .fillna(0)
        .reset_index()
        .sort_values("saldo_total", ascending=False)
    )

    agencia_agg = (
        df.groupby("agencia_nome")["saldo_atual"].sum().sort_values(ascending=False).head(10).reset_index()
    )

    linha_agg = (
        df.groupby("linha")["valor_contrato"].sum().sort_values(ascending=False).head(8).reset_index()
    )

    carteira_risco = (
        df.groupby("carteira_nome")
        .agg(
            risco_medio=("risco_inicial_score", "mean"),
            saldo_total=("saldo_atual", "sum"),
        )
        .fillna(0)
        .reset_index()
        .sort_values("risco_medio", ascending=False)
        .head(8)
    )

    top_clientes = (
        df.sort_values("saldo_atual", ascending=False)
        .head(10)[["cliente_nome", "carteira_nome", "agencia_nome", "saldo_atual", "atraso"]]
        .copy()
    )

    perfil_chart = json.dumps(
        {
            "labels": perfil_agg["cliente_perfil"].fillna("Não informado").tolist(),
            "datasets": [
                {
                    "label": "Saldo total",
                    "data": [round(float(v or 0), 2) for v in perfil_agg["saldo_total"].tolist()],
                    "backgroundColor": _build_palette(len(perfil_agg)),
                }
            ],
        }
    )

    agencia_chart = json.dumps(
        {
            "labels": agencia_agg["agencia_nome"].fillna("Não informado").tolist(),
            "datasets": [
                {
                    "label": "Saldo total",
                    "data": [round(float(v or 0), 2) for v in agencia_agg["saldo_atual"].tolist()],
                    "backgroundColor": _build_palette(len(agencia_agg)),
                }
            ],
        }
    )

    linha_chart = json.dumps(
        {
            "labels": linha_agg["linha"].fillna("Não informado").tolist(),
            "datasets": [
                {
                    "label": "Valor contratado",
                    "data": [round(float(v or 0), 2) for v in linha_agg["valor_contrato"].tolist()],
                    "backgroundColor": _build_palette(len(linha_agg)),
                }
            ],
        }
    )

    carteira_risco_display = [
        {
            "carteira_nome": row["carteira_nome"] or "Não informado",
            "risco_medio": f"{float(row['risco_medio'] or 0):.2f}",
            "saldo_total": _format_brl(float(row["saldo_total"] or 0)),
        }
        for _, row in carteira_risco.iterrows()
    ]

    top_clientes_display = [
        {
            "cliente_nome": row["cliente_nome"] or "-",
            "carteira_nome": row["carteira_nome"] or "-",
            "agencia_nome": row["agencia_nome"] or "-",
            "saldo_atual": _format_brl(float(row["saldo_atual"] or 0)),
            "atraso": f"{float(row['atraso'] or 0):.0f} dias",
        }
        for _, row in top_clientes.iterrows()
    ]

    return {
        "has_data": True,
        "kpis": {
            "total_clientes": f"{total_clientes:,}".replace(",", "."),
            "total_contratos": f"{total_contratos:,}".replace(",", "."),
            "saldo_total": _format_brl(saldo_total_val),
            "ticket_medio": _format_brl(ticket_medio_val),
            "pct_atraso": f"{pct_atraso_val:.1f}%",
        },
        "perfil_chart": perfil_chart,
        "agencia_chart": agencia_chart,
        "linha_chart": linha_chart,
        "carteira_risco": carteira_risco_display,
        "top_clientes": top_clientes_display,
    }


@app.route("/")
def root():
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    try:
        engine = get_engine()
    except EnvironmentError as exc:  # missing PG* variables
        logger.error("Falha ao construir engine do Postgres: %s", exc)
        context = prepare_dashboard_context(pd.DataFrame())
        context["error"] = (
            "Defina PGHOST, PGPORT, PGDATABASE, PGUSER e PGPASSWORD (ou crie um arquivo .env) antes de acessar o dashboard."
        )
        return render_template_string(_DASHBOARD_TEMPLATE, **context)

    logger.info("Montando dashboard consolidado")
    with engine.connect() as conn:
        df = pd.read_sql(_BASE_QUERY, conn)
    context = prepare_dashboard_context(df)
    context.setdefault("error", None)
    return render_template_string(_DASHBOARD_TEMPLATE, **context)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
