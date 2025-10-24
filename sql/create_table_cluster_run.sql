-- Tabelas para versionamento dos clusters gerados pelo algoritmo de segmentação

CREATE TABLE IF NOT EXISTS cluster_run (
    run_id UUID PRIMARY KEY,
    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    perfil TEXT NOT NULL,
    algoritmo TEXT NOT NULL,
    parametros JSONB,
    metricas JSONB
);

CREATE TABLE IF NOT EXISTS cluster_run_clientes (
    run_id UUID NOT NULL REFERENCES cluster_run(run_id) ON DELETE CASCADE,
    cliente_id TEXT NOT NULL,
    cliente_perfil TEXT,
    agencia_nome TEXT,
    carteira_nome TEXT,
    linha TEXT,
    cluster INTEGER NOT NULL,
    risco_inicial TEXT,
    risco_inicial_score INTEGER,
    factors JSONB,
    PRIMARY KEY (run_id, cliente_id)
);

CREATE TABLE IF NOT EXISTS cluster_run_resumo (
    run_id UUID NOT NULL REFERENCES cluster_run(run_id) ON DELETE CASCADE,
    cluster INTEGER NOT NULL,
    total_clientes INTEGER,
    risco_inicial_medio NUMERIC(10,2),
    cobertura_media NUMERIC(10,2),
    atraso_medio NUMERIC(10,2),
    valor_contrato_medio NUMERIC(14,2),
    saldo_atual_medio NUMERIC(14,2),
    PRIMARY KEY (run_id, cluster)
);

CREATE INDEX IF NOT EXISTS idx_cluster_run_perfil ON cluster_run (perfil);
CREATE INDEX IF NOT EXISTS idx_cluster_run_clientes_cliente ON cluster_run_clientes (cliente_id);
CREATE INDEX IF NOT EXISTS idx_cluster_run_clientes_cluster ON cluster_run_clientes (cluster);
