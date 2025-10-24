-- CREATE TABLE para a carteira de clientes
-- Ajuste tipos e constraints conforme necessário

CREATE TABLE IF NOT EXISTS clientes_carteira (
  cliente_id TEXT PRIMARY KEY,
  cliente_documento TEXT,
  cliente_nome TEXT,
  e_cooperado BOOLEAN,
  cliente_estado_civil TEXT,
  cliente_sexo TEXT,
  cliente_nascimento DATE,
  cliente_relacionamento TEXT,
  risco_crl TEXT,
  renda_bruta NUMERIC(15,2),
  agencia_id TEXT,
  agencia_nome TEXT,
  carteira_nome TEXT,
  cliente_perfil TEXT,
  profissao TEXT,
  vinculo_empregaticio TEXT,
  atividade_economica TEXT,
  grupo_cnae TEXT,
  cnae TEXT,
  num_contrato TEXT,
  tipo_contrato TEXT,
  mod_bacen TEXT,
  sub_mod_bacen TEXT,
  linha TEXT,
  atraso INTEGER,
  risco_atual TEXT,
  saldo_atual NUMERIC(15,2),
  saldo_provisao NUMERIC(15,2),
  valor_garantia NUMERIC(15,2),
  cob_garantia NUMERIC(15,2),
  data_operacao DATE,
  data_vencimento DATE,
  taxa NUMERIC(10,6),
  indexador TEXT,
  valor_contrato NUMERIC(15,2),
  risco_inicial TEXT,
  extras JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Índices úteis para consultas de análise
CREATE INDEX IF NOT EXISTS idx_clientes_documento ON clientes_carteira (cliente_documento);
CREATE INDEX IF NOT EXISTS idx_clientes_agencia ON clientes_carteira (agencia_id);
CREATE INDEX IF NOT EXISTS idx_clientes_carteira_nome ON clientes_carteira (carteira_nome);
CREATE INDEX IF NOT EXISTS idx_clientes_atraso ON clientes_carteira (atraso);
CREATE INDEX IF NOT EXISTS idx_clientes_risco_atual ON clientes_carteira (risco_atual);
CREATE INDEX IF NOT EXISTS idx_clientes_data_operacao ON clientes_carteira (data_operacao);

-- GIN index para pesquisa em texto livre / extras
CREATE INDEX IF NOT EXISTS idx_clientes_extras_gin ON clientes_carteira USING gin (extras);

-- Observações:
-- - Use CPF/CNPJ limpo (apenas dígitos) em `cliente_documento` para facilitar joins/uniqueness.
-- - Se `cliente_documento` for único por cliente, considere adicionar UNIQUE(cliente_documento).
-- - Para grandes volumes, avalie particionamento por RANGE (data_operacao) ou por carteira_nome.

-- Dimensão de Profissões (reduz alta cardinalidade de profissões textuais)
CREATE TABLE IF NOT EXISTS profissao_dim (
  id SERIAL PRIMARY KEY,
  nome TEXT UNIQUE
);

-- Adiciona coluna de FK na tabela principal para referenciar profissão
ALTER TABLE clientes_carteira ADD COLUMN IF NOT EXISTS profissao_id INTEGER;
CREATE INDEX IF NOT EXISTS idx_clientes_profissao_id ON clientes_carteira (profissao_id);
