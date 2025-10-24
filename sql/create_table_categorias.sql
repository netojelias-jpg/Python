-- Tabela de categorias com combinações de atributos de clientes e produtos
-- Execute este script dentro do seu banco existente (ex: sanalise)

CREATE TABLE IF NOT EXISTS categoria (
  id SERIAL PRIMARY KEY,
  e_cooperado BOOLEAN,
  cliente_estado_civil VARCHAR(50),
  cliente_perfil VARCHAR(100),
  profissao TEXT,
  vinculo_empregaticio VARCHAR(150),
  atividade_economica TEXT,
  grupo_cnae VARCHAR(100),
  cnae VARCHAR(150),
  tipo_contrato VARCHAR(100),
  mod_bacen VARCHAR(120),
  sub_mod_bacen VARCHAR(150),
  linha VARCHAR(150),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_categoria_profissao ON categoria(profissao);
CREATE INDEX IF NOT EXISTS idx_categoria_cnae ON categoria(cnae);
CREATE INDEX IF NOT EXISTS idx_categoria_tipo_contrato ON categoria(tipo_contrato);
