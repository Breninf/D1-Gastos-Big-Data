CREATE TABLE IF NOT EXISTS despesas_tratadas (
    id_documento BIGINT PRIMARY KEY,
    id_deputado BIGINT,
    nome_parlamentar TEXT,
    sigla_uf VARCHAR(2),
    sigla_partido VARCHAR(50),
    descricao TEXT,
    fornecedor TEXT,
    cnpj_cpf TEXT,
    numero_documento TEXT,
    data_emissao TIMESTAMP,
    valor_documento NUMERIC(15,2),
    valor_glosa NUMERIC(15,2),
    valor_liquido NUMERIC(15,2),
    mes INTEGER,
    ano INTEGER,
    url_documento TEXT,
    operacao_cdc CHAR(1) NOT NULL,
    origem_ts_ms BIGINT,
    processado_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_despesas_tratadas_ano_mes
    ON despesas_tratadas (ano, mes);

CREATE INDEX IF NOT EXISTS idx_despesas_tratadas_uf
    ON despesas_tratadas (sigla_uf);
CREATE TABLE IF NOT EXISTS despesas_tratadas (
    id_documento BIGINT PRIMARY KEY,
    id_deputado BIGINT,
    nome_parlamentar TEXT,
    sigla_uf VARCHAR(2),
    sigla_partido VARCHAR(50),
    descricao TEXT,
    fornecedor TEXT,
    cnpj_cpf TEXT,
    numero_documento TEXT,
    data_emissao TIMESTAMP,
    valor_documento NUMERIC(15,2),
    valor_glosa NUMERIC(15,2),
    valor_liquido NUMERIC(15,2),
    mes INTEGER,
    ano INTEGER,
    url_documento TEXT,
    operacao_cdc CHAR(1) NOT NULL,
    origem_ts_ms BIGINT,
    processado_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_despesas_tratadas_ano_mes
    ON despesas_tratadas (ano, mes);

CREATE INDEX IF NOT EXISTS idx_despesas_tratadas_uf
    ON despesas_tratadas (sigla_uf);
