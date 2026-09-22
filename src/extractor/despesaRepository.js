const { Pool } = require("pg");
const crypto = require("crypto");

/*az duas coisas: mapeia as tags brutas do XML para as colunas da tabela despesas_raw
(via TAG_MAP, os mesmos nomes de campo do CEAP que já validamos antes) e monta um INSERT multi-linha
parametrizado com ON CONFLICT (id_documento) DO UPDATE, calculando um checksum por registro para só 
atualizar quando o dado realmente mudou.*/

const pool = new Pool({
  host: process.env.POSTGRES_HOST || "localhost",
  port: process.env.POSTGRES_PORT || 5432,
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  max: 10,
});

// Mapeamento: coluna da tabela -> tag XML correspondente.
// CONFIRME esses nomes contra uma amostra real do XML antes de rodar em
// escala — os nomes históricos do CEAP variam um pouco entre exportações.
//Nota do Alysson: antes de fazer o teste com um xml de menor escala, deve cofirmar os nomes das tags contra uma amotra real do XML.
const TAG_MAP = {
  id_documento: "idDocumento",
  id_deputado: "numeroDeputadoID",
  nome_parlamentar: "nomeParlamentar",
  cpf: "cpf",
  sigla_uf: "siglaUF",
  sigla_partido: "siglaPartido",
  descricao: "descricao",
  fornecedor: "fornecedor",
  cnpj_cpf: "cnpjCPF",
  numero_documento: "numero",
  data_emissao: "dataEmissao",
  valor_documento: "valorDocumento",
  valor_glosa: "valorGlosa",
  valor_liquido: "valorLiquido",
  mes: "mes",
  ano: "ano",
  url_documento: "urlDocumento",
};

const COLUNAS_DADOS = Object.keys(TAG_MAP);
const COLUNAS_TOTAIS = [...COLUNAS_DADOS, "ingested_at", "status_processamento", "checksum"];

function parseNumero(valor) {
  if (valor === undefined || valor === null || valor.trim() === "") return null;
  return valor.trim().replace(",", ".");
}

function parseData(valor) {
  if (!valor) return null;
  return valor.trim(); // Postgres aceita 'YYYY-MM-DD' diretamente
}

function calcularChecksum(registro) {
  const base = COLUNAS_DADOS.map((c) => registro[c] ?? "").join("|");
  return crypto.createHash("sha256").update(base).digest("hex");
}

/**
 * Converte o objeto bruto vindo do parser (chaves = tags do XML) para o
 * formato de colunas da tabela `despesas_raw`, já com metadados de
 * rastreabilidade.
 */
function mapearDespesa(despesaBruta) {
  const registro = {};

  for (const [coluna, tag] of Object.entries(TAG_MAP)) {
    let valor = despesaBruta[tag] ?? null;

    if (["valor_documento", "valor_glosa", "valor_liquido"].includes(coluna)) {
      valor = parseNumero(valor);
    } else if (coluna === "data_emissao") {
      valor = parseData(valor);
    } else if (["id_documento", "id_deputado", "mes", "ano"].includes(coluna)) {
      valor = valor ? Number(valor) : null;
    }

    registro[coluna] = valor;
  }

  registro.ingested_at = new Date().toISOString();
  registro.status_processamento = "raw";
  registro.checksum = calcularChecksum(registro);

  return registro;
}

/**
 * Monta um INSERT multi-linha parametrizado e idempotente
 * (ON CONFLICT em id_documento).
 */
function construirQueryBatch(lote) {
  const linhas = [];
  const valores = [];
  let paramIndex = 1;

  for (const registro of lote) {
    const placeholders = COLUNAS_TOTAIS.map(() => `$${paramIndex++}`);
    linhas.push(`(${placeholders.join(", ")})`);
    valores.push(...COLUNAS_TOTAIS.map((c) => registro[c]));
  }

  const texto = `
    INSERT INTO despesas_raw (${COLUNAS_TOTAIS.join(", ")})
    VALUES ${linhas.join(", ")}
    ON CONFLICT (id_documento)
    DO UPDATE SET
      valor_documento = EXCLUDED.valor_documento,
      valor_glosa = EXCLUDED.valor_glosa,
      valor_liquido = EXCLUDED.valor_liquido,
      status_processamento = EXCLUDED.status_processamento,
      checksum = EXCLUDED.checksum,
      ingested_at = EXCLUDED.ingested_at
    WHERE despesas_raw.checksum IS DISTINCT FROM EXCLUDED.checksum;
  `;

  return { texto, valores };
}

async function inserirLote(loteBruto) {
  if (loteBruto.length === 0) return 0;

  const lote = loteBruto.map(mapearDespesa).filter((r) => r.id_documento !== null);
  if (lote.length === 0) return 0;

  const { texto, valores } = construirQueryBatch(lote);

  const client = await pool.connect();
  try {
    await client.query(texto, valores);
    return lote.length;
  } finally {
    client.release();
  }
}

async function encerrarPool() {
  await pool.end();
}

module.exports = {
  inserirLote,
  encerrarPool,
};