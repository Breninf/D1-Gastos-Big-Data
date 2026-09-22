/**
 * ingestDespesas.js
 * ---------------------------------------------------------------
 * Consome o parser readXml.js e persiste os registros na tabela
 * despesas_raw do PostgreSQL. Mapeamento de tags CONFIRMADO contra
 * amostra real do XML da Câmara dos Deputados.
 */

const crypto = require("crypto");
const { Pool } = require("pg");
const readXml = require("./readXml");

const pool = new Pool({
  host: process.env.POSTGRES_HOST || "localhost",
  port: process.env.POSTGRES_PORT || 5432,
  database: process.env.POSTGRES_DB || "camara",
  user: process.env.POSTGRES_USER || "postgres",
  password: process.env.POSTGRES_PASSWORD || "postgres",
  max: 5,
});

const BATCH_SIZE = 5000;

// coluna da tabela -> tag real do XML (confirmado com amostra do arquivo)
const COLUMN_TAG_MAP = {
  id_documento: "idDocumento",
  id_deputado: "numeroDeputadoID", // idDeputado vem vazio; o ID real está aqui
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

const COLUMNS = Object.keys(COLUMN_TAG_MAP);

function calcularChecksum(registro) {
  const base = COLUMNS.map((c) => registro[c] ?? "").join("|");
  return crypto.createHash("sha256").update(base).digest("hex");
}

function normalizarNumero(valor) {
  if (valor === undefined || valor === null || String(valor).trim() === "") return null;
  return parseFloat(String(valor).trim().replace(",", "."));
}

function normalizarTexto(valor) {
  if (valor === undefined || valor === null) return null;
  const limpo = String(valor).trim();
  return limpo === "" ? null : limpo;
}

function mapearRegistro(despesaXml) {
  const registro = {};

  for (const [coluna, tag] of Object.entries(COLUMN_TAG_MAP)) {
    let valor = despesaXml[tag] ?? null;

    if (["valor_documento", "valor_glosa", "valor_liquido"].includes(coluna)) {
      valor = normalizarNumero(valor);
    } else if (["id_documento", "id_deputado", "mes", "ano"].includes(coluna)) {
      valor = valor && String(valor).trim() !== "" ? parseInt(valor, 10) : null;
    } else {
      valor = normalizarTexto(valor);
    }

    registro[coluna] = valor;
  }

  registro.checksum = calcularChecksum(registro);
  return registro;
}

async function inserirLote(lote) {
  // descarta registros sem id_documento (chave de idempotência obrigatória)
  const loteValido = lote.filter((r) => r.id_documento !== null);
  const descartados = lote.length - loteValido.length;

  if (loteValido.length === 0) return { inseridos: 0, descartados };

  const colunasSql = [...COLUMNS, "ingested_at", "status_processamento", "checksum"];
  const valuesClauses = [];
  const params = [];
  let idx = 1;

  for (const registro of loteValido) {
    const linha = COLUMNS.map((c) => registro[c]);
    linha.push(new Date(), "raw", registro.checksum);

    const placeholders = linha.map(() => `$${idx++}`);
    valuesClauses.push(`(${placeholders.join(",")})`);
    params.push(...linha);
  }

  const query = `
    INSERT INTO despesas_raw (${colunasSql.join(",")})
    VALUES ${valuesClauses.join(",")}
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

  const client = await pool.connect();
  try {
    await client.query(query, params);
  } finally {
    client.release();
  }

  return { inseridos: loteValido.length, descartados };
}

async function processarArquivo(filePath) {
  let lote = [];
  let totalProcessado = 0;
  let totalDescartados = 0;
  const inicio = Date.now();

  await readXml(filePath, async (despesaXml) => {
    const registro = mapearRegistro(despesaXml);
    lote.push(registro);

    if (lote.length >= BATCH_SIZE) {
      const loteAtual = lote;
      lote = [];
      const { inseridos, descartados } = await inserirLote(loteAtual);
      totalProcessado += inseridos;
      totalDescartados += descartados;
      console.log(`[${filePath}] Processados: ${totalProcessado} | Descartados: ${totalDescartados}`);
    }
  });

  const { inseridos, descartados } = await inserirLote(lote);
  totalProcessado += inseridos;
  totalDescartados += descartados;

  const duracaoSeg = ((Date.now() - inicio) / 1000).toFixed(1);
  console.log(
    `Arquivo ${filePath} finalizado. Total: ${totalProcessado} | Descartados: ${totalDescartados} | Tempo: ${duracaoSeg}s`
  );

  return { totalProcessado, totalDescartados };
}

async function main() {
  const arquivos = process.argv.slice(2);

  if (arquivos.length === 0) {
    console.error("Uso: node ingestDespesas.js <arquivo1.xml> [arquivo2.xml ...]");
    process.exit(1);
  }

  let totalGeral = 0;
  for (const arquivo of arquivos) {
    const resultado = await processarArquivo(arquivo);
    totalGeral += resultado.totalProcessado;
  }

  console.log(`Concluído. Total geral processado: ${totalGeral}`);
  await pool.end();
}

if (require.main === module) {
  main().catch((err) => {
    console.error("Erro fatal:", err);
    process.exit(1);
  });
}

module.exports = { processarArquivo, mapearRegistro, calcularChecksum };