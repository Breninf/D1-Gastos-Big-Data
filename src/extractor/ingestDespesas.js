require("dotenv").config();

const crypto = require("crypto");
const { Pool } = require("pg");
const readXml = require("./readXml");

const pool = new Pool({
  host: process.env.POSTGRES_HOST || "localhost",
  port: Number(process.env.POSTGRES_PORT || 5432),
  database: process.env.POSTGRES_DB || "camara",
  user: process.env.POSTGRES_USER || "postgres",
  password: process.env.POSTGRES_PASSWORD || "postgres",
  max: 5,
});

// 1.000 registros x 20 colunas = aproximadamente 20.000 parâmetros.
const BATCH_SIZE = 1000;

const COLUMN_TAG_MAP = {
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

const COLUMNS = Object.keys(COLUMN_TAG_MAP);
const NUMERIC_COLUMNS = [
  "id_documento",
  "id_deputado",
  "mes",
  "ano",
];
const DECIMAL_COLUMNS = [
  "valor_documento",
  "valor_glosa",
  "valor_liquido",
];

function normalizarTexto(valor) {
  if (valor === undefined || valor === null) return null;

  const texto = String(valor).trim();
  return texto === "" ? null : texto;
}

function normalizarInteiro(valor) {
  const texto = normalizarTexto(valor);
  if (texto === null) return null;

  const numero = Number.parseInt(texto, 10);
  return Number.isNaN(numero) ? null : numero;
}

function normalizarDecimal(valor) {
  const texto = normalizarTexto(valor);
  if (texto === null) return null;

  // Aceita tanto 1467.50 quanto 1.467,50.
  const normalizado = texto.includes(",")
    ? texto.replace(/\./g, "").replace(",", ".")
    : texto;

  const numero = Number.parseFloat(normalizado);
  return Number.isNaN(numero) ? null : numero;
}

function calcularChecksum(registro) {
  const base = COLUMNS
    .map((coluna) => registro[coluna] ?? "")
    .join("|");

  return crypto
    .createHash("sha256")
    .update(base, "utf8")
    .digest("hex");
}

function mapearRegistro(despesaXml) {
  const registro = {};

  for (const [coluna, tag] of Object.entries(COLUMN_TAG_MAP)) {
    const valorBruto = despesaXml[tag];

    if (NUMERIC_COLUMNS.includes(coluna)) {
      registro[coluna] = normalizarInteiro(valorBruto);
    } else if (DECIMAL_COLUMNS.includes(coluna)) {
      registro[coluna] = normalizarDecimal(valorBruto);
    } else {
      registro[coluna] = normalizarTexto(valorBruto);
    }
  }

  registro.checksum = calcularChecksum(registro);
  return registro;
}

function deduplicarPorIdDocumento(registros) {
  const unicos = new Map();
  let duplicados = 0;

  for (const registro of registros) {
    if (registro.id_documento === null) {
      continue;
    }

    if (unicos.has(registro.id_documento)) {
      duplicados += 1;
    }

    // Mantém a última ocorrência do mesmo documento no lote.
    unicos.set(registro.id_documento, registro);
  }

  return {
    registros: Array.from(unicos.values()),
    duplicados,
  };
}

async function inserirLote(lote) {
  const {
    registros: loteSemDuplicatas,
    duplicados,
  } = deduplicarPorIdDocumento(lote);

  const loteValido = loteSemDuplicatas.filter(
    (registro) => registro.id_documento !== null
  );

  const descartados = lote.length - loteValido.length;

  if (loteValido.length === 0) {
    return {
      inseridos: 0,
      descartados,
      duplicados,
    };
  }

  const colunasSql = [
    ...COLUMNS,
    "ingested_at",
    "status_processamento",
    "checksum",
  ];

  const valuesClauses = [];
  const params = [];
  let parameterIndex = 1;

  for (const registro of loteValido) {
    const linha = COLUMNS.map((coluna) => registro[coluna]);

    linha.push(
      new Date(),
      "raw",
      registro.checksum
    );

    const placeholders = linha.map(
      () => `$${parameterIndex++}`
    );

    valuesClauses.push(`(${placeholders.join(",")})`);
    params.push(...linha);
  }

  const query = `
    INSERT INTO despesas_raw (${colunasSql.join(",")})
    VALUES ${valuesClauses.join(",")}
    ON CONFLICT (id_documento)
    DO UPDATE SET
      id_deputado = EXCLUDED.id_deputado,
      nome_parlamentar = EXCLUDED.nome_parlamentar,
      cpf = EXCLUDED.cpf,
      sigla_uf = EXCLUDED.sigla_uf,
      sigla_partido = EXCLUDED.sigla_partido,
      descricao = EXCLUDED.descricao,
      fornecedor = EXCLUDED.fornecedor,
      cnpj_cpf = EXCLUDED.cnpj_cpf,
      numero_documento = EXCLUDED.numero_documento,
      data_emissao = EXCLUDED.data_emissao,
      valor_documento = EXCLUDED.valor_documento,
      valor_glosa = EXCLUDED.valor_glosa,
      valor_liquido = EXCLUDED.valor_liquido,
      mes = EXCLUDED.mes,
      ano = EXCLUDED.ano,
      url_documento = EXCLUDED.url_documento,
      status_processamento = EXCLUDED.status_processamento,
      checksum = EXCLUDED.checksum,
      ingested_at = EXCLUDED.ingested_at
    WHERE despesas_raw.checksum IS DISTINCT FROM EXCLUDED.checksum;
  `;

  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    await client.query(query, params);
    await client.query("COMMIT");
  } catch (erro) {
    await client.query("ROLLBACK");
    throw erro;
  } finally {
    client.release();
  }

  return {
    inseridos: loteValido.length,
    descartados,
    duplicados,
  };
}

async function processarArquivo(filePath) {
  let lote = [];
  let totalProcessado = 0;
  let totalDescartados = 0;
  let totalDuplicados = 0;
  const inicio = Date.now();

  const processarDespesa = async (despesaXml) => {
    const registro = mapearRegistro(despesaXml);
    lote.push(registro);

    if (lote.length < BATCH_SIZE) {
      return;
    }

    const loteAtual = lote;
    lote = [];

    const resultado = await inserirLote(loteAtual);

    totalProcessado += resultado.inseridos;
    totalDescartados += resultado.descartados;
    totalDuplicados += resultado.duplicados;

    console.log(
      `[${filePath}] Processados: ${totalProcessado} | ` +
      `Descartados: ${totalDescartados} | ` +
      `Duplicados: ${totalDuplicados}`
    );
  };

  await readXml(filePath, processarDespesa);

  if (lote.length > 0) {
    const resultadoFinal = await inserirLote(lote);

    totalProcessado += resultadoFinal.inseridos;
    totalDescartados += resultadoFinal.descartados;
    totalDuplicados += resultadoFinal.duplicados;
  }

  const duracaoSegundos = ((Date.now() - inicio) / 1000).toFixed(1);

  console.log(
    `Arquivo ${filePath} finalizado. ` +
    `Total: ${totalProcessado} | ` +
    `Descartados: ${totalDescartados} | ` +
    `Duplicados: ${totalDuplicados} | ` +
    `Tempo: ${duracaoSegundos}s`
  );

  return {
    totalProcessado,
    totalDescartados,
    totalDuplicados,
  };
}

async function main() {
  const arquivos = process.argv.slice(2);

  if (arquivos.length === 0) {
    console.error(
      "Uso: node src/extractor/ingestDespesas.js <arquivo.xml>"
    );
    process.exitCode = 1;
    return;
  }

  let totalGeral = 0;
  let descartadosGeral = 0;
  let duplicadosGeral = 0;

  try {
    for (const arquivo of arquivos) {
      const resultado = await processarArquivo(arquivo);
      totalGeral += resultado.totalProcessado;
      descartadosGeral += resultado.totalDescartados;
      duplicadosGeral += resultado.totalDuplicados;
    }

    console.log(
      `Total geral: ${totalGeral} | ` +
      `Descartados: ${descartadosGeral} | ` +
      `Duplicados: ${duplicadosGeral}`
    );
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  main().catch((erro) => {
    console.error("Erro fatal:", erro);
    process.exitCode = 1;
  });
}

module.exports = {
  mapearRegistro,
  deduplicarPorIdDocumento,
  processarArquivo,
};