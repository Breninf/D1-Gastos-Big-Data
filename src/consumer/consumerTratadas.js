require("dotenv").config();

const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const kafka = new Kafka({
  clientId: "camara-consumer",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "camara-tratadas-consumer-v1",
});

const pool = new Pool({
  host: process.env.POSTGRES_HOST || "localhost",
  port: Number(process.env.POSTGRES_PORT || 5432),
  database: process.env.POSTGRES_DB || "camara",
  user: process.env.POSTGRES_USER || "postgres",
  password: process.env.POSTGRES_PASSWORD || "postgres",
});

const TOPIC = process.env.KAFKA_TOPIC || "camara.public.despesas_raw";

function extrairEvento(message) {
  if (!message.value) return null;

  const evento = JSON.parse(message.value.toString());
  return evento.payload || evento;
}

function registroDoEvento(evento) {
  if (evento.op === "d") return evento.before;
  return evento.after;
}

function normalizarTimestamp(valor) {
  if (valor === null || valor === undefined || valor === "") {
    return null;
  }

  let data;

  if (typeof valor === "number") {
    data = new Date(valor);
  } else {
    const texto = String(valor).trim();

    if (/^\d+$/.test(texto)) {
      data = new Date(Number(texto));
    } else {
      data = new Date(texto);
    }
  }

  if (Number.isNaN(data.getTime())) {
    return null;
  }

  return data;
}

async function aplicarEvento(evento) {
  const registro = registroDoEvento(evento);
  if (!registro || registro.id_documento === null || registro.id_documento === undefined) {
    return "ignorado";
  }

  if (evento.op === "d") {
    await pool.query(
      "DELETE FROM despesas_tratadas WHERE id_documento = $1",
      [registro.id_documento]
    );
    return "deletado";
  }

  const query = `
    INSERT INTO despesas_tratadas (
      id_documento,
      id_deputado,
      nome_parlamentar,
      sigla_uf,
      sigla_partido,
      descricao,
      fornecedor,
      cnpj_cpf,
      numero_documento,
      data_emissao,
      valor_documento,
      valor_glosa,
      valor_liquido,
      mes,
      ano,
      url_documento,
      operacao_cdc,
      origem_ts_ms,
      processado_at,
      atualizado_at
    ) VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
      $11, $12, $13, $14, $15, $16, $17, $18, now(), now()
    )
    ON CONFLICT (id_documento)
    DO UPDATE SET
      id_deputado = EXCLUDED.id_deputado,
      nome_parlamentar = EXCLUDED.nome_parlamentar,
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
      operacao_cdc = EXCLUDED.operacao_cdc,
      origem_ts_ms = EXCLUDED.origem_ts_ms,
      atualizado_at = now();
  `;

  const valores = [
    registro.id_documento,
    registro.id_deputado,
    registro.nome_parlamentar,
    registro.sigla_uf,
    registro.sigla_partido,
    registro.descricao,
    registro.fornecedor,
    registro.cnpj_cpf,
    registro.numero_documento,
    normalizarTimestamp(registro.data_emissao),
    registro.valor_documento,
    registro.valor_glosa,
    registro.valor_liquido,
    registro.mes,
    registro.ano,
    registro.url_documento,
    evento.op,
    evento.ts_ms || evento.source?.ts_ms || null,
  ];

  await pool.query(query, valores);
  return evento.op === "r" ? "snapshot" : "gravado";
}

async function main() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log(`Consumindo tópico: ${TOPIC}`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const evento = extrairEvento(message);
        if (!evento) return;

        const resultado = await aplicarEvento(evento);
        console.log(`offset=${message.offset} op=${evento.op} resultado=${resultado}`);
      } catch (erro) {
        console.error("Erro ao processar evento:", erro.message);
      }
    },
  });
}

async function encerrar() {
  await consumer.disconnect();
  await pool.end();
}

process.on("SIGINT", async () => {
  await encerrar();
  process.exit(0);
});

main().catch(async (erro) => {
  console.error("Erro fatal no consumer:", erro);
  await encerrar();
  process.exit(1);
});
