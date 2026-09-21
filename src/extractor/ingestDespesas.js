require("dotenv").config();

const readXml = require("./readXml");
const { inserirLote, encerrarPool } = require("./despesaRepository");

const BATCH_SIZE = 5000;

/*é o orquestrador: acumula despesas em um buffer de 5.000 registros, 
e quando o buffer enche, pausa o stream do arquivo, insere o lote no Postgres, 
e só então retoma a leitura. É exatamente esse ciclo de pause/insert/resume que 
impede o processo de estourar memória ao lidar com 600M de registros — sem isso, 
o Node continuaria lendo o arquivo mais rápido do que o banco consegue gravar. */

/**
 * Orquestra a leitura em streaming do XML + carga em lote no Postgres,
 * com backpressure: a leitura do arquivo é pausada enquanto o lote atual
 * está sendo inserido no banco, e só é retomada depois.
 */
async function ingestarArquivo(caminhoArquivo) {
  let buffer = [];
  let totalProcessado = 0;
  let totalComErro = 0;
  const inicio = Date.now();

  await readXml(caminhoArquivo, (despesa, control) => {
    buffer.push(despesa);

    if (buffer.length >= BATCH_SIZE) {
      const lote = buffer;
      buffer = [];

      control.pause();

      inserirLote(lote)
        .then((inseridos) => {
          totalProcessado += inseridos;
          totalComErro += lote.length - inseridos;

          const segundos = ((Date.now() - inicio) / 1000).toFixed(1);
          console.log(
            `[${caminhoArquivo}] ${totalProcessado} registros processados ` +
              `(${totalComErro} descartados) em ${segundos}s`
          );
        })
        .catch((erro) => {
          console.error("Erro ao inserir lote:", erro);
        })
        .finally(() => {
          control.resume();
        });
    }
  });

  // insere o que restou no buffer após o fim do arquivo
  if (buffer.length > 0) {
    const inseridos = await inserirLote(buffer);
    totalProcessado += inseridos;
  }

  const segundosTotal = ((Date.now() - inicio) / 1000).toFixed(1);
  console.log(
    `[${caminhoArquivo}] Concluído: ${totalProcessado} registros em ${segundosTotal}s ` +
      `(${totalComErro} descartados por falta de id_documento)`
  );

  return totalProcessado;
}

async function main() {
  const arquivos = process.argv.slice(2);

  if (arquivos.length === 0) {
    console.error("Uso: node ingestDespesas.js <arquivo1.xml> [arquivo2.xml ...]");
    process.exit(1);
  }

  let totalGeral = 0;

  for (const arquivo of arquivos) {
    totalGeral += await ingestarArquivo(arquivo);
  }

  console.log(`Total geral processado: ${totalGeral}`);
  await encerrarPool();
}

main().catch((erro) => {
  console.error("Falha na ingestão:", erro);
  process.exit(1);
});