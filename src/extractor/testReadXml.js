const readXml = require("./readXml");

const arquivo = process.argv[2];

//Arquivo apenas para testes de XML

if (!arquivo) {
  console.error("Uso: node testReadXml.js caminho/arquivo.xml");
  process.exit(1);
}

let total = 0;

readXml(arquivo, (despesa) => {
  total += 1;

  if (total <= 3) {
    console.log(JSON.stringify(despesa, null, 2));
  }

  if (total % 10000 === 0) {
    console.log(`Registros lidos: ${total}`);
  }
})
  .then(() => {
    console.log(`Leitura concluída. Total: ${total}`);
  })
  .catch((erro) => {
    console.error("Erro ao ler XML:", erro);
    process.exit(1);
  });