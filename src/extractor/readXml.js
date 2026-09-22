const fs = require("fs");
const sax = require("sax");

/**
 * Lê um arquivo XML em streaming e chama onDespesa(despesa, control) para
 * cada tag <despesa> encontrada.
 *
 * `control.pause()` / `control.resume()` permitem que o consumidor (quem
 * grava no banco) pause a leitura do arquivo enquanto processa um lote,
 * evitando que 600M de registros se acumulem em memória (backpressure).
 */
function readXml(filePath, onDespesa) {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath, {
      encoding: "utf8",
    });

    const parser = sax.createStream(true, {
      trim: true,
    });

    let currentDespesa = null;
    let currentElement = null;
    let currentValue = "";

    const control = {
      pause: () => stream.pause(),
      resume: () => stream.resume(),
    };

    parser.on("opentag", (node) => {
      const tagName = node.name;

      if (tagName === "despesa") {
        currentDespesa = {};
      }

      currentElement = tagName;
      currentValue = "";
    });

    parser.on("text", (text) => {
      currentValue += text;
    });

    parser.on("closetag", (tagName) => {
  if (!currentDespesa) {
    return;
  }

  if (tagName === "despesa") {
    const despesaCompleta = currentDespesa;
    currentDespesa = null;
    currentElement = null;
    currentValue = "";

    stream.pause();
    Promise.resolve(onDespesa(despesaCompleta))
      .then(() => stream.resume())
      .catch((error) => {
        stream.destroy(error);
      });

    return;
  }

  if (currentElement === tagName) {
    currentDespesa[tagName] = currentValue;
  }

  currentElement = null;
  currentValue = "";
});

    parser.on("error", (error) => {
      reject(error);
    });

    parser.on("end", () => {
      resolve();
    });

    stream.on("error", (error) => {
      reject(error);
    });

    stream.pipe(parser);
  });
}

module.exports = readXml;