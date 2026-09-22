# Desafio Big Data 1 — Pipeline de Dados da Câmara dos Deputados

## 📌 Sobre o projeto

Este projeto está sendo desenvolvido como parte de um desafio acadêmico voltado para o processamento e tratamento de grandes volumes de dados.

A proposta utiliza como fonte os **Dados Abertos da Câmara dos Deputados**, que disponibilizam informações públicas relacionadas às atividades parlamentares.

Para o desenvolvimento da solução, será utilizado como ponto de partida um conjunto de dados de despesas parlamentares disponibilizado em formato XML.

O objetivo é projetar uma arquitetura capaz de receber, armazenar, processar e disponibilizar esses dados de forma organizada, escalável e com possibilidade de processamento contínuo.

---

## 🎯 Problema

O crescimento do volume de dados torna necessário o desenvolvimento de soluções capazes de processar grandes quantidades de informações sem depender de um único processo ou componente.

Neste desafio, o cenário proposto envolve o processamento de até **600 milhões de registros**, tornando necessário pensar em aspectos como:

- processamento distribuído;
- escalabilidade;
- baixa latência;
- controle do processamento;
- rastreabilidade dos dados;
- qualidade e organização das informações.

A solução deve permitir que os dados sejam processados de maneira eficiente e que os resultados possam ser utilizados posteriormente para análises e geração de informações.

---

## 💡 Proposta da solução

A ideia é construir um pipeline de dados no qual as informações passem por diferentes etapas.

De forma simplificada:

**Dados Abertos → Armazenamento → Distribuição → Processamento → Dados Tratados → Visualização**

A arquitetura planejada utilizará um banco de dados para armazenamento, uma plataforma de mensageria para distribuição dos registros e consumidores responsáveis pelo processamento.

Essa abordagem permite dividir o trabalho entre diferentes processos e possibilita aumentar a capacidade de processamento conforme o volume de dados cresce.

---

## 🏛️ Fonte dos dados

Os dados utilizados são provenientes do portal de **Dados Abertos da Câmara dos Deputados**.

Fonte:

https://github.com/CamaraDosDeputados/dados-abertos

O conjunto escolhido contém informações relacionadas às despesas parlamentares, incluindo dados como:

- parlamentar;
- partido;
- estado;
- fornecedor;
- data da despesa;
- descrição da despesa;
- valor do documento;
- valor da glosa;
- valor líquido;
- identificação do documento.

Essas informações serão utilizadas para construir e testar o pipeline proposto.

---

## 🔄 Fluxo planejado

A arquitetura está sendo planejada seguindo o seguinte fluxo:

```text
             Dados Abertos
                  │
                  ▼
             Banco de Dados
                  │
                  ▼
           Kafka Connect
                  │
                  ▼
                Kafka
                  │
          ┌───────┼───────┐
          ▼       ▼       ▼
     Consumer  Consumer  Consumer
          │       │       │
          └───────┼───────┘
                  ▼
           Dados Tratados
                  │
                  ▼
             Visualização

Dependências:
instalar sax, pg e dotenv
npm install sax pg dotenv

Comando para teste:
node ingestDespesas.js caminho/para/Ano-2023.xml

(antes de utilizar o comando de teste, conferir se as tags em despesaRepository.js batem com as do XML de teste).