# CNPJ Data Pipeline Parallel (Performance Research)

Este projeto é uma **pesquisa de performance e otimização** baseada no pipeline original de dados do CNPJ: [caiopizzol/cnpj-data-pipeline](https://github.com/caiopizzol/cnpj-data-pipeline).

---

## 🎖️ Créditos e Reconhecimento

Todo o mérito de **processamento, limpeza inicial, preparação dos dados, relações entre tabelas e definição de índices** pertence ao projeto original de [Caio Pizzol](https://github.com/caiopizzol). Este projeto foca exclusivamente em levar essa lógica ao limite da performance através de paralelismo massivo e tuning de recursos (PostgreSQL/Docker/Python).

---

## 🚀 Performance: 32 Minutos

O objetivo desta pesquisa foi reduzir o tempo de processamento total para menos de 40 minutos em hardware doméstico (32GB RAM / 16 Cores / NVMe). 

### Destaques das Otimizações:
- **Polars Massivo**: Uso intensivo da biblioteca Polars para leitura batcheada e paralela de CSVs.
- **Psycopg3 COPY**: Migração para o protocolo de Bulk Load mais rápido disponível para PostgreSQL.
- **Adaptive Resource Sizing**: O pipeline detecta a RAM disponível e ajusta automaticamente o número de workers e memória de sessão (`BALANCED-HIGH-PERF`).
- **Postgres Tuning**: Configurações agressivas de `shared_buffers`, `maintenance_work_mem` e uso de tabelas `UNLOGGED` para evitar gargalos de I/O.
- **Resiliência a Dados Sujos**: Implementação de filtros de truncagem e limpeza automática para lidar com aspas malformadas e campos gigantes nos arquivos da Receita Federal.

---

## 📋 Instruções de Execução

Para rodar o pipeline com a configuração otimizada:

1. **Configuração inicial**:
   ```bash
   cp .env.example .env
   ```

2. **Limpeza e Execução**:
   (Recomendado limpar volumes antigos para garantir a aplicação das novas otimizações)
   ```bash
   docker compose down -v
   docker compose up --build
   ```

---

## 🛤️ Próximos Passos (Backlog)

- [ ] **Integração com Download**: Acoplar a esteira de download resiliente ao processamento paralelo.
- [ ] **DuckDB Pipeline**: Pesquisa de performance utilizando DuckDB como alternativa/complemento ao PostgreSQL.
- [ ] **Refinamento de Código**: Revisão modular para garantir manutenibilidade a longo prazo.
- [ ] **Zstandard Cache**: Implementação de compressão Zstd para aceleração de leitura em I/O intensivo.

