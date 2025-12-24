# Benchmark: Teste com Temp Tables + ON CONFLICT

**Data:** 2025-12-23  
**Início:** 19:07:33  
**Fim:** 20:14:25  
**Tempo Total:** **67 minutos (1h 7min)**

## Resultado: MUITO RUIM ❌

### Código Utilizado

**Método de Ingestão:** `bulk_upsert` com temp tables + ON CONFLICT

#### database.py
```python
def bulk_upsert(self, df: pl.DataFrame, table_name: str, columns: List[str]):
    """Bulk upsert using temp table + COPY + ON CONFLICT."""
    temp_table = f"temp_{table_name}_{id(df)}"
    
    # 1. CREATE TEMP TABLE (LIKE table)
    # 2. COPY to temp
    # 3. INSERT INTO table SELECT FROM temp ON CONFLICT DO NOTHING
    # 4. DROP temp (on commit)
```

**Polars:** `unique()` por batch (500k linhas)  
**Workers:** 2

---

## Análise Detalhada

### Arquivos Mais Lentos

| Arquivo | Linhas | Tempo | Linhas/seg | CPU Avg |
|---------|--------|-------|------------|---------|
| **Estabelecimentos0.zip** | 26,098,579 | **1,242s (20.7 min)** | 21,012 | 22.1% |
| **Simples.zip** | 46,180,709 | **999.8s (16.7 min)** | 46,191 | 11.4% |
| **Empresas0.zip** | 25,243,134 | **543.5s (9.1 min)** | 46,440 | 14.5% |
| Socios0.zip | 8,632,739 | 323.3s (5.4 min) | 26,710 | 11.6% |

### Problemas Críticos

#### 1. **CPU Extremamente Baixa (11-22%)**
- Postgres esperando I/O na maior parte do tempo
- 2 workers não estão saturando CPU
- Gargalo é DISCO, não CPU

#### 2. **Checkpoints Frequentes**
```
19:13:55 - checkpoint starting: wal
19:19:29 - checkpoint complete (333s)
19:20:14 - checkpoint starting: wal
19:25:55 - checkpoint complete (340s)
```

**Cada checkpoint leva 5-6 minutos!** Durante checkpoint, I/O é bloqueado.

#### 3. **Overhead de Temp Tables**

Para **cada batch de 500k linhas**:
1. CREATE TEMP TABLE (~100ms)
2. COPY to temp (~1s)
3. INSERT SELECT ON CONFLICT (~2-5s) ← **gargalo principal**
4. DROP temp (~50ms)

**Simples.zip**: 46M linhas ÷ 500k = 92 batches × ~5s = **460 segundos só de overhead!**

#### 4. **Memória Alta**
- Estabelecimentos0: **7.8GB pico**
- Simples: **3.2GB pico**

Polars + Postgres competindo por RAM.

---

## Comparação com Código Novo (Não Testado Ainda)

| Aspecto | Código Antigo (Testado) | Código Novo (Implementado) |
|---------|------------------------|---------------------------|
| Método | Temp table + ON CONFLICT | COPY direto |
| Overhead por batch | ~5s | ~0.5s |
| Tempo estimado Simples.zip | 999s (16.7 min) | ~200s (3.3 min) |
| Tempo total estimado | **67 min** | **~20-25 min** |

---

## Por Que Está Tão Lento?

### O Ciclo do Temp Table

```
Para CADA batch (92 batches no Simples.zip):
┌─────────────────────────────────────┐
│ 1. CREATE TEMP TABLE (fsync)       │ ← I/O
│ 2. COPY to temp (write WAL)        │ ← I/O
│ 3. INSERT SELECT (read temp + write)│ ← I/O
│ 4. UPDATE timestamps               │ ← I/O
│ 5. COMMIT (fsync WAL)              │ ← I/O
│ 6. DROP temp (cleanup)             │ ← I/O
└─────────────────────────────────────┘
Total: ~5-6 segundos de I/O por batch
```

**Problemas:**
- 6 operações I/O por batch
- fsync a cada commit
- Checkpoints pausam tudo
- WAL cresce muito rápido

---

## Conclusão

**Este método é estruturalmente lento.** Não há como otimizar para < 30 minutos com temp tables.

**Próximo teste:** COPY direto sem temp tables (código já implementado).

**Expectativa:** ~20-25 minutos totais.
