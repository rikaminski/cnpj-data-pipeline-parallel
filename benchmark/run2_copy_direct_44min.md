# Benchmark: COPY Direto (Teste 2)

**Data:** 2025-12-23  
**Início:** 20:45 (estimado)  
**Fim:** 21:15:34  
**Tempo até erro:** **~44 minutos**

## Resultado: Melhor, mas com erro no final ⚠️

### Código Utilizado

**Método de Ingestão:** `bulk_load` - COPY direto (sem temp tables)

#### database.py
```python
def bulk_load(self, df: pl.DataFrame, table_name: str, columns: List[str]):
    """Direct COPY to table (fast, no temp table, no upsert)."""
    # Write CSV to BytesIO
    # COPY directly to table
    # commit
```

**Polars:** `unique()` por batch (500k linhas)  
**Workers:** 2  
**PKs:** Removidas durante carga, criadas depois

---

## Performance

### Tempo por Fase

| Fase | Tempo | Status |
|------|-------|--------|
| Data load (COPY direto) | ~29 min | ✅ |
| Create indexes + dedup | ~15 min | ✅ |
| VACUUM ANALYZE | - | ❌ Erro |
| **Total** | **~44 min** | ⚠️ |

### Comparação com Teste 1

| Métrica | Teste 1 (Temp Tables) | Teste 2 (COPY Direto) | Melhoria |
|---------|----------------------|---------------------|----------|
| Data load | ~57 min | ~29 min | **-49%** |
| Total | 67 min | 44 min | **-34%** |

---

## Erro Encontrado

```
VACUUM cannot run inside a transaction block
```

**Causa:** `create_indexes.sql` tem `VACUUM ANALYZE` no final, mas foi executado dentro de uma transação Python.

**Solução aplicada:** Mover VACUUM para código Python com `autocommit=True`.

---

## Análise

### Pontos Positivos ✅
1. COPY direto é **49% mais rápido** que temp tables
2. CPU usage melhor (~20-30% vs 11-22%)
3. Código muito mais simples

### Problemas ⚠️
1. VACUUM falhou (já corrigido)
2. Não sabemos se houve duplicatas (logs não mostram RAISE NOTICE)

---

## Próximo Teste

Com a correção do VACUUM, expectativa: **~45-48 min** totais.

**Ainda não está nos 30 min desejados**, mas é progresso.
