#!/usr/bin/env python3
"""
Partitioned Global Deduplication Pipeline.

Strategy: 
- scan_csv reads ALL files of a type at once
- Partition by last digit of cnpj_basico (10 partitions)
- Each partition is deduplicated globally
- COPY directly to Postgres (no conflicts possible)
"""

import gc
import io
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import polars as pl
import psutil
import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@dataclass
class TableConfig:
    """Configuration for a table."""
    table_name: str
    file_pattern: str
    columns: List[str]
    pk_columns: List[str]
    date_columns: List[str]
    partition_column: str = "cnpj_basico"


# Table configurations
TABLE_CONFIGS = {
    "empresas": TableConfig(
        table_name="empresas",
        file_pattern="*EMPRE*",
        columns=[
            "cnpj_basico", "razao_social", "natureza_juridica",
            "qualificacao_responsavel", "capital_social", "porte",
            "ente_federativo_responsavel",
        ],
        pk_columns=["cnpj_basico"],
        date_columns=[],
    ),
    "estabelecimentos": TableConfig(
        table_name="estabelecimentos",
        file_pattern="*ESTABELE*",
        columns=[
            "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
            "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
            "motivo_situacao_cadastral", "nome_cidade_exterior", "pais",
            "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria",
            "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
            "cep", "uf", "municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2",
            "ddd_fax", "fax", "correio_eletronico", "situacao_especial",
            "data_situacao_especial",
        ],
        pk_columns=["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
        date_columns=["data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"],
    ),
    "socios": TableConfig(
        table_name="socios",
        file_pattern="*SOCIO*",
        columns=[
            "cnpj_basico", "identificador_de_socio", "nome_socio", "cnpj_cpf_do_socio",
            "qualificacao_do_socio", "data_entrada_sociedade", "pais",
            "representante_legal", "nome_do_representante",
            "qualificacao_do_representante_legal", "faixa_etaria",
        ],
        pk_columns=["cnpj_basico", "identificador_de_socio", "cnpj_cpf_do_socio"],
        date_columns=["data_entrada_sociedade"],
    ),
    "dados_simples": TableConfig(
        table_name="dados_simples",
        file_pattern="*SIMPLES*",
        columns=[
            "cnpj_basico", "opcao_pelo_simples", "data_opcao_pelo_simples",
            "data_exclusao_do_simples", "opcao_pelo_mei", "data_opcao_pelo_mei",
            "data_exclusao_do_mei",
        ],
        pk_columns=["cnpj_basico"],
        date_columns=["data_opcao_pelo_simples", "data_exclusao_do_simples", 
                      "data_opcao_pelo_mei", "data_exclusao_do_mei"],
    ),
}


class ResourceMonitor:
    """Monitor CPU and memory usage."""
    
    def __init__(self):
        self.samples = []
        self.start_time = None
        
    def sample(self):
        """Take a sample of current resource usage."""
        self.samples.append({
            "cpu": psutil.cpu_percent(),
            "memory_mb": psutil.Process().memory_info().rss / 1024 / 1024,
            "time": time.time(),
        })
    
    def start(self):
        self.start_time = time.time()
        self.samples = []
        self.sample()
    
    def stop(self):
        self.sample()
        duration = time.time() - self.start_time
        
        if not self.samples:
            return {}
            
        cpu_values = [s["cpu"] for s in self.samples]
        mem_values = [s["memory_mb"] for s in self.samples]
        
        return {
            "duration_s": round(duration, 1),
            "cpu_avg": round(sum(cpu_values) / len(cpu_values), 1),
            "cpu_max": round(max(cpu_values), 1),
            "mem_avg_mb": round(sum(mem_values) / len(mem_values), 1),
            "mem_max_mb": round(max(mem_values), 1),
        }


def log_resources(table: str, partition: str, stats: dict):
    """Log resource usage."""
    logger.info(
        f"[{table}] Partition {partition} | "
        f"Duration: {stats.get('duration_s', 0)}s | "
        f"CPU: avg={stats.get('cpu_avg', 0)}%, max={stats.get('cpu_max', 0)}% | "
        f"MEM: avg={stats.get('mem_avg_mb', 0)}MB, max={stats.get('mem_max_mb', 0)}MB"
    )


def transform_empresas(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Transform empresas data."""
    return lf.with_columns(
        pl.col("capital_social")
        .str.replace_all(r"\.", "")
        .str.replace(",", ".")
        .cast(pl.Float64, strict=False)
    )


def transform_estabelecimentos(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Transform estabelecimentos data."""
    # Pad pais code
    lf = lf.with_columns(pl.col("pais").str.zfill(3))
    
    # Handle invalid dates
    for date_col in ["data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"]:
        lf = lf.with_columns(
            pl.when(pl.col(date_col).is_in(["0", "00000000", ""]))
            .then(None)
            .otherwise(pl.col(date_col))
            .alias(date_col)
        )
    
    return lf


def transform_socios(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Transform socios data."""
    # Ensure cnpj_cpf_do_socio is not null (part of PK)
    lf = lf.with_columns(
        pl.col("cnpj_cpf_do_socio").fill_null("00000000000000")
    )
    
    # Handle invalid dates
    lf = lf.with_columns(
        pl.when(pl.col("data_entrada_sociedade").is_in(["0", "00000000", ""]))
        .then(None)
        .otherwise(pl.col("data_entrada_sociedade"))
        .alias("data_entrada_sociedade")
    )
    
    return lf


def transform_dados_simples(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Transform dados_simples data."""
    date_cols = ["data_opcao_pelo_simples", "data_exclusao_do_simples",
                 "data_opcao_pelo_mei", "data_exclusao_do_mei"]
    
    for date_col in date_cols:
        lf = lf.with_columns(
            pl.when(pl.col(date_col).is_in(["0", "00000000", ""]))
            .then(None)
            .otherwise(pl.col(date_col))
            .alias(date_col)
        )
    
    return lf


TRANSFORMERS = {
    "empresas": transform_empresas,
    "estabelecimentos": transform_estabelecimentos,
    "socios": transform_socios,
    "dados_simples": transform_dados_simples,
}


def ingest_table_partitioned(
    config: TableConfig,
    data_dir: Path,
    database_url: str,
    num_partitions: int = 10,
    chunk_size: int = 200_000,
):
    """
    Ingest a table using partitioned global deduplication.
    
    Strategy:
    - scan_csv reads ALL files matching pattern
    - For each partition (0-9), filter by last digit of cnpj_basico
    - Deduplicate globally within partition
    - COPY to Postgres
    """
    logger.info(f"═" * 60)
    logger.info(f"Starting ingestion: {config.table_name}")
    logger.info(f"═" * 60)
    
    # Find all matching files
    csv_files = list(data_dir.glob(config.file_pattern))
    if not csv_files:
        logger.warning(f"No files found for pattern: {config.file_pattern}")
        return
    
    logger.info(f"Found {len(csv_files)} files matching {config.file_pattern}")
    for f in csv_files:
        logger.info(f"  - {f.name}")
    
    # Create lazy frame scanning ALL files
    base_lf = pl.scan_csv(
        csv_files,
        separator=";",
        has_header=False,
        new_columns=config.columns,
        encoding="utf8-lossy",
        infer_schema_length=0,
        null_values=[""],
        ignore_errors=True,
        low_memory=True,
    )
    
    # Apply transformations
    if config.table_name in TRANSFORMERS:
        base_lf = TRANSFORMERS[config.table_name](base_lf)
    
    # Connect to database
    conn = psycopg.connect(database_url, autocommit=False)
    
    try:
        with conn.cursor() as cur:
            # Disable logging for faster writes
            cur.execute(f"ALTER TABLE {config.table_name} SET UNLOGGED")
            conn.commit()
        
        total_rows = 0
        table_start = time.time()
        
        # Process each partition
        for partition in range(num_partitions):
            gc.collect()  # Force cleanup before starting new partition
            suffix = str(partition).zfill(len(str(num_partitions-1)))
            monitor = ResourceMonitor()
            monitor.start()
            
            logger.info(f"[{config.table_name}] Processing partition {partition}/{num_partitions-1}...")
            
            # Filter by hash of partition column (more robust for many partitions)
            partition_lf = base_lf.filter(
                (pl.col(config.partition_column).hash() % num_partitions) == partition
            )
            
            # Global deduplication within this partition
            partition_lf = partition_lf.unique(
                subset=config.pk_columns,
                maintain_order=False,
                keep="first",
            )
            
            # Collect with streaming engine
            try:
                df = partition_lf.collect(engine="streaming")
            except Exception as e:
                logger.error(f"Error collecting partition {partition}: {e}")
                # Fallback: default engine
                df = partition_lf.collect()
            
            monitor.sample()
            
            if df.is_empty():
                logger.info(f"[{config.table_name}] Partition {partition}: 0 rows (empty)")
                continue
            
            rows_in_partition = len(df)
            total_rows += rows_in_partition
            
            # COPY to Postgres
            columns_str = ", ".join([f'"{col}"' for col in config.columns])
            
            with conn.cursor() as cur:
                # Prepare CSV bytes
                csv_buffer = io.BytesIO()
                df.write_csv(csv_buffer, include_header=False)
                csv_bytes = csv_buffer.getvalue()
                
                # Remove null bytes
                if b"\x00" in csv_bytes:
                    csv_bytes = csv_bytes.replace(b"\x00", b"")
                
                # COPY
                with cur.copy(
                    f'COPY {config.table_name} ({columns_str}) FROM STDIN WITH CSV ENCODING \'UTF8\''
                ) as copy:
                    copy.write(csv_bytes)
                
                conn.commit()
            
            stats = monitor.stop()
            stats["rows"] = rows_in_partition
            log_resources(config.table_name, f"{partition}/{num_partitions-1}", stats)
            
            # Free memory
            del df
        
        # Re-enable logging
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE {config.table_name} SET LOGGED")
            conn.commit()
        
        table_duration = time.time() - table_start
        logger.info(f"═" * 60)
        logger.info(f"✓ {config.table_name} completed: {total_rows:,} rows in {table_duration:.1f}s")
        logger.info(f"═" * 60)
        
    except Exception as e:
        logger.error(f"Error ingesting {config.table_name}: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    """Run partitioned global deduplication for all main tables."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Partitioned Global Deduplication Pipeline")
    parser.add_argument("--data-dir", default="temp", help="Directory containing CSV files")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/cnpj"))
    parser.add_argument("--tables", nargs="+", default=["empresas", "estabelecimentos", "socios", "dados_simples"])
    parser.add_argument("--partitions", type=int, default=40, help="Number of partitions (default: 40)")
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error(f"Data directory does not exist: {data_dir}")
        return
    
    start_time = time.time()
    
    for table_name in args.tables:
        if table_name not in TABLE_CONFIGS:
            logger.warning(f"Unknown table: {table_name}, skipping")
            continue
        
        config = TABLE_CONFIGS[table_name]
        ingest_table_partitioned(
            config=config,
            data_dir=data_dir,
            database_url=args.database_url,
            num_partitions=args.partitions,
        )
    
    total_time = time.time() - start_time
    logger.info(f"")
    logger.info(f"═" * 60)
    logger.info(f"✓✓✓ ALL TABLES COMPLETED IN {total_time:.1f}s ({total_time/60:.1f} min)")
    logger.info(f"═" * 60)


if __name__ == "__main__":
    main()
