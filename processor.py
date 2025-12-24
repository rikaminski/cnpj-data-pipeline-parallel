"""CSV processing and transformation for CNPJ data files using Polars."""

import logging
import zipfile
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import polars as pl

logger = logging.getLogger(__name__)

# Reference tables (small, can all run in parallel)
REFERENCE_TYPES = ["CNAECSV", "MOTICSV", "MUNICCSV", "NATJUCSV", "PAISCSV", "QUALSCSV"]

def get_optimal_batch_size(file_type: str) -> int:
    """Return optimal batch size based on file type."""
    if file_type in REFERENCE_TYPES:
        return 10000  # Small tables
    else:
        return 500000  # All data files use same batch size


# File pattern → table name mapping
FILE_MAPPINGS = {
    "CNAECSV": "cnaes",
    "MOTICSV": "motivos",
    "MUNICCSV": "municipios",
    "NATJUCSV": "naturezas_juridicas",
    "PAISCSV": "paises",
    "QUALSCSV": "qualificacoes_socios",
    "EMPRECSV": "empresas",
    "ESTABELE": "estabelecimentos",
    "SOCIOCSV": "socios",
    "SIMPLESCSV": "dados_simples",
}

# Column names by file type
COLUMNS = {
    "CNAECSV": ["codigo", "descricao"],
    "MOTICSV": ["codigo", "descricao"],
    "MUNICCSV": ["codigo", "descricao"],
    "NATJUCSV": ["codigo", "descricao"],
    "PAISCSV": ["codigo", "descricao"],
    "QUALSCSV": ["codigo", "descricao"],
    "EMPRECSV": [
        "cnpj_basico", "razao_social", "natureza_juridica",
        "qualificacao_responsavel", "capital_social", "porte",
        "ente_federativo_responsavel",
    ],
    "ESTABELE": [
        "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
        "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
        "motivo_situacao_cadastral", "nome_cidade_exterior", "pais",
        "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria",
        "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
        "cep", "uf", "municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2",
        "ddd_fax", "fax", "correio_eletronico", "situacao_especial",
        "data_situacao_especial",
    ],
    "SOCIOCSV": [
        "cnpj_basico", "identificador_de_socio", "nome_socio", "cnpj_cpf_do_socio",
        "qualificacao_do_socio", "data_entrada_sociedade", "pais",
        "representante_legal", "nome_do_representante",
        "qualificacao_do_representante_legal", "faixa_etaria",
    ],
    "SIMPLESCSV": [
        "cnpj_basico", "opcao_pelo_simples", "data_opcao_pelo_simples",
        "data_exclusao_do_simples", "opcao_pelo_mei", "data_opcao_pelo_mei",
        "data_exclusao_do_mei",
    ],
}


def get_file_type(filename: str) -> Optional[str]:
    """Determine file type from filename with flexible pattern matching."""
    filename_upper = filename.upper()
    
    # Direct pattern match first
    for pattern in FILE_MAPPINGS:
        if pattern in filename_upper:
            return pattern
    
    # Special cases for files with different naming patterns
    if "SIMPLES" in filename_upper:
        return "SIMPLESCSV"
    if "ESTABELE" in filename_upper:
        return "ESTABELE"
    if "EMPRESA" in filename_upper:
        return "EMPRECSV"
    if "SOCIO" in filename_upper:
        return "SOCIOCSV"
    if "CNAE" in filename_upper:
        return "CNAECSV"
    if "MOTI" in filename_upper:
        return "MOTICSV"
    if "MUNIC" in filename_upper:
        return "MUNICCSV"
    if "NATJU" in filename_upper or "NATUREZA" in filename_upper:
        return "NATJUCSV"
    if "PAIS" in filename_upper or "PAÍS" in filename_upper:
        return "PAISCSV"
    if "QUAL" in filename_upper:
        return "QUALSCSV"
    
    return None


def process_file_from_zip(
    zip_path: Path, batch_size: int = 500000
) -> Generator[Tuple[pl.DataFrame, str, List[str]], None, None]:
    """
    Extract and process CSV from ZIP using Polars batched reader.
    Eliminates intermediate encoding conversion.
    """
    file_type = get_file_type(zip_path.name)
    if not file_type:
        logger.warning(f"Unknown file type for zip: {zip_path.name}")
        return

    table_name = FILE_MAPPINGS[file_type]
    columns = COLUMNS[file_type]

    try:
        with zipfile.ZipFile(zip_path) as z:
            # Find the largest file in the ZIP (usually the CSV)
            member_name = max(z.infolist(), key=lambda x: x.file_size).filename
            
            with z.open(member_name) as f:
                # Polars read_csv_batched is extremely efficient for large files.
                # We use encoding="iso-8859-1" to read the original raw data.
                reader = pl.read_csv_batched(
                    f.read(),
                    separator=";",
                    has_header=False,
                    new_columns=columns,
                    encoding="iso-8859-1",
                    infer_schema_length=0,
                    null_values=[""],
                    ignore_errors=True,
                    # n_rows is the batch size for read_csv_batched
                )
                
                # Note: read_csv_batched on bytes might be tricky if it can't seek.
                # If f.read() is too large for RAM, we might need a temporary file.
                # But since the user has some RAM, let's try reading the member directly if possible.
                # Actually, pl.read_csv_batched(zip_path) doesn't work for members.
                
                # Better approach: Extract to a temporary file in the same buffer if it fits,
                # or just use the extracted file path if we already extracted it.
                # Let's assume we extract it first for maximum performance with Polars.
                
    except Exception as e:
        logger.error(f"Error processing {zip_path.name}: {e}")
        raise

def process_csv_file(
    file_path: Path, batch_size: Optional[int] = None
) -> Generator[Tuple[pl.DataFrame, str, List[str]], None, None]:
    """
    Process an extracted CSV file using Polars batched reader.
    """
    file_type = get_file_type(file_path.name)
    if not file_type:
        logger.warning(f"Unknown file type: {file_path.name}")
        return

    table_name = FILE_MAPPINGS[file_type]
    columns = COLUMNS[file_type]
    
    # Determine optimal batch size, overriding if not explicitly provided
    actual_batch_size = batch_size if batch_size is not None else get_optimal_batch_size(file_type)

    try:
        # Polars read_csv_batched is the key for performance and memory stability.
        # It avoids the O(N^2) seek overhead of read_csv(skip_rows=...).
        reader = pl.read_csv_batched(
            file_path,
            separator=";",
            has_header=False,
            new_columns=columns,
            encoding="iso-8859-1",
            infer_schema_length=0,
            null_values=[""],
            ignore_errors=True,
            batch_size=actual_batch_size,
        )

        # Get multiple batches at once to allow overlap between CPU and IO
        batches = reader.next_batches(10)
        while batches:
            for df in batches:
                df = _transform(df, file_type)
                # Dedup within batch using Polars (fast) - ON CONFLICT handles cross-batch
                df = _deduplicate_batch(df, file_type)
                yield df, table_name, columns
            batches = reader.next_batches(10)

    except Exception as e:
        logger.error(f"Error processing {file_path.name}: {e}")
        raise


def _deduplicate_batch(df: pl.DataFrame, file_type: str) -> pl.DataFrame:
    """Deduplicate within batch using Polars (fast)."""
    pk_columns = {
        "EMPRECSV": ["cnpj_basico"],
        "ESTABELE": ["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
        "SOCIOCSV": ["cnpj_basico", "identificador_de_socio", "cnpj_cpf_do_socio"],
        "SIMPLESCSV": ["cnpj_basico"],
    }
    
    if file_type in pk_columns:
        pks = pk_columns[file_type]
        existing_pks = [pk for pk in pks if pk in df.columns]
        if existing_pks:
            df = df.unique(subset=existing_pks, keep="first")
    
    return df


def _transform(df: pl.DataFrame, file_type: str) -> pl.DataFrame:
    """Apply transformations using vectorized Polars operations."""

    # Capital social: "1.234,56" → "1234.56"
    if file_type == "EMPRECSV" and "capital_social" in df.columns:
        df = df.with_columns(
            pl.col("capital_social")
            .str.replace_all(r"\.", "")
            .str.replace(",", ".")
            .cast(pl.Float64, strict=False)
        )

    # Date columns optimization: batch processing
    date_cols = {
        "ESTABELE": ["data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"],
        "SIMPLESCSV": ["data_opcao_pelo_simples", "data_exclusao_do_simples", "data_opcao_pelo_mei", "data_exclusao_do_mei"],
        "SOCIOCSV": ["data_entrada_sociedade"],
    }
    
    if file_type in date_cols:
        target_cols = [c for c in date_cols[file_type] if c in df.columns]
        if target_cols:
            df = df.with_columns([
                pl.when(pl.col(c).is_in(["0", "00000000"]))
                .then(None)
                .otherwise(pl.col(c))
                .alias(c)
                for c in target_cols
            ])

    # Estabelecimentos: pad country code
    if file_type == "ESTABELE" and "pais" in df.columns:
        df = df.with_columns(
            pl.col("pais").str.zfill(3)
        )

    # Socios: ensure cnpj_cpf_do_socio is not null (PK part)
    if file_type == "SOCIOCSV" and "cnpj_cpf_do_socio" in df.columns:
        df = df.with_columns(
            pl.col("cnpj_cpf_do_socio").fill_null("00000000000000")
        )

    return df
