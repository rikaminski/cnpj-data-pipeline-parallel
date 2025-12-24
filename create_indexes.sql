-- Create indexes and handle duplicates conditionally
-- Strategy: Try CREATE UNIQUE INDEX, if fails due to duplicates, clean and retry

-- Empresas
DO $$
BEGIN
    CREATE UNIQUE INDEX idx_empresas_pk ON empresas(cnpj_basico);
EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE 'Duplicates found in empresas, cleaning...';
    DELETE FROM empresas a USING empresas b 
    WHERE a.ctid > b.ctid AND a.cnpj_basico = b.cnpj_basico;
    CREATE UNIQUE INDEX idx_empresas_pk ON empresas(cnpj_basico);
END $$;

-- Estabelecimentos
DO $$
BEGIN
    CREATE UNIQUE INDEX idx_estabelecimentos_pk ON estabelecimentos(cnpj_basico, cnpj_ordem, cnpj_dv);
EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE 'Duplicates found in estabelecimentos, cleaning...';
    DELETE FROM estabelecimentos a USING estabelecimentos b 
    WHERE a.ctid > b.ctid 
      AND a.cnpj_basico = b.cnpj_basico 
      AND a.cnpj_ordem = b.cnpj_ordem 
      AND a.cnpj_dv = b.cnpj_dv;
    CREATE UNIQUE INDEX idx_estabelecimentos_pk ON estabelecimentos(cnpj_basico, cnpj_ordem, cnpj_dv);
END $$;

-- Socios
DO $$
BEGIN
    CREATE UNIQUE INDEX idx_socios_pk ON socios(cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio);
EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE 'Duplicates found in socios, cleaning...';
    DELETE FROM socios a USING socios b 
    WHERE a.ctid > b.ctid 
      AND a.cnpj_basico = b.cnpj_basico 
      AND a.identificador_de_socio = b.identificador_de_socio 
      AND a.cnpj_cpf_do_socio = b.cnpj_cpf_do_socio;
    CREATE UNIQUE INDEX idx_socios_pk ON socios(cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio);
END $$;

-- Dados Simples
DO $$
BEGIN
    CREATE UNIQUE INDEX idx_dados_simples_pk ON dados_simples(cnpj_basico);
EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE 'Duplicates found in dados_simples, cleaning...';
    DELETE FROM dados_simples a USING dados_simples b 
    WHERE a.ctid > b.ctid AND a.cnpj_basico = b.cnpj_basico;
    CREATE UNIQUE INDEX idx_dados_simples_pk ON dados_simples(cnpj_basico);
END $$;

-- Additional indexes for queries
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_uf ON estabelecimentos(uf);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_municipio ON estabelecimentos(municipio);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_situacao ON estabelecimentos(situacao_cadastral);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnae ON estabelecimentos(cnae_fiscal_principal);
CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios(cnpj_basico);
