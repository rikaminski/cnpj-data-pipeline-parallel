-- BLOCK: empresas
CREATE UNLOGGED TABLE empresas_new AS 
SELECT DISTINCT ON (cnpj_basico) * 
FROM empresas 
ORDER BY cnpj_basico, data_atualizacao DESC;

DROP TABLE empresas;
ALTER TABLE empresas_new RENAME TO empresas;
ALTER TABLE empresas ADD PRIMARY KEY (cnpj_basico);

-- BLOCK: estabelecimentos
CREATE UNLOGGED TABLE estabelecimentos_new AS 
SELECT DISTINCT ON (cnpj_basico, cnpj_ordem, cnpj_dv) * 
FROM estabelecimentos 
ORDER BY cnpj_basico, cnpj_ordem, cnpj_dv, data_atualizacao DESC;

DROP TABLE estabelecimentos;
ALTER TABLE estabelecimentos_new RENAME TO estabelecimentos;
ALTER TABLE estabelecimentos ADD PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv);

-- BLOCK: socios
CREATE UNLOGGED TABLE socios_new AS 
SELECT DISTINCT ON (cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio) * 
FROM socios 
ORDER BY cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio, data_atualizacao DESC;

DROP TABLE socios;
ALTER TABLE socios_new RENAME TO socios;
ALTER TABLE socios ADD PRIMARY KEY (cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio);

-- BLOCK: dados_simples
CREATE UNLOGGED TABLE dados_simples_new AS 
SELECT DISTINCT ON (cnpj_basico) * 
FROM dados_simples 
ORDER BY cnpj_basico, data_atualizacao DESC;

DROP TABLE dados_simples;
ALTER TABLE dados_simples_new RENAME TO dados_simples;
ALTER TABLE dados_simples ADD PRIMARY KEY (cnpj_basico);

-- BLOCK: index_estab_uf
CREATE INDEX idx_estabelecimentos_uf ON estabelecimentos(uf);

-- BLOCK: index_estab_mun
CREATE INDEX idx_estabelecimentos_municipio ON estabelecimentos(municipio);

-- BLOCK: index_estab_sit
CREATE INDEX idx_estabelecimentos_situacao ON estabelecimentos(situacao_cadastral);

-- BLOCK: index_estab_cnae
CREATE INDEX idx_estabelecimentos_cnae ON estabelecimentos(cnae_fiscal_principal);

-- BLOCK: index_socios_basico
CREATE INDEX idx_socios_cnpj_basico ON socios(cnpj_basico);
