-- BLOCK: empresas
CREATE TABLE empresas_new AS 
SELECT DISTINCT ON (cnpj_basico) * 
FROM empresas 
ORDER BY cnpj_basico, data_atualizacao DESC;

DROP TABLE empresas;
ALTER TABLE empresas_new RENAME TO empresas;
ALTER TABLE empresas ADD PRIMARY KEY (cnpj_basico);
ALTER TABLE empresas SET LOGGED;

-- BLOCK: estabelecimentos
CREATE TABLE estabelecimentos_new AS 
SELECT DISTINCT ON (cnpj_basico, cnpj_ordem, cnpj_dv) * 
FROM estabelecimentos 
ORDER BY cnpj_basico, cnpj_ordem, cnpj_dv, data_atualizacao DESC;

DROP TABLE estabelecimentos;
ALTER TABLE estabelecimentos_new RENAME TO estabelecimentos;
ALTER TABLE estabelecimentos ADD PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv);
ALTER TABLE estabelecimentos SET LOGGED;

-- BLOCK: socios
CREATE TABLE socios_new AS 
SELECT DISTINCT ON (cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio) * 
FROM socios 
ORDER BY cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio, data_atualizacao DESC;

DROP TABLE socios;
ALTER TABLE socios_new RENAME TO socios;
ALTER TABLE socios ADD PRIMARY KEY (cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio);
ALTER TABLE socios SET LOGGED;

-- BLOCK: dados_simples
CREATE TABLE dados_simples_new AS 
SELECT DISTINCT ON (cnpj_basico) * 
FROM dados_simples 
ORDER BY cnpj_basico, data_atualizacao DESC;

DROP TABLE dados_simples;
ALTER TABLE dados_simples_new RENAME TO dados_simples;
ALTER TABLE dados_simples ADD PRIMARY KEY (cnpj_basico);
ALTER TABLE dados_simples SET LOGGED;

-- BLOCK: additional_indexes
CREATE INDEX idx_estabelecimentos_uf ON estabelecimentos(uf);
CREATE INDEX idx_estabelecimentos_municipio ON estabelecimentos(municipio);
CREATE INDEX idx_estabelecimentos_situacao ON estabelecimentos(situacao_cadastral);
CREATE INDEX idx_estabelecimentos_cnae ON estabelecimentos(cnae_fiscal_principal);
CREATE INDEX idx_socios_cnpj_basico ON socios(cnpj_basico);
