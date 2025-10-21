CREATE TABLE IF NOT EXISTS anuncios (
            id_imovel VARCHAR(36) PRIMARY KEY,
            id_anuncio BIGINT,
            url TEXT,
            imagens TEXT,
            titulo TEXT,
            descricao TEXT
        );