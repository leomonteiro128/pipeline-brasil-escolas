-- ============================================================
-- FASE 0.4 — Schema completo do Supabase para Brasil Escolas
-- Executar no Supabase SQL Editor: Project → SQL Editor → New query
-- ============================================================

-- ═══ ESCOLAS ═══
CREATE TABLE IF NOT EXISTS escolas (
  id                    BIGSERIAL PRIMARY KEY,
  codigo_inep           TEXT UNIQUE NOT NULL,
  nome                  TEXT NOT NULL,
  slug                  TEXT UNIQUE NOT NULL,
  uf                    CHAR(2) NOT NULL,
  municipio             TEXT NOT NULL,
  bairro                TEXT DEFAULT '',
  endereco              TEXT DEFAULT '',
  cep                   TEXT DEFAULT '',
  telefone              TEXT DEFAULT '',
  email                 TEXT DEFAULT '',
  site                  TEXT DEFAULT '',
  instagram             TEXT DEFAULT '',
  facebook              TEXT DEFAULT '',
  whatsapp              TEXT DEFAULT '',
  dependencia           TEXT NOT NULL
    CHECK (dependencia IN ('federal','estadual','municipal','privada')),
  localizacao           TEXT DEFAULT 'urbana'
    CHECK (localizacao IN ('urbana','rural')),
  niveis                TEXT[] DEFAULT '{}',
  infraestrutura        TEXT[] DEFAULT '{}',
  suporte_tea           BOOLEAN DEFAULT FALSE,
  suporte_tdah          BOOLEAN DEFAULT FALSE,
  suporte_dislexia      BOOLEAN DEFAULT FALSE,
  acessibilidade_fisica BOOLEAN DEFAULT FALSE,
  profissional_apoio    BOOLEAN DEFAULT FALSE,
  descricao_inclusao    TEXT DEFAULT '',
  imagem_url            TEXT DEFAULT '',
  imagem_placeholder_cor TEXT DEFAULT '#0085CA',
  nota_google           NUMERIC(2,1),
  total_avaliacoes      INTEGER DEFAULT 0,
  status                TEXT DEFAULT 'ativo'
    CHECK (status IN ('ativo','inativo','pendente')),
  fonte                 TEXT DEFAULT 'inep_automatico',
  criado_em             TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_escolas_uf         ON escolas(uf);
CREATE INDEX IF NOT EXISTS idx_escolas_municipio  ON escolas(municipio);
CREATE INDEX IF NOT EXISTS idx_escolas_dep        ON escolas(dependencia);
CREATE INDEX IF NOT EXISTS idx_escolas_slug       ON escolas(slug);
CREATE INDEX IF NOT EXISTS idx_escolas_status     ON escolas(status);
CREATE INDEX IF NOT EXISTS idx_escolas_tea        ON escolas(suporte_tea) WHERE suporte_tea = TRUE;
CREATE INDEX IF NOT EXISTS idx_escolas_tdah       ON escolas(suporte_tdah) WHERE suporte_tdah = TRUE;

ALTER TABLE escolas ADD COLUMN IF NOT EXISTS fts TSVECTOR
  GENERATED ALWAYS AS (
    to_tsvector('portuguese',
      coalesce(nome,'') || ' ' ||
      coalesce(municipio,'') || ' ' ||
      coalesce(bairro,'') || ' ' ||
      coalesce(uf,'')
    )
  ) STORED;
CREATE INDEX IF NOT EXISTS idx_escolas_fts ON escolas USING GIN(fts);

-- ═══ CADASTROS MANUAIS ═══
CREATE TABLE IF NOT EXISTS escolas_cadastro_manual (
  id            BIGSERIAL PRIMARY KEY,
  nome          TEXT NOT NULL,
  cnpj          TEXT,
  tipo          TEXT CHECK (tipo IN ('publica','privada')),
  endereco      TEXT,
  cep           TEXT,
  bairro        TEXT,
  municipio     TEXT NOT NULL,
  uf            CHAR(2) NOT NULL,
  whatsapp      TEXT NOT NULL,
  telefone      TEXT,
  email         TEXT,
  site          TEXT,
  instagram     TEXT,
  facebook      TEXT,
  niveis        TEXT[] DEFAULT '{}',
  suporte_tea           BOOLEAN DEFAULT FALSE,
  suporte_tdah          BOOLEAN DEFAULT FALSE,
  suporte_dislexia      BOOLEAN DEFAULT FALSE,
  acessibilidade_fisica BOOLEAN DEFAULT FALSE,
  profissional_apoio    BOOLEAN DEFAULT FALSE,
  descricao     TEXT,
  logo_url      TEXT,
  fotos         TEXT[] DEFAULT '{}',
  status        TEXT DEFAULT 'pendente'
    CHECK (status IN ('pendente','aprovado','rejeitado')),
  ip_cadastro   TEXT,
  criado_em     TIMESTAMPTZ DEFAULT NOW()
);

-- ═══ ARTIGOS ═══
CREATE TABLE IF NOT EXISTS artigos (
  id               BIGSERIAL PRIMARY KEY,
  titulo           TEXT NOT NULL,
  slug             TEXT UNIQUE NOT NULL,
  excerpt          TEXT,
  conteudo         TEXT NOT NULL,
  imagem_url       TEXT,
  imagem_alt       TEXT,
  autor            TEXT DEFAULT 'Leonardo Monteiro',
  categoria        TEXT,
  tags             TEXT[] DEFAULT '{}',
  meta_title       TEXT,
  meta_description TEXT,
  schema_json      JSONB,
  status           TEXT DEFAULT 'publicado',
  publicado_em     TIMESTAMPTZ DEFAULT NOW(),
  atualizado_em    TIMESTAMPTZ DEFAULT NOW()
);

-- ═══ WEB STORIES ═══
CREATE TABLE IF NOT EXISTS web_stories (
  id           BIGSERIAL PRIMARY KEY,
  titulo       TEXT NOT NULL,
  slug         TEXT UNIQUE NOT NULL,
  capa_url     TEXT,
  paginas      JSONB NOT NULL DEFAULT '[]',
  artigo_id    BIGINT REFERENCES artigos(id),
  status       TEXT DEFAULT 'publicado',
  publicado_em TIMESTAMPTZ DEFAULT NOW()
);

-- ═══ VAGAS ═══
CREATE TABLE IF NOT EXISTS vagas (
  id               BIGSERIAL PRIMARY KEY,
  escola_nome      TEXT NOT NULL,
  cnpj             TEXT NOT NULL,
  cargo            TEXT NOT NULL,
  tipo             TEXT CHECK (tipo IN ('emprego','estagio')),
  area             TEXT,
  requisitos       TEXT,
  carga_horaria    TEXT,
  remuneracao      TEXT DEFAULT 'A combinar',
  prazo_inscricao  DATE,
  contato          TEXT,
  uf               CHAR(2),
  municipio        TEXT,
  status           TEXT DEFAULT 'pendente'
    CHECK (status IN ('pendente','aprovado','rejeitado')),
  criado_em        TIMESTAMPTZ DEFAULT NOW()
);

-- ═══ ROW LEVEL SECURITY ═══
ALTER TABLE escolas                ENABLE ROW LEVEL SECURITY;
ALTER TABLE artigos                ENABLE ROW LEVEL SECURITY;
ALTER TABLE web_stories            ENABLE ROW LEVEL SECURITY;
ALTER TABLE vagas                  ENABLE ROW LEVEL SECURITY;
ALTER TABLE escolas_cadastro_manual ENABLE ROW LEVEL SECURITY;

-- Políticas de leitura pública
CREATE POLICY IF NOT EXISTS "leitura_escolas" ON escolas
  FOR SELECT USING (status = 'ativo');
CREATE POLICY IF NOT EXISTS "leitura_artigos" ON artigos
  FOR SELECT USING (status = 'publicado');
CREATE POLICY IF NOT EXISTS "leitura_stories" ON web_stories
  FOR SELECT USING (status = 'publicado');
CREATE POLICY IF NOT EXISTS "leitura_vagas" ON vagas
  FOR SELECT USING (status = 'aprovado');
CREATE POLICY IF NOT EXISTS "insercao_cadastro" ON escolas_cadastro_manual
  FOR INSERT WITH CHECK (TRUE);
CREATE POLICY IF NOT EXISTS "insercao_vagas" ON vagas
  FOR INSERT WITH CHECK (TRUE);

-- Trigger para atualizar atualizado_em automaticamente
CREATE OR REPLACE FUNCTION atualizar_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.atualizado_em = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_escolas_atualizado
  BEFORE UPDATE ON escolas
  FOR EACH ROW EXECUTE FUNCTION atualizar_timestamp();

CREATE TRIGGER trigger_artigos_atualizado
  BEFORE UPDATE ON artigos
  FOR EACH ROW EXECUTE FUNCTION atualizar_timestamp();

SELECT 'Schema Brasil Escolas criado com sucesso!' AS resultado;
