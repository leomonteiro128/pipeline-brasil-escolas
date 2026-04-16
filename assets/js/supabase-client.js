/* ============================================================
   Brasil Escolas — Supabase Client + funções de consulta
   ============================================================ */

const { createClient } = supabase;
const db = createClient(SUPABASE_URL, SUPABASE_ANON);

/* ── Escolas ── */

async function buscarEscolas({
  uf = null, municipio = null, dependencia = null,
  niveis = [], tea = false, tdah = false,
  dislexia = false, acessibilidade = false,
  texto = '', pagina = 1
} = {}) {
  const per = CONFIG.ESCOLAS_POR_PAGINA;
  const from = (pagina - 1) * per;
  let q = db.from('escolas').select('*', { count: 'exact' })
    .eq('status', 'ativo')
    .range(from, from + per - 1)
    .order('nome');

  if (uf)          q = q.eq('uf', uf.toUpperCase());
  if (municipio)   q = q.ilike('municipio', `%${municipio}%`);
  if (dependencia) q = q.eq('dependencia', dependencia);
  if (tea)         q = q.eq('suporte_tea', true);
  if (tdah)        q = q.eq('suporte_tdah', true);
  if (dislexia)    q = q.eq('suporte_dislexia', true);
  if (acessibilidade) q = q.eq('acessibilidade_fisica', true);
  if (niveis.length)  q = q.overlaps('niveis', niveis);
  if (texto.trim()) {
    q = q.textSearch('fts', texto.trim().split(/\s+/).join(' & '), {
      type: 'websearch', config: 'portuguese'
    });
  }

  const { data, error, count } = await q;
  if (error) throw error;
  return { escolas: data || [], total: count || 0, paginas: Math.ceil((count || 0) / per) };
}

async function buscarEscolaPorSlug(slug) {
  const { data, error } = await db.from('escolas')
    .select('*').eq('slug', slug).eq('status', 'ativo').single();
  if (error) return null;
  return data;
}

async function buscarEscolasProximas(uf, municipio, excludeId, limit = 3) {
  const { data } = await db.from('escolas')
    .select('id,nome,slug,municipio,uf,dependencia,imagem_url,imagem_placeholder_cor,suporte_tea,suporte_tdah')
    .eq('status', 'ativo')
    .eq('uf', uf)
    .ilike('municipio', `%${municipio}%`)
    .neq('id', excludeId)
    .limit(limit);
  return data || [];
}

async function buscarPorTexto(texto, limit = 8) {
  if (!texto || texto.trim().length < 2) return [];
  const { data } = await db.from('escolas')
    .select('id,nome,slug,municipio,uf,dependencia')
    .eq('status', 'ativo')
    .textSearch('fts', texto.trim().split(/\s+/).join(' & '), {
      type: 'websearch', config: 'portuguese'
    })
    .limit(limit);
  return data || [];
}

async function buscarEscolasPorUF(uf, pagina = 1) {
  return buscarEscolas({ uf, pagina });
}

async function buscarMunicipiosPorUF(uf) {
  const { data } = await db.from('escolas')
    .select('municipio')
    .eq('uf', uf.toUpperCase())
    .eq('status', 'ativo')
    .order('municipio');
  if (!data) return [];
  const unique = [...new Set(data.map(r => r.municipio))];
  return unique;
}

async function contarEscolasPorUF() {
  const { data } = await db.from('escolas')
    .select('uf')
    .eq('status', 'ativo');
  if (!data) return {};
  return data.reduce((acc, r) => {
    acc[r.uf] = (acc[r.uf] || 0) + 1;
    return acc;
  }, {});
}

async function inserirCadastroManual(dados) {
  // Verificar duplicata
  const { data: dup } = await db.from('escolas_cadastro_manual')
    .select('id')
    .ilike('nome', dados.nome)
    .eq('uf', dados.uf)
    .limit(1);
  if (dup && dup.length > 0) {
    throw new Error('DUPLICATA: Esta escola já está cadastrada em nossa plataforma.');
  }
  const { data, error } = await db.from('escolas_cadastro_manual').insert([dados]).select();
  if (error) throw error;
  return data;
}

/* ── Artigos ── */

async function buscarArtigos({ categoria = null, pagina = 1, limit = null } = {}) {
  const per = limit || CONFIG.ARTIGOS_POR_PAGINA;
  const from = (pagina - 1) * per;
  let q = db.from('artigos')
    .select('id,titulo,slug,excerpt,imagem_url,imagem_alt,autor,categoria,tags,publicado_em', { count: 'exact' })
    .eq('status', 'publicado')
    .order('publicado_em', { ascending: false })
    .range(from, from + per - 1);
  if (categoria) q = q.eq('categoria', categoria);
  const { data, error, count } = await q;
  if (error) throw error;
  return { artigos: data || [], total: count || 0, paginas: Math.ceil((count || 0) / per) };
}

async function buscarArtigoPorSlug(slug) {
  const { data, error } = await db.from('artigos')
    .select('*').eq('slug', slug).eq('status', 'publicado').single();
  if (error) return null;
  return data;
}

/* ── Web Stories ── */

async function buscarStories(limit = 4) {
  const { data } = await db.from('web_stories')
    .select('id,titulo,slug,capa_url')
    .eq('status', 'publicado')
    .order('publicado_em', { ascending: false })
    .limit(limit);
  return data || [];
}

/* ── Vagas ── */

async function buscarVagas({ uf = null, tipo = null, pagina = 1 } = {}) {
  const per = CONFIG.VAGAS_POR_PAGINA;
  const from = (pagina - 1) * per;
  let q = db.from('vagas')
    .select('*', { count: 'exact' })
    .eq('status', 'aprovado')
    .order('criado_em', { ascending: false })
    .range(from, from + per - 1);
  if (uf)   q = q.eq('uf', uf);
  if (tipo) q = q.eq('tipo', tipo);
  const { data, error, count } = await q;
  if (error) throw error;
  return { vagas: data || [], total: count || 0, paginas: Math.ceil((count || 0) / per) };
}

async function inserirVaga(dados) {
  const { data, error } = await db.from('vagas').insert([dados]).select();
  if (error) throw error;
  return data;
}
