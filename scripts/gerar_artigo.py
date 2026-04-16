#!/usr/bin/env python3
"""
gerar_artigo.py — Pipeline Brasil Escolas
Gera um artigo de educação via Claude (fallback: Gemini),
insere no Supabase e salva o HTML estático.

Autor: Leonardo Monteiro — jornalista MTE 0041108/RJ
"""

import os
import sys
import json
import re
import math
import hashlib
import datetime
import unicodedata
import requests

from pathlib import Path
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader

# ─── Caminhos base ────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).resolve().parent.parent
DATA_DIR     = BASE_DIR / "data"
TEMPLATES_DIR = BASE_DIR / "templates"
SITE_DIR     = Path(__file__).resolve().parent.parent.parent / "BRASIL ESCOLAS v2"
ARTIGOS_DIR  = SITE_DIR / "artigos"

# ─── Carregar variáveis de ambiente ───────────────────────────────────────────
load_dotenv(BASE_DIR / ".env")

ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY     = os.getenv("GEMINI_API_KEY", "")
SUPABASE_URL       = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# ─── Lista de 30 temas ────────────────────────────────────────────────────────
TEMAS = [
    "como escolher escola para filho com TEA",
    "direitos da criança com TDAH na escola",
    "como funciona o IDEB e para que serve",
    "diferenças entre escola pública e privada no Brasil",
    "como preparar seu filho para a primeira escola",
    "educação inclusiva no Brasil: avanços e desafios",
    "o que é o Censo Escolar INEP e por que importa",
    "como identificar sinais de dislexia em crianças",
    "escola de tempo integral: vantagens e como matricular",
    "profissional de apoio escolar: o que é e como solicitar",
    "lei da inclusão escolar: o que a escola é obrigada a oferecer",
    "ENEM 2026: dicas de estudo para o ensino médio",
    "creche pública: como garantir uma vaga para seu filho",
    "educação bilíngue: vale a pena e como escolher",
    "bullying na escola: como identificar e o que fazer",
    "como acompanhar o desenvolvimento escolar do seu filho",
    "ensino técnico: opções de carreira e como se matricular",
    "EJA: educação para jovens e adultos, como funciona",
    "alimentação escolar saudável: programa nacional",
    "transporte escolar: direitos e como solicitar",
    "como montar uma boa rotina de estudos para crianças",
    "escola rural: desafios e programas de apoio do governo",
    "educação especial: modalidades e atendimento especializado",
    "como é o ensino de crianças autistas em escolas regulares",
    "adaptações curriculares: o que são e quando aplicar",
    "BNCC: o que é e como impacta a educação dos seus filhos",
    "pedagogia Montessori: princípios e aplicação no Brasil",
    "como escolher escola particular: critérios essenciais",
    "avaliação escolar: formativa vs somativa",
    "relacionamento escola-família: como participar ativamente",
]

# ─── Mapeamento tema → categoria ──────────────────────────────────────────────
CATEGORIAS = {
    "TEA": "TEA",
    "TDAH": "TDAH",
    "ENEM": "ENEM",
    "dislexia": "Educação Inclusiva",
    "inclusiva": "Educação Inclusiva",
    "inclusão": "Educação Inclusiva",
    "autista": "TEA",
    "Montessori": "Dica para Pais",
    "escola particular": "Escola Privada",
    "escola pública": "Escola Pública",
    "creche": "Escola Pública",
    "pais": "Dica para Pais",
    "filho": "Dica para Pais",
    "família": "Dica para Pais",
}

SYSTEM_PROMPT = """Você é Leonardo Monteiro, jornalista com registro MTE 0041108/RJ,
especialista em educação brasileira. Escreva artigos informativos, precisos e empáticos
para pais, mães e educadores. Use linguagem acessível sem ser superficial.
Estruture o conteúdo com subtítulos H2/H3, listas quando pertinente e parágrafos curtos.
Sempre cite a base legal ou fontes oficiais (MEC, INEP, LBI, LDB) quando relevante.
Nunca invente dados ou estatísticas. Todo o conteúdo deve ser em português do Brasil."""

# ─── Helpers ──────────────────────────────────────────────────────────────────

def slugify(texto: str) -> str:
    """Converte texto para slug URL-amigável."""
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    texto = texto.lower()
    texto = re.sub(r"[^\w\s-]", "", texto)
    texto = re.sub(r"[\s_]+", "-", texto)
    texto = re.sub(r"-+", "-", texto)
    return texto.strip("-")


def estimar_tempo_leitura(html: str) -> int:
    """Estima tempo de leitura em minutos (200 palavras/min)."""
    texto = re.sub(r"<[^>]+>", " ", html)
    palavras = len(texto.split())
    return max(1, math.ceil(palavras / 200))


def extrair_excerpt(html: str, max_chars: int = 200) -> str:
    """Extrai excerpt do HTML do artigo."""
    texto = re.sub(r"<[^>]+>", " ", html)
    texto = re.sub(r"\s+", " ", texto).strip()
    if len(texto) <= max_chars:
        return texto
    return texto[:max_chars].rsplit(" ", 1)[0] + "…"


def inferir_categoria(tema: str) -> str:
    """Infere categoria a partir do tema."""
    for kw, cat in CATEGORIAS.items():
        if kw.lower() in tema.lower():
            return cat
    return "Educação Inclusiva"


def gerar_slug_unico(titulo: str) -> str:
    """Gera slug único com sufixo de hash curto."""
    base = slugify(titulo)[:60]
    sufixo = hashlib.md5(titulo.encode()).hexdigest()[:6]
    return f"{base}-{sufixo}"


def carregar_temas_usados() -> list:
    """Carrega lista de temas já usados."""
    arquivo = DATA_DIR / "temas_usados.json"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if arquivo.exists():
        with open(arquivo, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def salvar_tema_usado(tema: str) -> None:
    """Registra tema como usado."""
    arquivo = DATA_DIR / "temas_usados.json"
    usados = carregar_temas_usados()
    if tema not in usados:
        usados.append(tema)
    # Se todos os temas foram usados, reinicia a rotação
    if len(usados) >= len(TEMAS):
        usados = [tema]
    with open(arquivo, "w", encoding="utf-8") as f:
        json.dump(usados, f, ensure_ascii=False, indent=2)


def selecionar_tema() -> str:
    """Seleciona o próximo tema em rotação."""
    usados = carregar_temas_usados()
    disponiveis = [t for t in TEMAS if t not in usados]
    if not disponiveis:
        # Todos usados: reinicia e pega o primeiro
        disponiveis = TEMAS
    return disponiveis[0]


# ─── Geração de conteúdo ──────────────────────────────────────────────────────

def gerar_com_claude(tema: str) -> dict:
    """
    Chama Claude claude-sonnet-4-20250514 para gerar o artigo completo.
    Retorna dict com titulo, conteudo, meta_title, meta_description.
    Lança exceção se falhar.
    """
    import anthropic

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt_usuario = f"""Escreva um artigo completo sobre o tema: "{tema}"

Formato exigido (retorne APENAS o JSON abaixo, sem markdown):
{{
  "titulo": "Título principal do artigo (máx. 70 chars)",
  "meta_title": "Título SEO (máx. 60 chars)",
  "meta_description": "Descrição SEO atrativa (máx. 155 chars)",
  "conteudo": "HTML completo do artigo com tags h2, h3, p, ul, li, strong — mínimo 800 palavras"
}}

Requisitos do conteúdo:
- Linguagem acessível para pais e educadores brasileiros
- Mínimo de 4 subtítulos H2
- Pelo menos uma lista com recomendações práticas
- Cite legislação brasileira quando aplicável (LDB, LBI, etc.)
- Conclua com uma seção de perguntas frequentes (H2 + pares pergunta/resposta)
- NÃO use dados inventados ou estatísticas sem fonte real
- Todo o texto em português do Brasil"""

    resposta = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt_usuario}],
    )

    texto = resposta.content[0].text.strip()

    # Remover blocos de código markdown se presentes
    texto = re.sub(r"^```(?:json)?\s*", "", texto, flags=re.MULTILINE)
    texto = re.sub(r"\s*```$", "", texto, flags=re.MULTILINE)

    dados = json.loads(texto)
    dados["modelo"] = "claude-sonnet-4-20250514"
    return dados


def gerar_com_gemini(tema: str) -> dict:
    """
    Fallback: chama Gemini gemini-2.0-flash para gerar o artigo.
    Retorna dict com titulo, conteudo, meta_title, meta_description.
    Lança exceção se falhar.
    """
    import google.generativeai as genai

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(
        model_name="gemini-2.0-flash",
        system_instruction=SYSTEM_PROMPT,
    )

    prompt_usuario = f"""Escreva um artigo completo sobre o tema: "{tema}"

Retorne APENAS o seguinte JSON (sem markdown, sem explicações):
{{
  "titulo": "Título principal do artigo (máx. 70 chars)",
  "meta_title": "Título SEO (máx. 60 chars)",
  "meta_description": "Descrição SEO atrativa (máx. 155 chars)",
  "conteudo": "HTML completo com h2, h3, p, ul, li, strong — mínimo 800 palavras"
}}

Requisitos: linguagem acessível para pais brasileiros, mínimo 4 subtítulos H2,
pelo menos uma lista de recomendações, citar legislação quando aplicável,
seção de perguntas frequentes ao final, português do Brasil."""

    resposta = model.generate_content(prompt_usuario)
    texto = resposta.text.strip()
    texto = re.sub(r"^```(?:json)?\s*", "", texto, flags=re.MULTILINE)
    texto = re.sub(r"\s*```$", "", texto, flags=re.MULTILINE)

    dados = json.loads(texto)
    dados["modelo"] = "gemini-2.0-flash"
    return dados


# ─── Imagem Unsplash ──────────────────────────────────────────────────────────

def buscar_imagem_unsplash(tema: str) -> tuple[str, str]:
    """
    Busca imagem via Unsplash source (sem auth necessário).
    Retorna (url_imagem, alt_text).
    """
    # Unsplash Source API — URL pública sem chave de API
    url = "https://source.unsplash.com/800x450/?education,brazil,school"
    try:
        # A URL redireciona — capturamos a URL final
        resp = requests.get(url, allow_redirects=True, timeout=15)
        resp.raise_for_status()
        imagem_url = resp.url
        # Se ainda for a URL source, usa URL direta com seed para variedade
        if "source.unsplash.com" in imagem_url:
            seed = abs(hash(tema)) % 1000
            imagem_url = f"https://source.unsplash.com/800x450/?education,school,children&sig={seed}"
        alt_text = f"Imagem ilustrativa sobre educação — {tema[:60]}"
        return imagem_url, alt_text
    except Exception as e:
        print(f"[AVISO] Falha ao buscar imagem Unsplash: {e}", file=sys.stderr)
        # Fallback: placeholder via picsum (sem auth)
        seed = abs(hash(tema)) % 1000
        imagem_url = f"https://picsum.photos/seed/{seed}/800/450"
        alt_text = f"Imagem ilustrativa sobre educação — {tema[:60]}"
        return imagem_url, alt_text


# ─── Supabase REST ────────────────────────────────────────────────────────────

def inserir_artigo_supabase(artigo: dict) -> dict:
    """
    Insere artigo no Supabase via REST API usando SUPABASE_SERVICE_KEY.
    Retorna o registro inserido com o ID gerado.
    Lança exceção se falhar.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise ValueError("SUPABASE_URL ou SUPABASE_SERVICE_KEY não configurados no .env")

    endpoint = f"{SUPABASE_URL}/rest/v1/artigos"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    # Payload para inserção
    payload = {
        "titulo":           artigo["titulo"],
        "slug":             artigo["slug"],
        "meta_title":       artigo["meta_title"],
        "meta_description": artigo["meta_description"],
        "conteudo":         artigo["conteudo"],
        "excerpt":          artigo["excerpt"],
        "categoria":        artigo["categoria"],
        "imagem_url":       artigo["imagem_url"],
        "imagem_alt":       artigo["imagem_alt"],
        "tempo_leitura":    artigo["tempo_leitura"],
        "autor":            "Leonardo Monteiro",
        "publicado":        True,
        "publicado_em":     artigo["publicado_em"],
        "modelo_ia":        artigo.get("modelo", "claude-sonnet-4-20250514"),
    }

    resp = requests.post(endpoint, headers=headers, json=payload, timeout=30)

    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Supabase retornou {resp.status_code}: {resp.text[:300]}"
        )

    registros = resp.json()
    if isinstance(registros, list) and registros:
        return registros[0]
    return payload


# ─── Renderização HTML ────────────────────────────────────────────────────────

def renderizar_html(artigo: dict) -> str:
    """Renderiza o template Jinja2 com os dados do artigo."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=False,
    )

    # Filtro urlencode para Jinja2
    from urllib.parse import quote_plus
    env.filters["urlencode"] = lambda s: quote_plus(str(s))

    template = env.get_template("artigo.html.jinja2")
    return template.render(artigo=artigo)


def salvar_html(slug: str, html: str) -> Path:
    """Salva o HTML estático em BRASIL ESCOLAS v2/artigos/[slug]/index.html."""
    destino = ARTIGOS_DIR / slug / "index.html"
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", encoding="utf-8") as f:
        f.write(html)
    return destino


# ─── Pipeline principal ───────────────────────────────────────────────────────

def main() -> dict:
    print("=" * 60)
    print("Brasil Escolas — Gerador de Artigos")
    print("=" * 60)

    # 1. Selecionar tema
    tema = selecionar_tema()
    print(f"\n[1/8] Tema selecionado: {tema}")

    # 2. Gerar conteúdo via IA
    dados_ia = None
    print("[2/8] Gerando conteúdo com Claude…")

    if ANTHROPIC_API_KEY:
        try:
            dados_ia = gerar_com_claude(tema)
            print(f"      OK — modelo: {dados_ia['modelo']}")
        except Exception as e:
            print(f"      FALHA Claude: {e}", file=sys.stderr)

    if dados_ia is None:
        print("[2/8] Tentando fallback com Gemini…")
        if GEMINI_API_KEY:
            try:
                dados_ia = gerar_com_gemini(tema)
                print(f"      OK — modelo: {dados_ia['modelo']}")
            except Exception as e:
                print(f"      FALHA Gemini: {e}", file=sys.stderr)

    if dados_ia is None:
        print("\n[ERRO] Ambas as APIs falharam. Abortando.", file=sys.stderr)
        sys.exit(1)

    # 3. Construir objeto artigo
    print("[3/8] Processando metadados…")
    agora = datetime.datetime.utcnow()
    titulo   = dados_ia["titulo"]
    conteudo = dados_ia["conteudo"]
    slug     = gerar_slug_unico(titulo)
    categoria = inferir_categoria(tema)
    excerpt  = extrair_excerpt(conteudo)
    tempo_leitura = estimar_tempo_leitura(conteudo)

    # Formatar data legível
    meses = [
        "", "janeiro", "fevereiro", "março", "abril", "maio", "junho",
        "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
    ]
    publicado_em_iso = agora.strftime("%Y-%m-%dT%H:%M:%SZ")
    publicado_em_fmt = f"{agora.day} de {meses[agora.month]} de {agora.year}"

    # 4. Buscar imagem
    print("[4/8] Buscando imagem Unsplash…")
    imagem_url, imagem_alt = buscar_imagem_unsplash(tema)
    print(f"      URL: {imagem_url}")

    artigo = {
        "titulo":                titulo,
        "slug":                  slug,
        "meta_title":            dados_ia.get("meta_title", titulo[:60]),
        "meta_description":      dados_ia.get("meta_description", excerpt[:155]),
        "conteudo":              conteudo,
        "excerpt":               excerpt,
        "categoria":             categoria,
        "imagem_url":            imagem_url,
        "imagem_alt":            imagem_alt,
        "tempo_leitura":         tempo_leitura,
        "publicado_em":          publicado_em_iso,
        "atualizado_em":         publicado_em_iso,
        "publicado_em_formatado": publicado_em_fmt,
        "modelo":                dados_ia.get("modelo", "claude-sonnet-4-20250514"),
        "autor":                 "Leonardo Monteiro",
    }

    # 5. Inserir no Supabase
    print("[5/8] Inserindo no Supabase…")
    try:
        registro = inserir_artigo_supabase(artigo)
        artigo["id"] = registro.get("id")
        print(f"      OK — ID: {artigo['id']}")
    except Exception as e:
        print(f"[ERRO] Falha ao inserir no Supabase: {e}", file=sys.stderr)
        sys.exit(1)

    # 6. Gerar HTML
    print("[6/8] Renderizando HTML via template Jinja2…")
    try:
        html = renderizar_html(artigo)
    except Exception as e:
        print(f"[ERRO] Falha ao renderizar template: {e}", file=sys.stderr)
        sys.exit(1)

    # 7. Salvar HTML
    print("[7/8] Salvando HTML estático…")
    try:
        caminho = salvar_html(slug, html)
        print(f"      Salvo em: {caminho}")
    except Exception as e:
        print(f"[ERRO] Falha ao salvar HTML: {e}", file=sys.stderr)
        sys.exit(1)

    # 8. Registrar tema como usado
    print("[8/8] Registrando tema como usado…")
    salvar_tema_usado(tema)

    # Retornar dados para o próximo passo (gerar_webstory.py)
    resultado = {
        "id":         artigo["id"],
        "slug":       slug,
        "titulo":     titulo,
        "conteudo":   conteudo,
        "imagem_url": imagem_url,
        "categoria":  categoria,
        "caminho":    str(caminho),
    }

    print("\n" + "=" * 60)
    print("Artigo gerado com sucesso!")
    print(f"  URL: https://brasilescolas.com.br/artigos/{slug}/")
    print("=" * 60)

    # Imprimir JSON para pipe com gerar_webstory.py
    print("\n--- JSON ---")
    print(json.dumps(resultado, ensure_ascii=False, indent=2))

    return resultado


if __name__ == "__main__":
    main()
