#!/usr/bin/env python3
"""
gerar_webstory.py — Pipeline Brasil Escolas
Gera uma Web Story AMP a partir de um artigo:
  - Imagem de capa via DALL-E 3 (fallback: Gemini Imagen)
  - Pillow: converte para WebP, comprime até ≤80KB
  - Extrai 4 dicas do artigo
  - Insere story no Supabase
  - Gera HTML AMP via template Jinja2
  - Salva em BRASIL ESCOLAS v2/web-stories/[slug]/index.html

Uso:
  python gerar_webstory.py                        # lê JSON de stdin (pipe)
  python gerar_webstory.py --artigo-id ID ...     # flags explícitas
  python gerar_webstory.py --json '{"id":...}'    # JSON inline

Autor: Leonardo Monteiro — jornalista MTE 0041108/RJ
"""

import os
import sys
import json
import re
import io
import argparse
import datetime
import unicodedata
import requests

from pathlib import Path
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from PIL import Image, ImageDraw, ImageFont

# ─── Caminhos base ────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
SITE_DIR      = Path(__file__).resolve().parent.parent.parent / "BRASIL ESCOLAS v2"
WEB_STORIES_DIR = SITE_DIR / "web-stories"
ASSETS_STORIES  = SITE_DIR / "assets" / "img" / "stories"

# ─── Variáveis de ambiente ────────────────────────────────────────────────────
load_dotenv(BASE_DIR / ".env")

OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")
GEMINI_API_KEY       = os.getenv("GEMINI_API_KEY", "")
SUPABASE_URL         = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# ─── Limites de imagem ────────────────────────────────────────────────────────
MAX_BYTES    = 80 * 1024          # 80 KB
QUALIDADES   = [85, 75, 65]       # tentativas de compressão
CAPA_W, CAPA_H = 1024, 1792      # proporção 9:16 DALL-E 3

# ─── Helpers ──────────────────────────────────────────────────────────────────

def slugify(texto: str) -> str:
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    texto = texto.lower()
    texto = re.sub(r"[^\w\s-]", "", texto)
    texto = re.sub(r"[\s_]+", "-", texto)
    texto = re.sub(r"-+", "-", texto)
    return texto.strip("-")


def extrair_dicas(conteudo_html: str, max_dicas: int = 4) -> list[dict]:
    """
    Extrai os primeiros `max_dicas` pontos principais do HTML do artigo.
    Prioriza itens de lista (li), depois parágrafos curtos após H2/H3.
    Retorna lista de dicts {titulo, texto}.
    """
    # Tentar extrair de listas primeiro
    itens_li = re.findall(r"<li[^>]*>(.*?)</li>", conteudo_html, re.DOTALL | re.IGNORECASE)
    itens_li = [re.sub(r"<[^>]+>", "", item).strip() for item in itens_li]
    itens_li = [i for i in itens_li if len(i) > 20]

    # Tentar extrair pares H2 + parágrafo
    secoes = re.findall(
        r"<h2[^>]*>(.*?)</h2>\s*<p[^>]*>(.*?)</p>",
        conteudo_html,
        re.DOTALL | re.IGNORECASE,
    )
    secoes_limpas = [
        {
            "titulo": re.sub(r"<[^>]+>", "", t).strip()[:60],
            "texto":  re.sub(r"<[^>]+>", "", p).strip()[:200],
        }
        for t, p in secoes
        if len(re.sub(r"<[^>]+>", "", p).strip()) > 20
    ]

    dicas = []

    # Combinar: primeiro usar seções com título + texto
    for secao in secoes_limpas[:max_dicas]:
        dicas.append(secao)

    # Complementar com itens de lista se precisar
    for item in itens_li:
        if len(dicas) >= max_dicas:
            break
        # Verificar se não é duplicado
        if not any(item[:40] in d.get("texto", "") for d in dicas):
            dicas.append({"titulo": None, "texto": item[:200]})

    # Se ainda faltar, pegar parágrafos avulsos
    if len(dicas) < max_dicas:
        paragrafos = re.findall(r"<p[^>]*>(.*?)</p>", conteudo_html, re.DOTALL | re.IGNORECASE)
        for p in paragrafos:
            if len(dicas) >= max_dicas:
                break
            texto = re.sub(r"<[^>]+>", "", p).strip()
            if len(texto) > 40:
                dicas.append({"titulo": None, "texto": texto[:200]})

    return dicas[:max_dicas]


# ─── Geração de imagem de capa ────────────────────────────────────────────────

def gerar_imagem_dalle(titulo: str) -> bytes:
    """
    Gera imagem de capa 1024x1792 via DALL-E 3.
    Retorna bytes da imagem PNG.
    Lança exceção se falhar.
    """
    import openai

    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    prompt = (
        f"Professional, warm and educational illustration for a Brazilian education article "
        f"titled '{titulo[:100]}'. "
        "Style: modern flat illustration, diverse Brazilian children and families in a school setting, "
        "bright and welcoming colors (blue #003F7F, yellow #EFAA1B), portrait orientation 9:16, "
        "no text, no watermarks. High quality digital art."
    )

    resp = client.images.generate(
        model="dall-e-3",
        prompt=prompt,
        size="1024x1792",
        quality="standard",
        n=1,
        response_format="url",
    )

    url_imagem = resp.data[0].url
    r = requests.get(url_imagem, timeout=30)
    r.raise_for_status()
    return r.content


def gerar_imagem_gemini(titulo: str) -> bytes:
    """
    Fallback: gera imagem via Gemini Imagen.
    Retorna bytes da imagem.
    Lança exceção se falhar.
    """
    import google.generativeai as genai

    genai.configure(api_key=GEMINI_API_KEY)

    prompt = (
        f"Educational illustration for Brazilian article about: {titulo[:100]}. "
        "Warm, modern flat design with diverse children in school, blue and yellow palette, "
        "portrait 9:16 format, no text."
    )

    model = genai.ImageGenerationModel("imagen-3.0-generate-001")
    result = model.generate_images(
        prompt=prompt,
        number_of_images=1,
        aspect_ratio="9:16",
        safety_filter_level="block_only_high",
    )

    if not result.images:
        raise RuntimeError("Gemini Imagen não retornou imagens.")

    return result.images[0]._image_bytes


def gerar_placeholder_azul(slug: str) -> bytes:
    """
    Gera imagem placeholder azul via Pillow quando ambas as APIs falham
    ou quando o arquivo ultrapassa 80KB mesmo na qualidade mínima.
    """
    img = Image.new("RGB", (CAPA_W, CAPA_H), color=(0, 63, 127))
    draw = ImageDraw.Draw(img)

    # Gradiente simples desenhando faixas horizontais
    for y in range(CAPA_H):
        fator = y / CAPA_H
        r = int(0   + fator * 0)
        g = int(63  + fator * 70)
        b = int(127 + fator * 75)
        draw.line([(0, y), (CAPA_W, y)], fill=(r, g, b))

    # Tentar adicionar texto com fonte padrão (sem depender de fontes externas)
    try:
        font = ImageFont.load_default()
        texto = "Brasil Escolas"
        draw.text((CAPA_W // 2, CAPA_H // 2), texto, fill=(255, 255, 255), anchor="mm", font=font)
    except Exception:
        pass

    buf = io.BytesIO()
    img.save(buf, format="WEBP", quality=75)
    return buf.getvalue()


def comprimir_para_webp(imagem_bytes: bytes) -> bytes:
    """
    Converte bytes de imagem para WebP, tentando qualidades 85→75→65.
    Se mesmo quality=65 ultrapassar 80KB, usa placeholder azul.
    Retorna bytes WebP.
    """
    img = Image.open(io.BytesIO(imagem_bytes)).convert("RGB")
    # Redimensionar para 1024x1792 se necessário
    if img.size != (CAPA_W, CAPA_H):
        img = img.resize((CAPA_W, CAPA_H), Image.LANCZOS)

    for qualidade in QUALIDADES:
        buf = io.BytesIO()
        img.save(buf, format="WEBP", quality=qualidade, method=6)
        dados = buf.getvalue()
        print(f"      WebP quality={qualidade}: {len(dados) / 1024:.1f} KB")
        if len(dados) <= MAX_BYTES:
            return dados

    print(f"      AVISO: imagem excede {MAX_BYTES // 1024}KB mesmo em quality=65 — usando placeholder.")
    return gerar_placeholder_azul("placeholder")


def salvar_imagem_capa(slug: str, dados_webp: bytes) -> tuple[Path, str]:
    """
    Salva imagem de capa WebP em assets/img/stories/[slug]/capa.webp
    Retorna (caminho_local, url_publica).
    """
    destino_dir = ASSETS_STORIES / slug
    destino_dir.mkdir(parents=True, exist_ok=True)
    caminho = destino_dir / "capa.webp"
    with open(caminho, "wb") as f:
        f.write(dados_webp)
    url = f"/assets/img/stories/{slug}/capa.webp"
    return caminho, url


# ─── Supabase REST ────────────────────────────────────────────────────────────

def inserir_story_supabase(story: dict) -> dict:
    """
    Insere story no Supabase via REST API.
    Retorna o registro inserido com ID.
    Lança exceção se falhar.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise ValueError("SUPABASE_URL ou SUPABASE_SERVICE_KEY não configurados no .env")

    endpoint = f"{SUPABASE_URL}/rest/v1/web_stories"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    payload = {
        "artigo_id":   story["artigo_id"],
        "artigo_slug": story["artigo_slug"],
        "titulo":      story["titulo"],
        "slug":        story["slug"],
        "descricao":   story.get("descricao", ""),
        "capa_url":    story["capa_url"],
        "categoria":   story.get("categoria", "Educação"),
        "paginas":     json.dumps(story["paginas"], ensure_ascii=False),
        "publicado":   True,
        "publicado_em": story["publicado_em"],
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


# ─── Renderização HTML AMP ────────────────────────────────────────────────────

def renderizar_html_amp(story: dict) -> str:
    """Renderiza o template AMP Jinja2 com os dados da story."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=False,
    )
    from urllib.parse import quote_plus
    env.filters["urlencode"] = lambda s: quote_plus(str(s))

    template = env.get_template("story.html.jinja2")
    return template.render(story=story)


def salvar_html_story(slug: str, html: str) -> Path:
    """Salva o HTML AMP em BRASIL ESCOLAS v2/web-stories/[slug]/index.html."""
    destino = WEB_STORIES_DIR / slug / "index.html"
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", encoding="utf-8") as f:
        f.write(html)
    return destino


# ─── Pipeline principal ───────────────────────────────────────────────────────

def main(artigo_id: str | int, slug: str, titulo: str,
         conteudo: str, imagem_url: str, categoria: str = "Educação") -> dict:

    print("=" * 60)
    print("Brasil Escolas — Gerador de Web Story")
    print("=" * 60)
    print(f"\n  Artigo ID : {artigo_id}")
    print(f"  Slug      : {slug}")
    print(f"  Título    : {titulo[:60]}")

    story_slug = f"story-{slug}"

    # 1. Gerar imagem de capa
    print("\n[1/7] Gerando imagem de capa com DALL-E 3…")
    imagem_bytes = None

    if OPENAI_API_KEY:
        try:
            imagem_bytes = gerar_imagem_dalle(titulo)
            print("      OK — DALL-E 3")
        except Exception as e:
            print(f"      FALHA DALL-E 3: {e}", file=sys.stderr)

    if imagem_bytes is None:
        print("[1/7] Tentando fallback com Gemini Imagen…")
        if GEMINI_API_KEY:
            try:
                imagem_bytes = gerar_imagem_gemini(titulo)
                print("      OK — Gemini Imagen")
            except Exception as e:
                print(f"      FALHA Gemini Imagen: {e}", file=sys.stderr)

    if imagem_bytes is None:
        print("      AVISO: ambas as APIs de imagem falharam — usando placeholder azul.")
        imagem_bytes = gerar_placeholder_azul(story_slug)
        # Já está em WebP — salvar diretamente
        caminho_capa, capa_url = salvar_imagem_capa(story_slug, imagem_bytes)
    else:
        # 2. Converter e comprimir para WebP
        print("[2/7] Convertendo e comprimindo para WebP…")
        webp_bytes = comprimir_para_webp(imagem_bytes)
        caminho_capa, capa_url = salvar_imagem_capa(story_slug, webp_bytes)

    print(f"      Capa salva em: {caminho_capa}")
    print(f"      URL pública  : {capa_url}")

    # 3. Extrair dicas do artigo
    print("[3/7] Extraindo dicas do conteúdo do artigo…")
    dicas = extrair_dicas(conteudo, max_dicas=4)
    print(f"      {len(dicas)} dica(s) extraída(s)")

    # Garantir pelo menos 1 dica (fallback)
    if not dicas:
        dicas = [{"titulo": "Saiba mais", "texto": titulo[:200]}]

    # 4. Montar páginas da story
    print("[4/7] Montando páginas da story…")
    # Páginas 2–5 = dicas (até 4)
    paginas = [
        {
            "titulo":     d.get("titulo"),
            "texto":      d["texto"],
            "imagem_url": None,  # imagens das páginas internas podem ser nulas
        }
        for d in dicas
    ]

    agora = datetime.datetime.utcnow()
    publicado_em_iso = agora.strftime("%Y-%m-%dT%H:%M:%SZ")

    story = {
        "artigo_id":   artigo_id,
        "artigo_slug": slug,
        "titulo":      titulo,
        "slug":        story_slug,
        "descricao":   f"Web Story sobre {titulo[:100]}",
        "capa_url":    capa_url,
        "categoria":   categoria,
        "paginas":     paginas,
        "publicado_em": publicado_em_iso,
    }

    # 5. Inserir no Supabase
    print("[5/7] Inserindo story no Supabase…")
    try:
        registro = inserir_story_supabase(story)
        story["id"] = registro.get("id")
        print(f"      OK — ID: {story['id']}")
    except Exception as e:
        print(f"[ERRO] Falha ao inserir story no Supabase: {e}", file=sys.stderr)
        sys.exit(1)

    # Para o template, converter paginas de volta para lista de dicts
    if isinstance(story["paginas"], str):
        story["paginas"] = json.loads(story["paginas"])

    # 6. Gerar HTML AMP
    print("[6/7] Renderizando HTML AMP via template Jinja2…")
    try:
        html = renderizar_html_amp(story)
    except Exception as e:
        print(f"[ERRO] Falha ao renderizar template AMP: {e}", file=sys.stderr)
        sys.exit(1)

    # 7. Salvar HTML
    print("[7/7] Salvando HTML estático…")
    try:
        caminho_html = salvar_html_story(story_slug, html)
        print(f"      Salvo em: {caminho_html}")
    except Exception as e:
        print(f"[ERRO] Falha ao salvar HTML AMP: {e}", file=sys.stderr)
        sys.exit(1)

    resultado = {
        "id":         story.get("id"),
        "slug":       story_slug,
        "titulo":     titulo,
        "capa_url":   capa_url,
        "artigo_slug": slug,
        "caminho":    str(caminho_html),
        "url":        f"https://brasilescolas.com.br/web-stories/{story_slug}/",
    }

    print("\n" + "=" * 60)
    print("Web Story gerada com sucesso!")
    print(f"  URL: {resultado['url']}")
    print("=" * 60)

    print("\n--- JSON ---")
    print(json.dumps(resultado, ensure_ascii=False, indent=2))

    return resultado


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gera Web Story AMP a partir de um artigo do Brasil Escolas."
    )
    parser.add_argument("--artigo-id",  help="ID do artigo no Supabase")
    parser.add_argument("--slug",       help="Slug do artigo")
    parser.add_argument("--titulo",     help="Título do artigo")
    parser.add_argument("--conteudo",   help="HTML do conteúdo do artigo")
    parser.add_argument("--imagem-url", help="URL da imagem do artigo")
    parser.add_argument("--categoria",  help="Categoria do artigo", default="Educação")
    parser.add_argument("--json",       help="JSON completo com os dados do artigo")
    args = parser.parse_args()

    dados = {}

    # Leitura por JSON inline
    if args.json:
        try:
            dados = json.loads(args.json)
        except json.JSONDecodeError as e:
            print(f"[ERRO] JSON inválido: {e}", file=sys.stderr)
            sys.exit(1)

    # Leitura por stdin (pipe com gerar_artigo.py)
    elif not sys.stdin.isatty():
        entrada = sys.stdin.read().strip()
        # gerar_artigo.py imprime texto + "--- JSON ---" + JSON
        if "--- JSON ---" in entrada:
            parte_json = entrada.split("--- JSON ---")[-1].strip()
        else:
            parte_json = entrada
        try:
            dados = json.loads(parte_json)
        except json.JSONDecodeError as e:
            print(f"[ERRO] Não foi possível parsear JSON do stdin: {e}", file=sys.stderr)
            sys.exit(1)

    # Leitura por flags CLI
    else:
        dados = {
            "id":         args.artigo_id,
            "slug":       args.slug,
            "titulo":     args.titulo,
            "conteudo":   args.conteudo or "",
            "imagem_url": args.imagem_url or "",
            "categoria":  args.categoria,
        }

    # Validar campos obrigatórios
    campos_obrigatorios = ["slug", "titulo"]
    for campo in campos_obrigatorios:
        if not dados.get(campo):
            print(f"[ERRO] Campo obrigatório ausente: '{campo}'", file=sys.stderr)
            print("Use: python gerar_webstory.py --slug SLUG --titulo TITULO [...]", file=sys.stderr)
            sys.exit(1)

    if not dados.get("conteudo"):
        print("[AVISO] conteudo não fornecido — story terá apenas a capa e o CTA.", file=sys.stderr)

    main(
        artigo_id   = dados.get("id", ""),
        slug        = dados["slug"],
        titulo      = dados["titulo"],
        conteudo    = dados.get("conteudo", ""),
        imagem_url  = dados.get("imagem_url", ""),
        categoria   = dados.get("categoria", "Educação"),
    )
