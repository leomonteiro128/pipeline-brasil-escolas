#!/usr/bin/env python3
"""
gerar_paginas_story.py — Brasil Escolas Pipeline
Gera páginas HTML físicas para cada Web Story publicada no Supabase.

Lógica:
  - Consulta a tabela web_stories (status = publicado) via REST API
  - Para cada story, cria web-stories/{slug}/index.html
  - O arquivo é uma cópia de web-stories/index.html com meta tags ajustadas
  - Isso garante que o Apache sirva o arquivo diretamente sem depender de mod_rewrite

Uso:
  python3 scripts/gerar_paginas_story.py

Variáveis de ambiente (ou valores padrão do config.js):
  SUPABASE_URL       — URL do projeto Supabase
  SUPABASE_ANON_KEY  — Chave anon pública
"""

import os
import re
import json
import sys
import urllib.request
from pathlib import Path

# ── Caminhos ────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).resolve().parent
BASE_DIR     = SCRIPT_DIR.parent
WS_DIR       = BASE_DIR / "web-stories"
TEMPLATE_SRC = WS_DIR / "index.html"

# ── Credenciais ──────────────────────────────────────────────────────────────
SUPABASE_URL      = os.getenv("SUPABASE_URL",      "https://bagsommckxgkgrpmewip.supabase.co")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJhZ3NvbW1ja3hna2dycG1ld2lwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzYyODc5NDEsImV4cCI6MjA5MTg2Mzk0MX0._PdQM24NGpftBeIx1bC8gFhwVcbTkElsWyBKTFwfNds")
SITE_URL          = "https://brasilescolas.com.br"


def buscar_stories() -> list[dict]:
    """Retorna todas as stories publicadas via Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/web_stories?select=*&order=publicado_em.desc"
    req = urllib.request.Request(url, headers={
        "apikey":        SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    # Filtrar apenas publicadas (pode não ter coluna status em todas as versões)
    publicadas = [s for s in data if s.get("status") in ("publicado", None)]
    return publicadas


def gerar_html_story(template_html: str, story: dict) -> str:
    """
    Recebe o HTML do template (web-stories/index.html) e substitui
    os meta tags padrão pelos valores específicos desta story.
    """
    slug      = story["slug"]
    titulo    = story.get("titulo", "Web Story | Brasil Escolas")
    descricao = story.get("descricao") or f"Web Story educativo sobre {titulo[:80]}"
    capa_url  = story.get("capa_url") or story.get("imagem_url") or ""
    og_image  = f"{SITE_URL}{capa_url}" if capa_url.startswith("/") else capa_url
    if not og_image:
        og_image = f"{SITE_URL}/assets/img/og-image.png"
    page_url  = f"{SITE_URL}/web-stories/{slug}/"

    html = template_html

    # <title>
    html = re.sub(
        r"<title>[^<]*</title>",
        f"<title>{titulo} | Brasil Escolas</title>",
        html, count=1
    )

    # meta description
    html = re.sub(
        r'(<meta name="description" content=")[^"]*(")',
        r"\g<1>" + descricao[:155].replace('"', '&quot;') + r"\g<2>",
        html, count=1
    )

    # canonical
    html = re.sub(
        r'(<link rel="canonical" href=")[^"]*(")',
        r"\g<1>" + page_url + r"\g<2>",
        html, count=1
    )

    # og:url
    html = re.sub(
        r'(<meta property="og:url" content=")[^"]*(")',
        r"\g<1>" + page_url + r"\g<2>",
        html, count=1
    )

    # og:title
    html = re.sub(
        r'(<meta property="og:title" content=")[^"]*(")',
        r"\g<1>" + titulo.replace('"', '&quot;') + r"\g<2>",
        html, count=1
    )

    # og:description
    html = re.sub(
        r'(<meta property="og:description" content=")[^"]*(")',
        r"\g<1>" + descricao[:155].replace('"', '&quot;') + r"\g<2>",
        html, count=1
    )

    # og:image
    html = re.sub(
        r'(<meta property="og:image" content=")[^"]*(")',
        r"\g<1>" + og_image + r"\g<2>",
        html, count=1
    )

    # twitter:title
    html = re.sub(
        r'(<meta name="twitter:title" content=")[^"]*(")',
        r"\g<1>" + titulo.replace('"', '&quot;') + r"\g<2>",
        html, count=1
    )

    # twitter:description
    html = re.sub(
        r'(<meta name="twitter:description" content=")[^"]*(")',
        r"\g<1>" + descricao[:155].replace('"', '&quot;') + r"\g<2>",
        html, count=1
    )

    # twitter:image
    html = re.sub(
        r'(<meta name="twitter:image" content=")[^"]*(")',
        r"\g<1>" + og_image + r"\g<2>",
        html, count=1
    )

    return html


def main():
    if not TEMPLATE_SRC.exists():
        print(f"[ERRO] Template não encontrado: {TEMPLATE_SRC}", file=sys.stderr)
        sys.exit(1)

    template_html = TEMPLATE_SRC.read_text(encoding="utf-8")

    print("Brasil Escolas — Gerador de Páginas de Web Story")
    print("=" * 55)

    print("\n[1/3] Consultando Supabase...")
    try:
        stories = buscar_stories()
    except Exception as e:
        print(f"[ERRO] Falha ao consultar Supabase: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"      {len(stories)} story(ies) publicada(s) encontrada(s)")

    if not stories:
        print("      Nenhuma story para gerar. Encerrando.")
        return

    print("\n[2/3] Gerando páginas HTML físicas...")
    geradas = 0
    for story in stories:
        slug = story.get("slug", "").strip()
        if not slug:
            print(f"  [!] Story ID {story.get('id')} sem slug — ignorada")
            continue

        destino_dir = WS_DIR / slug
        destino_dir.mkdir(parents=True, exist_ok=True)
        destino = destino_dir / "index.html"

        html = gerar_html_story(template_html, story)
        destino.write_text(html, encoding="utf-8")

        # Criar .htaccess na pasta da story — mantém RewriteEngine On para
        # que as regras do .htaccess raiz sejam herdadas corretamente.
        htaccess = destino_dir / ".htaccess"
        htaccess.write_text(
            "# Brasil Escolas — story page\n"
            "# RewriteEngine On obrigatorio para herdar regras do .htaccess raiz\n"
            "RewriteEngine On\n"
            "Options -Indexes\n",
            encoding="utf-8"
        )

        print(f"  OK web-stories/{slug}/index.html")
        geradas += 1

    print(f"\n[3/3] Concluído — {geradas} página(s) gerada(s)")
    print(f"      Diretório: {WS_DIR}")


if __name__ == "__main__":
    main()
