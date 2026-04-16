#!/usr/bin/env python3
"""
Gera HTML estático para uma escola a partir dos dados do Supabase.
Usado pelo pipeline importar-escolas.yml após cada inserção.
Uso: python3 scripts/gerar_pagina_escola.py --slug cemei-jardim-da-paz-sp
"""
import os, sys, json, argparse
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv

load_dotenv()

BASE_DIR    = Path(__file__).parent.parent
TEMPLATE_DIR = BASE_DIR / "templates"
OUTPUT_BASE = Path("C:/Users/leo-m/OneDrive/Área de Trabalho/BRASIL ESCOLAS v2/escolas")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

UF_SLUGS = {
    "AC":"acre","AL":"alagoas","AM":"amazonas","AP":"amapa","BA":"bahia",
    "CE":"ceara","DF":"distrito-federal","ES":"espirito-santo","GO":"goias",
    "MA":"maranhao","MG":"minas-gerais","MS":"mato-grosso-do-sul","MT":"mato-grosso",
    "PA":"para","PB":"paraiba","PE":"pernambuco","PI":"piaui","PR":"parana",
    "RJ":"rio-de-janeiro","RN":"rio-grande-do-norte","RO":"rondonia","RR":"roraima",
    "RS":"rio-grande-do-sul","SC":"santa-catarina","SE":"sergipe","SP":"sao-paulo",
    "TO":"tocantins",
}

ESTADO_NOMES = {
    "AC":"Acre","AL":"Alagoas","AM":"Amazonas","AP":"Amapá","BA":"Bahia",
    "CE":"Ceará","DF":"Distrito Federal","ES":"Espírito Santo","GO":"Goiás",
    "MA":"Maranhão","MG":"Minas Gerais","MS":"Mato Grosso do Sul","MT":"Mato Grosso",
    "PA":"Pará","PB":"Paraíba","PE":"Pernambuco","PI":"Piauí","PR":"Paraná",
    "RJ":"Rio de Janeiro","RN":"Rio Grande do Norte","RO":"Rondônia","RR":"Roraima",
    "RS":"Rio Grande do Sul","SC":"Santa Catarina","SE":"Sergipe","SP":"São Paulo",
    "TO":"Tocantins",
}

COR_DEPENDENCIA = {
    "federal": "#003F7F",
    "estadual": "#4CAF50",
    "municipal": "#0085CA",
    "privada": "#7B2D8B",
}

def buscar_escola(slug=None, codigo_inep=None):
    """Busca dados da escola no Supabase via REST API."""
    import urllib.request, json as j
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL e SUPABASE_SERVICE_KEY obrigatórios no .env")

    filtro = f"slug=eq.{slug}" if slug else f"codigo_inep=eq.{codigo_inep}"
    url = f"{SUPABASE_URL}/rest/v1/escolas?{filtro}&limit=1"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        data = j.loads(r.read())
    if not data:
        return None
    return data[0]

def substituir_config(html, supabase_url, anon_key):
    """Injeta credenciais públicas no config.js embutido."""
    return (html
        .replace("'%%SUPABASE_URL%%'", f"'{supabase_url}'")
        .replace("'%%SUPABASE_ANON_KEY%%'", f"'{anon_key}'"))

def gerar_html_escola(escola):
    """Gera o HTML da página da escola usando o template Jinja2."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=True,
    )
    # Filtro urlencode
    from urllib.parse import quote
    env.filters['urlencode'] = lambda s: quote(str(s), safe='')

    tpl = env.get_template("escola.html.jinja2")

    uf     = escola.get("uf", "")
    uf_slug = UF_SLUGS.get(uf, uf.lower())
    estado_nome = ESTADO_NOMES.get(uf, uf)

    # Cor do placeholder por dependência
    dep = escola.get("dependencia", "municipal")
    escola["imagem_placeholder_cor"] = COR_DEPENDENCIA.get(dep, "#0085CA")

    # Meta description
    niveis_str = ", ".join(escola.get("niveis") or [])
    meta_desc = (
        f"{escola['nome']} é uma escola {dep} em {escola['municipio']}/{uf}. "
        f"{'Níveis: ' + niveis_str + '. ' if niveis_str else ''}"
        f"{'Suporte para TEA, TDAH e educação inclusiva. ' if escola.get('suporte_tea') or escola.get('suporte_tdah') else ''}"
        f"Veja endereço, telefone e informações completas no Brasil Escolas."
    )

    html = tpl.render(
        escola=escola,
        uf_slug=uf_slug,
        estado_nome=estado_nome,
        meta_description=meta_desc[:160],
    )

    # Injetar credenciais públicas
    anon_key = os.getenv("SUPABASE_ANON_KEY", "")
    html = substituir_config(html, SUPABASE_URL, anon_key)
    return html, uf_slug

def salvar_html(escola, html, uf_slug):
    slug = escola["slug"]
    out_dir = OUTPUT_BASE / uf_slug / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "index.html"
    out_file.write_text(html, encoding="utf-8")
    return out_file

def main():
    parser = argparse.ArgumentParser(description="Gerar HTML de escola")
    parser.add_argument("--slug", help="Slug da escola")
    parser.add_argument("--inep", help="Código INEP da escola")
    parser.add_argument("--json", dest="json_file", help="Arquivo JSON com dados da escola")
    args = parser.parse_args()

    if args.json_file:
        with open(args.json_file, encoding="utf-8") as f:
            escola = json.load(f)
    elif args.slug:
        escola = buscar_escola(slug=args.slug)
        if not escola:
            print(f"ERRO: Escola com slug '{args.slug}' não encontrada")
            sys.exit(1)
    elif args.inep:
        escola = buscar_escola(codigo_inep=args.inep)
        if not escola:
            print(f"ERRO: Escola com INEP '{args.inep}' não encontrada")
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)

    html, uf_slug = gerar_html_escola(escola)
    out_file = salvar_html(escola, html, uf_slug)
    print(f"OK: {out_file}")

if __name__ == "__main__":
    main()
