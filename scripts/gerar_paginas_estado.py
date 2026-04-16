#!/usr/bin/env python3
"""
FASE 4 — Gerar páginas estáticas de cada estado.
Uso: python3 scripts/gerar_paginas_estado.py
Gera: escolas/[slug-estado]/index.html em BRASIL ESCOLAS v2/
"""
import os, json
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

# ── Dados dos estados (IBGE reais) ──
ESTADOS = [
    {"uf":"AC","nome":"Acre","slug":"acre","capital":"Rio Branco","municipios":22,"regiao":"Norte","site_governo":"https://www.ac.gov.br"},
    {"uf":"AL","nome":"Alagoas","slug":"alagoas","capital":"Maceió","municipios":102,"regiao":"Nordeste","site_governo":"https://www.al.gov.br"},
    {"uf":"AM","nome":"Amazonas","slug":"amazonas","capital":"Manaus","municipios":62,"regiao":"Norte","site_governo":"https://www.amazonas.am.gov.br"},
    {"uf":"AP","nome":"Amapá","slug":"amapa","capital":"Macapá","municipios":16,"regiao":"Norte","site_governo":"https://www.portal.ap.gov.br"},
    {"uf":"BA","nome":"Bahia","slug":"bahia","capital":"Salvador","municipios":417,"regiao":"Nordeste","site_governo":"https://www.ba.gov.br"},
    {"uf":"CE","nome":"Ceará","slug":"ceara","capital":"Fortaleza","municipios":184,"regiao":"Nordeste","site_governo":"https://www.ceara.gov.br"},
    {"uf":"DF","nome":"Distrito Federal","slug":"distrito-federal","capital":"Brasília","municipios":1,"regiao":"Centro-Oeste","site_governo":"https://www.df.gov.br"},
    {"uf":"ES","nome":"Espírito Santo","slug":"espirito-santo","capital":"Vitória","municipios":78,"regiao":"Sudeste","site_governo":"https://www.es.gov.br"},
    {"uf":"GO","nome":"Goiás","slug":"goias","capital":"Goiânia","municipios":246,"regiao":"Centro-Oeste","site_governo":"https://www.goias.gov.br"},
    {"uf":"MA","nome":"Maranhão","slug":"maranhao","capital":"São Luís","municipios":217,"regiao":"Nordeste","site_governo":"https://www.ma.gov.br"},
    {"uf":"MG","nome":"Minas Gerais","slug":"minas-gerais","capital":"Belo Horizonte","municipios":853,"regiao":"Sudeste","site_governo":"https://www.mg.gov.br"},
    {"uf":"MS","nome":"Mato Grosso do Sul","slug":"mato-grosso-do-sul","capital":"Campo Grande","municipios":79,"regiao":"Centro-Oeste","site_governo":"https://www.ms.gov.br"},
    {"uf":"MT","nome":"Mato Grosso","slug":"mato-grosso","capital":"Cuiabá","municipios":141,"regiao":"Centro-Oeste","site_governo":"https://www.mt.gov.br"},
    {"uf":"PA","nome":"Pará","slug":"para","capital":"Belém","municipios":144,"regiao":"Norte","site_governo":"https://www.pa.gov.br"},
    {"uf":"PB","nome":"Paraíba","slug":"paraiba","capital":"João Pessoa","municipios":223,"regiao":"Nordeste","site_governo":"https://www.pb.gov.br"},
    {"uf":"PE","nome":"Pernambuco","slug":"pernambuco","capital":"Recife","municipios":185,"regiao":"Nordeste","site_governo":"https://www.pe.gov.br"},
    {"uf":"PI","nome":"Piauí","slug":"piaui","capital":"Teresina","municipios":224,"regiao":"Nordeste","site_governo":"https://www.pi.gov.br"},
    {"uf":"PR","nome":"Paraná","slug":"parana","capital":"Curitiba","municipios":399,"regiao":"Sul","site_governo":"https://www.parana.pr.gov.br"},
    {"uf":"RJ","nome":"Rio de Janeiro","slug":"rio-de-janeiro","capital":"Rio de Janeiro","municipios":92,"regiao":"Sudeste","site_governo":"https://www.rj.gov.br"},
    {"uf":"RN","nome":"Rio Grande do Norte","slug":"rio-grande-do-norte","capital":"Natal","municipios":167,"regiao":"Nordeste","site_governo":"https://www.rn.gov.br"},
    {"uf":"RO","nome":"Rondônia","slug":"rondonia","capital":"Porto Velho","municipios":52,"regiao":"Norte","site_governo":"https://www.rondonia.ro.gov.br"},
    {"uf":"RR","nome":"Roraima","slug":"roraima","capital":"Boa Vista","municipios":15,"regiao":"Norte","site_governo":"https://www.rr.gov.br"},
    {"uf":"RS","nome":"Rio Grande do Sul","slug":"rio-grande-do-sul","capital":"Porto Alegre","municipios":497,"regiao":"Sul","site_governo":"https://www.estado.rs.gov.br"},
    {"uf":"SC","nome":"Santa Catarina","slug":"santa-catarina","capital":"Florianópolis","municipios":295,"regiao":"Sul","site_governo":"https://www.sc.gov.br"},
    {"uf":"SE","nome":"Sergipe","slug":"sergipe","capital":"Aracaju","municipios":75,"regiao":"Nordeste","site_governo":"https://www.se.gov.br"},
    {"uf":"SP","nome":"São Paulo","slug":"sao-paulo","capital":"São Paulo","municipios":645,"regiao":"Sudeste","site_governo":"https://www.saopaulo.sp.gov.br"},
    {"uf":"TO","nome":"Tocantins","slug":"tocantins","capital":"Palmas","municipios":139,"regiao":"Norte","site_governo":"https://www.to.gov.br"},
]

def gerar_paginas():
    BASE_DIR   = Path(__file__).parent.parent
    TEMPLATE_DIR = BASE_DIR / "templates"
    OUTPUT_DIR = Path("C:/Users/leo-m/OneDrive/Área de Trabalho/BRASIL ESCOLAS v2/escolas")

    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True)
    tpl = env.get_template("estado.html.jinja2")

    for estado in ESTADOS:
        out_dir = OUTPUT_DIR / estado["slug"]
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "index.html"

        html = tpl.render(
            estado=estado,
            meta_title=f"Escolas em {estado['nome']} | Brasil Escolas",
            meta_description=(
                f"Encontre escolas públicas e privadas em {estado['nome']} ({estado['uf']}). "
                f"{estado['municipios']} municípios, região {estado['regiao']}. "
                f"Filtros por tipo, nível e inclusão (TEA, TDAH, Dislexia)."
            ),
        )
        out_file.write_text(html, encoding="utf-8")
        print(f"OK: {estado['uf']} -> {out_file}")

    print(f"\nOK: {len(ESTADOS)} paginas de estado geradas em {OUTPUT_DIR}")

if __name__ == "__main__":
    gerar_paginas()
