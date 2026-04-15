#!/usr/bin/env python3
"""
Brasil Escolas — Script 2: Transformar dados das escolas para importação
Normaliza campos e gera listing_content descritivo.

REGRAS:
  - Sem badges de TEA/TDAH/Dislexia (preenchimento manual no WP Admin)
  - Apenas badge "acess" quando IN_ACESSIBILIDADE_RAMPAS=True (dado verificável do INEP)
  - Tags: apenas [sigla_uf, dependencia] — sem badges de inclusão
  - listing_content: descrição útil em português com dados reais disponíveis
"""
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR    = Path(os.environ.get("DATA_DIR", "data"))
INPUT_FILE  = DATA_DIR / "escolas_raw.json"
OUTPUT_FILE = DATA_DIR / "escolas_transformed.json"

DEPENDENCIA = {1: "Federal", 2: "Estadual", 3: "Municipal", 4: "Privada"}
LOCALIZACAO = {1: "Urbana", 2: "Rural"}
DEP_SLUG    = {1: "federal", 2: "estadual", 3: "municipal", 4: "privada"}

ESTADOS = {
    "AC": "Acre", "AL": "Alagoas", "AM": "Amazonas", "AP": "Amapá",
    "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal",
    "ES": "Espírito Santo", "GO": "Goiás", "MA": "Maranhão",
    "MG": "Minas Gerais", "MS": "Mato Grosso do Sul", "MT": "Mato Grosso",
    "PA": "Pará", "PB": "Paraíba", "PE": "Pernambuco", "PI": "Piauí",
    "PR": "Paraná", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
    "RO": "Rondônia", "RR": "Roraima", "RS": "Rio Grande do Sul",
    "SC": "Santa Catarina", "SE": "Sergipe", "SP": "São Paulo",
    "TO": "Tocantins",
}


def formatar_cep(cep: str) -> str:
    cep = cep.replace("-", "").strip()
    if len(cep) == 8 and cep.isdigit():
        return f"{cep[:5]}-{cep[5:]}"
    return cep


def gerar_descricao(escola: dict) -> str:
    dep      = DEPENDENCIA.get(escola.get("TP_DEPENDENCIA", 3), "Municipal")
    loc      = LOCALIZACAO.get(escola.get("TP_LOCALIZACAO", 1), "Urbana")
    nome     = escola.get("NO_ENTIDADE", "").strip()
    municipio = escola.get("NO_MUNICIPIO", "").strip()
    uf       = escola.get("SG_UF", "").strip()
    endereco = escola.get("DS_ENDERECO", "").strip()
    cep      = escola.get("NU_CEP", "").strip()
    telefone = escola.get("NU_TELEFONE", "").strip()
    site     = escola.get("DS_SITE", "").strip()
    email    = escola.get("DS_EMAIL", "").strip()
    inep     = escola.get("CO_ENTIDADE", "").strip()
    estado_nome = ESTADOS.get(uf, uf)

    partes = []

    # Frase de abertura
    nome_title = nome.title() if nome else "Esta escola"
    partes.append(
        f"A {nome_title} é uma escola de rede {dep.lower()} "
        f"localizada em {municipio}, {estado_nome} ({uf})."
    )

    # Endereço
    if endereco:
        cep_fmt = formatar_cep(cep)
        linha = f"Endereço: {endereco}"
        if cep_fmt:
            linha += f", CEP {cep_fmt}"
        linha += "."
        partes.append(linha)

    # Contato
    if telefone:
        partes.append(f"Telefone para contato: {telefone}.")
    if email:
        partes.append(f"E-mail: {email}.")
    if site:
        partes.append(f"Site oficial: {site}.")

    # Caracterização
    partes.append(
        f"Esta é uma escola de localização {loc.lower()}, "
        f"mantida pela rede {dep.lower()} de ensino."
    )

    # Acessibilidade (dado verificado pelo INEP)
    if escola.get("IN_ACESSIBILIDADE_RAMPAS"):
        partes.append("A escola possui rampas de acessibilidade.")

    # Aviso de verificação
    partes.append(
        "⚠️ As informações desta página foram obtidas a partir de dados públicos "
        "do Censo Escolar INEP. Entre em contato diretamente com a escola para "
        "confirmar informações antes de realizar matrícula."
    )

    # Código INEP
    if inep:
        partes.append(f"Código INEP: {inep}.")

    return " ".join(partes)


def infraestrutura_badges(escola: dict) -> list[str]:
    """
    Retorna apenas badges de infraestrutura verificáveis pelo INEP.
    NÃO inclui TEA, TDAH, Dislexia — esses são preenchidos manualmente.
    """
    badges = []
    if escola.get("IN_ACESSIBILIDADE_RAMPAS"):
        badges.append("acess")
    return badges


def normalize(raw: dict) -> dict:
    uf  = raw.get("SG_UF", "").strip()
    dep = int(raw.get("TP_DEPENDENCIA", 3))
    loc = int(raw.get("TP_LOCALIZACAO", 1))
    dep_slug = DEP_SLUG.get(dep, "municipal")
    badges = infraestrutura_badges(raw)

    return {
        "listing_title":    raw.get("NO_ENTIDADE", "").strip(),
        "listing_content":  gerar_descricao(raw),
        "listing_category": dep_slug,
        "listing_state":    ESTADOS.get(uf, uf),
        "listing_city":     raw.get("NO_MUNICIPIO", "").strip(),
        "listing_address":  raw.get("DS_ENDERECO", "").strip(),
        "listing_zip":      raw.get("NU_CEP", "").replace("-", ""),
        "listing_phone":    raw.get("NU_TELEFONE", "").strip(),
        "listing_website":  raw.get("DS_SITE", "").strip(),
        "listing_email":    raw.get("DS_EMAIL", "").strip(),
        "_escola_codigo_inep": str(raw.get("CO_ENTIDADE", "")),
        "_escola_uf":          uf,
        "_escola_dependencia": DEPENDENCIA.get(dep, "Municipal"),
        "_escola_localizacao": LOCALIZACAO.get(loc, "Urbana"),
        "_escola_badges":      badges,
        # Tags: apenas UF e dependência — sem badges de inclusão
        "tags": [uf.lower(), dep_slug],
    }


def main():
    if not INPUT_FILE.exists():
        log.error(f"Arquivo não encontrado: {INPUT_FILE}")
        sys.exit(1)

    with open(INPUT_FILE, encoding="utf-8") as f:
        raw_data = json.load(f)

    escolas_raw = raw_data.get("escolas", [])
    log.info(f"Transformando {len(escolas_raw)} escolas...")

    transformed, errors = [], 0
    for i, escola in enumerate(escolas_raw):
        try:
            transformed.append(normalize(escola))
        except Exception as e:
            log.warning(f"  Escola {i} ({escola.get('NO_ENTIDADE', '?')}): {e}")
            errors += 1

    # Estatísticas por UF
    stats_uf: dict[str, int] = {}
    for e in transformed:
        uf = e["_escola_uf"]
        stats_uf[uf] = stats_uf.get(uf, 0) + 1

    log.info(f"OK: {len(transformed)} | Erros: {errors}")
    log.info("UF: " + ", ".join(f"{k}:{v}" for k, v in sorted(stats_uf.items())))

    # Verificação de sanidade: nenhuma escola deve ter badges de inclusão falsos
    with_inclusion_badges = [
        e for e in transformed
        if any(b in e.get("_escola_badges", []) for b in ("tea", "tdah", "disl"))
    ]
    if with_inclusion_badges:
        log.error(
            f"ERRO: {len(with_inclusion_badges)} escolas com badges de inclusão gerados "
            "automaticamente. Isso não deveria acontecer — verifique normalize()."
        )
        sys.exit(1)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "metadata": {
                "transformed_at": datetime.utcnow().isoformat(),
                "total":          len(transformed),
                "errors":         errors,
                "stats_por_uf":   stats_uf,
            },
            "escolas": transformed,
        }, f, ensure_ascii=False, indent=2)

    log.info(f"Salvo em {OUTPUT_FILE}")
    return len(transformed)


if __name__ == "__main__":
    sys.exit(0 if main() > 0 else 1)
