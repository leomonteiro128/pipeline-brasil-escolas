#!/usr/bin/env python3
"""
Brasil Escolas — Script 2: Transformar e enriquecer dados das escolas
Normaliza campos, classifica por tipo, adiciona badges de inclusão (TEA/TDAH/disl/acess).
"""
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR    = Path(os.environ.get("DATA_DIR", "data"))
INPUT_FILE  = DATA_DIR / "escolas_raw.json"
OUTPUT_FILE = DATA_DIR / "escolas_transformed.json"

DEPENDENCIA = {1: "Federal", 2: "Estadual", 3: "Municipal", 4: "Privada"}
LOCALIZACAO = {1: "Urbana", 2: "Rural"}
ESTADOS = {
    "AC":"Acre","AL":"Alagoas","AM":"Amazonas","AP":"Amapá","BA":"Bahia",
    "CE":"Ceará","DF":"Distrito Federal","ES":"Espírito Santo","GO":"Goiás",
    "MA":"Maranhão","MG":"Minas Gerais","MS":"Mato Grosso do Sul","MT":"Mato Grosso",
    "PA":"Pará","PB":"Paraíba","PE":"Pernambuco","PI":"Piauí","PR":"Paraná",
    "RJ":"Rio de Janeiro","RN":"Rio Grande do Norte","RO":"Rondônia","RR":"Roraima",
    "RS":"Rio Grande do Sul","SC":"Santa Catarina","SE":"Sergipe","SP":"São Paulo",
    "TO":"Tocantins"
}


def badges(e: dict) -> list:
    b = []
    if e.get("IN_ACESSIBILIDADE_RAMPAS") or e.get("IN_SALA_RECURSOS_MULTIFUNCIONAIS_TIPO_I"):
        b.append("acess")
    if e.get("IN_NECESSIDADES_ESPECIAIS") or e.get("IN_EDUCACAO_ESPECIAL"):
        b += ["tea", "tdah", "disl"]
    if e.get("IN_AEE") and "tea" not in b:
        b.append("tea")
    return list(set(b))


def normalize(raw: dict) -> dict:
    uf  = raw.get("SG_UF", "")
    dep = int(raw.get("TP_DEPENDENCIA", 3))
    loc = int(raw.get("TP_LOCALIZACAO", 1))
    return {
        "listing_title":   raw.get("NO_ENTIDADE", "").strip(),
        "listing_content": (
            f"Escola {DEPENDENCIA.get(dep,'').lower()} em "
            f"{raw.get('NO_MUNICIPIO','')}/{uf}. "
            f"Localização: {LOCALIZACAO.get(loc,'Urbana')}. "
            f"Código INEP: {raw.get('CO_ENTIDADE','N/D')}."
        ),
        "listing_category": {1:"federal",2:"estadual",3:"municipal",4:"privada"}.get(dep,"municipal"),
        "listing_state":    ESTADOS.get(uf, uf),
        "listing_city":     raw.get("NO_MUNICIPIO", "").strip(),
        "listing_address":  raw.get("DS_ENDERECO", "").strip(),
        "listing_zip":      raw.get("NU_CEP", "").replace("-", ""),
        "_escola_codigo_inep": str(raw.get("CO_ENTIDADE", "")),
        "_escola_uf":          uf,
        "_escola_dependencia": DEPENDENCIA.get(dep, "Municipal"),
        "_escola_localizacao": LOCALIZACAO.get(loc, "Urbana"),
        "_escola_badges":      badges(raw),
        "tags":                badges(raw) + [uf.lower()],
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
            log.warning(f"  Escola {i}: {e}")
            errors += 1

    stats_uf = {}
    for e in transformed:
        uf = e["_escola_uf"]
        stats_uf[uf] = stats_uf.get(uf, 0) + 1

    log.info(f"OK: {len(transformed)} | Erros: {errors}")
    log.info("UF: " + ", ".join(f"{k}:{v}" for k, v in sorted(stats_uf.items())))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "metadata": {
                "transformed_at": datetime.utcnow().isoformat(),
                "total": len(transformed),
                "errors": errors,
                "stats_por_uf": stats_uf,
            },
            "escolas": transformed
        }, f, ensure_ascii=False, indent=2)

    log.info(f"Salvo em {OUTPUT_FILE}")
    return len(transformed)


if __name__ == "__main__":
    sys.exit(0 if main() > 0 else 1)
