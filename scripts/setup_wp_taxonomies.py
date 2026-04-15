#!/usr/bin/env python3
"""
Brasil Escolas — Utilitário: Configurar taxonomias do Directorist no WordPress
Cria/verifica as categorias federal/estadual/municipal/privada.
Renomeia "Uncategorized" para "Sem categoria" se existir.

Variáveis de ambiente:
  WP_URL, WP_USER, WP_APP_PASSWORD, WP_PIPELINE_TOKEN
"""
import json
import logging
import os
import sys

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WP_URL            = os.environ.get("WP_URL", "https://brasilescolas.com.br")
WP_USER           = os.environ.get("WP_USER", "")
WP_APP_PASSWORD   = os.environ.get("WP_APP_PASSWORD", "")
WP_PIPELINE_TOKEN = os.environ.get("WP_PIPELINE_TOKEN", "")

CATEGORY_TAX = "at_biz_dir-category"

CATEGORIAS_NECESSARIAS = {
    "federal":   "Federal",
    "estadual":  "Estadual",
    "municipal": "Municipal",
    "privada":   "Privada",
}


def auth_params(extra: dict = None) -> dict:
    p = {}
    if WP_PIPELINE_TOKEN:
        p["_ptk"] = WP_PIPELINE_TOKEN
    if extra:
        p.update(extra)
    return p


def main():
    if not WP_USER or not WP_APP_PASSWORD:
        log.error("Configure WP_USER e WP_APP_PASSWORD")
        sys.exit(1)

    session = requests.Session()
    session.auth = (WP_USER, WP_APP_PASSWORD)
    session.headers["User-Agent"] = "BrasilEscolas-Pipeline/1.0"
    base = WP_URL.rstrip("/") + "/wp-json"

    # Verificar auth
    r = session.get(f"{base}/wp/v2/users/me", params=auth_params(), timeout=15)
    if r.status_code != 200:
        log.error(f"Auth falhou: {r.status_code} — {r.text[:200]}")
        sys.exit(1)
    log.info(f"Autenticado como: {r.json().get('name', '?')}")

    # Listar categorias existentes
    r = session.get(
        f"{base}/wp/v2/{CATEGORY_TAX}",
        params=auth_params({"per_page": 100, "_fields": "id,slug,name"}),
        timeout=15,
    )
    if r.status_code != 200:
        log.warning(f"Taxonomia '{CATEGORY_TAX}' inacessível: {r.status_code}")
        log.warning("  Verifique se o Directorist está ativo e a taxonomia está registrada.")
        # Não falhar — o import vai tentar criar as categorias também
        return

    existing: dict[str, dict] = {}
    for term in r.json():
        existing[term["slug"]] = term
        log.info(f"  Encontrada: [{term['id']}] {term['slug']} → {term['name']}")

    # Renomear "uncategorized" → "Sem categoria"
    for slug in ("uncategorized", "sem-categoria-2"):
        if slug in existing:
            term = existing[slug]
            r2 = session.post(
                f"{base}/wp/v2/{CATEGORY_TAX}/{term['id']}",
                json={"name": "Sem categoria", "slug": "sem-categoria"},
                params=auth_params(),
                timeout=15,
            )
            if r2.status_code in (200, 201):
                log.info(f"  Renomeada: '{slug}' → 'Sem categoria'")
            else:
                log.warning(f"  Erro ao renomear '{slug}': {r2.status_code}")

    # Criar categorias faltantes
    mapping: dict[str, int] = {}
    for slug, nome in CATEGORIAS_NECESSARIAS.items():
        if slug in existing:
            mapping[slug] = existing[slug]["id"]
            log.info(f"  OK: '{slug}' já existe (ID {existing[slug]['id']})")
        else:
            r2 = session.post(
                f"{base}/wp/v2/{CATEGORY_TAX}",
                json={"name": nome, "slug": slug},
                params=auth_params(),
                timeout=15,
            )
            if r2.status_code in (200, 201):
                tid = r2.json()["id"]
                mapping[slug] = tid
                log.info(f"  Criada: '{slug}' → ID {tid}")
            else:
                log.error(f"  Erro ao criar '{slug}': {r2.status_code} — {r2.text[:100]}")
                mapping[slug] = 0

    log.info("=== Mapeamento final ===")
    for slug, tid in mapping.items():
        log.info(f"  {slug}: {tid}")

    # Salvar mapeamento para uso no import
    out_path = "data/category_ids.json"
    import os
    os.makedirs("data", exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(mapping, f, indent=2)
    log.info(f"Mapeamento salvo em {out_path}")

    if any(v == 0 for v in mapping.values()):
        log.error("Algumas categorias não puderam ser criadas — verifique o log acima.")
        sys.exit(1)

    log.info("Taxonomias configuradas com sucesso.")


if __name__ == "__main__":
    main()
