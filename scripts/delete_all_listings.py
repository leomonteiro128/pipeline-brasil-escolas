#!/usr/bin/env python3
"""
Brasil Escolas — Utilitário: Deletar TODOS os listings at_biz_dir do WordPress.
USE COM CUIDADO — irreversível.

Variáveis de ambiente:
  WP_URL           URL base do WordPress
  WP_USER          Usuário admin
  WP_APP_PASSWORD  Application password
  WP_PIPELINE_TOKEN Token de pipeline (alternativa ao App Password)
"""
import os
import sys
import time
import logging
import requests
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WP_URL            = os.environ.get("WP_URL", "https://brasilescolas.com.br")
WP_USER           = os.environ.get("WP_USER", "")
WP_APP_PASSWORD   = os.environ.get("WP_APP_PASSWORD", "")
WP_PIPELINE_TOKEN = os.environ.get("WP_PIPELINE_TOKEN", "")
LISTING_TYPE      = "at_biz_dir"


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

    # Verificar autenticação
    r = session.get(f"{base}/wp/v2/users/me", params=auth_params(), timeout=15)
    if r.status_code != 200:
        log.error(f"Autenticação falhou: {r.status_code} — {r.text[:200]}")
        sys.exit(1)
    log.info(f"Autenticado como: {r.json().get('name', '?')}")

    # Coletar todos os IDs (paginando)
    ids: list[tuple[int, str]] = []
    page = 1
    while True:
        try:
            r = session.get(
                f"{base}/wp/v2/{LISTING_TYPE}",
                params=auth_params({"per_page": 100, "page": page, "_fields": "id,title", "status": "any"}),
                timeout=30,
            )
            if r.status_code == 400:
                break
            r.raise_for_status()
            items = r.json()
            if not items:
                break
            for item in items:
                title = item.get("title", {}).get("rendered", "")
                ids.append((item["id"], title))
            log.info(f"  Pág {page}: {len(items)} listings (total até agora: {len(ids)})")
            if len(items) < 100:
                break
            page += 1
        except Exception as e:
            log.warning(f"  Erro na pág {page}: {e}")
            break

    log.info(f"Total encontrado: {len(ids)} listings")
    if not ids:
        log.info("Nenhum listing encontrado. Nada a deletar.")
        return

    # Deletar um a um
    deleted = 0
    errors = 0
    for lid, title in ids:
        try:
            r = session.delete(
                f"{base}/wp/v2/{LISTING_TYPE}/{lid}",
                params=auth_params({"force": "true"}),
                timeout=20,
            )
            if r.status_code in (200, 201):
                deleted += 1
                log.info(f"  Deletado ID {lid}: {title}")
            else:
                errors += 1
                log.warning(f"  ERRO deletando ID {lid}: {r.status_code} — {r.text[:80]}")
            time.sleep(0.1)  # respeitar rate limit
        except Exception as e:
            errors += 1
            log.warning(f"  Exceção ao deletar ID {lid}: {e}")

    log.info(f"Total deletado: {deleted} | Erros: {errors}")
    if errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
