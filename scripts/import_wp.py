#!/usr/bin/env python3
"""
Brasil Escolas — Script 3: Importar escolas para WordPress via Directorist REST API
Lê escolas_transformed.json e cria/atualiza listings no WordPress.
Requer: WP_URL, WP_USER, WP_APP_PASSWORD (env vars / GitHub Secrets)
"""
import os
import sys
import json
import time
import logging
import requests
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WP_URL            = os.environ.get("WP_URL", "https://brasilescolas.com.br")
WP_USER           = os.environ.get("WP_USER", "")
WP_APP_PASSWORD   = os.environ.get("WP_APP_PASSWORD", "")
WP_PIPELINE_TOKEN = os.environ.get("WP_PIPELINE_TOKEN", "")
BATCH_SIZE        = int(os.environ.get("IMPORT_BATCH_SIZE", "50"))
DRY_RUN           = os.environ.get("DRY_RUN", "false").lower() == "true"

DATA_DIR    = Path(os.environ.get("DATA_DIR", "data"))
INPUT_FILE  = DATA_DIR / "escolas_transformed.json"
LOG_FILE    = DATA_DIR / "import_log.json"

LISTING_TYPE = "at_biz_dir"


def auth_params(extra: dict = None) -> dict:
    """Retorna parâmetros de autenticação (token ou vazio)."""
    p = {}
    if WP_PIPELINE_TOKEN:
        p["_ptk"] = WP_PIPELINE_TOKEN
    if extra:
        p.update(extra)
    return p


def test_connection(session, base: str) -> bool:
    try:
        r = session.get(f"{base}/wp/v2/users/me", params=auth_params(), timeout=15)
        if r.status_code == 200:
            log.info(f"Autenticado como: {r.json().get('name','?')}")
            return True
        log.error(f"Auth falhou: {r.status_code} — {r.text[:200]}")
        return False
    except Exception as e:
        log.error(f"Conexão falhou: {e}")
        return False


def get_existing_codes(session, base: str) -> set:
    existing, page = set(), 1
    while True:
        try:
            r = session.get(f"{base}/wp/v2/{LISTING_TYPE}",
                            params=auth_params({"per_page": 100, "page": page,
                                               "_fields": "id,meta"}), timeout=30)
            if r.status_code == 400:
                break
            r.raise_for_status()
            items = r.json()
            if not items:
                break
            for item in items:
                code = (item.get("meta") or {}).get("_escola_codigo_inep")
                if code:
                    existing.add(str(code))
            page += 1
        except Exception as e:
            log.warning(f"Erro pág {page}: {e}")
            break
    log.info(f"Já importados: {len(existing)} códigos INEP")
    return existing


def create_listing(session, base: str, escola: dict):
    if DRY_RUN:
        log.debug(f"  [DRY] {escola['listing_title']}")
        return -1
    payload = {
        "title":   escola["listing_title"],
        "content": escola.get("listing_content", ""),
        "status":  "publish",
        "atbdp_listing_types": [2],
        "meta": {
            "_directory_type":     "2",
            "_never_expire":       "1",
            "_featured":           "0",
            "_escola_codigo_inep": escola.get("_escola_codigo_inep", ""),
            "_escola_uf":          escola.get("_escola_uf", ""),
            "_escola_dependencia": escola.get("_escola_dependencia", ""),
            "_escola_localizacao": escola.get("_escola_localizacao", ""),
            "_escola_badges":      json.dumps(escola.get("_escola_badges", [])),
            "listing_address":     escola.get("listing_address", ""),
            "listing_city":        escola.get("listing_city", ""),
            "listing_state":       escola.get("listing_state", ""),
            "listing_zip":         escola.get("listing_zip", ""),
        }
    }
    r = session.post(f"{base}/wp/v2/{LISTING_TYPE}", json=payload,
                     params=auth_params(), timeout=30)
    if r.status_code in (200, 201):
        return r.json().get("id")
    log.warning(f"  ERRO {r.status_code}: {escola['listing_title'][:40]} — {r.text[:80]}")
    return None


def main():
    if not WP_USER or not WP_APP_PASSWORD:
        log.error("Configure WP_USER e WP_APP_PASSWORD (GitHub Secrets)")
        sys.exit(1)
    if not INPUT_FILE.exists():
        log.error(f"Arquivo não encontrado: {INPUT_FILE}")
        sys.exit(1)

    session = requests.Session()
    session.auth = (WP_USER, WP_APP_PASSWORD)
    session.headers["User-Agent"] = "BrasilEscolas-Pipeline/1.0"
    base = WP_URL.rstrip("/") + "/wp-json"

    if not test_connection(session, base):
        sys.exit(1)

    with open(INPUT_FILE, encoding="utf-8") as f:
        escolas = json.load(f).get("escolas", [])

    log.info(f"Total para importar: {len(escolas)}")
    existing = get_existing_codes(session, base)
    novas = [e for e in escolas if e.get("_escola_codigo_inep") not in existing]
    log.info(f"Novas (não importadas): {len(novas)}")

    if DRY_RUN:
        log.info("[DRY RUN] Simulação ativa — sem escrita")

    results = {"created": 0, "errors": 0, "batches": 0}
    t0 = datetime.utcnow()

    for i in range(0, len(novas), BATCH_SIZE):
        batch = novas[i:i + BATCH_SIZE]
        results["batches"] += 1
        log.info(f"Lote {results['batches']}: {len(batch)} escolas ({i+1}–{i+len(batch)})")
        for escola in batch:
            nid = create_listing(session, base, escola)
            results["created" if nid else "errors"] += 1
        if i + BATCH_SIZE < len(novas):
            time.sleep(2)

    elapsed = (datetime.utcnow() - t0).total_seconds()
    log.info(f"Criadas: {results['created']} | Erros: {results['errors']} | {elapsed:.1f}s")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump({**results, "elapsed_s": elapsed,
                   "finished_at": datetime.utcnow().isoformat()}, f, indent=2)

    return results["errors"] == 0


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
