#!/usr/bin/env python3
"""
Brasil Escolas — Script 3: Importar escolas para WordPress via Directorist REST API
Lê escolas_transformed.json e cria listings no WordPress.

Melhorias:
  - Resolve IDs de taxonomia (at_biz_dir-category) antes de importar
  - Cria categorias ausentes (federal/estadual/municipal/privada)
  - Gera imagens placeholder PNG por tipo de dependência (4 imagens compartilhadas)
  - Loga progresso por estado
  - Campos novos: listing_phone, listing_website, listing_email

Variáveis de ambiente:
  WP_URL, WP_USER, WP_APP_PASSWORD, WP_PIPELINE_TOKEN
  IMPORT_BATCH_SIZE   (padrão: 50)
  DRY_RUN             (padrão: false)
  UPLOAD_PLACEHOLDERS (padrão: true — gera e faz upload de 4 imagens placeholder)
"""
import io
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WP_URL            = os.environ.get("WP_URL", "https://brasilescolas.com.br")
WP_USER           = os.environ.get("WP_USER", "")
WP_APP_PASSWORD   = os.environ.get("WP_APP_PASSWORD", "")
WP_PIPELINE_TOKEN = os.environ.get("WP_PIPELINE_TOKEN", "")
BATCH_SIZE        = int(os.environ.get("IMPORT_BATCH_SIZE", "50"))
DRY_RUN           = os.environ.get("DRY_RUN", "false").lower() == "true"
UPLOAD_PLACEHOLDERS = os.environ.get("UPLOAD_PLACEHOLDERS", "true").lower() == "true"

DATA_DIR   = Path(os.environ.get("DATA_DIR", "data"))
INPUT_FILE = DATA_DIR / "escolas_transformed.json"
LOG_FILE   = DATA_DIR / "import_log.json"

LISTING_TYPE   = "at_biz_dir"
CATEGORY_TAX   = "at_biz_dir-category"

# Categorias obrigatórias: slug → nome exibido
CATEGORIAS = {
    "federal":   "Federal",
    "estadual":  "Estadual",
    "municipal": "Municipal",
    "privada":   "Privada",
}

# Cores por dependência para placeholder PNG
DEP_COLORS = {
    "federal":   (30,  64, 175),   # azul
    "estadual":  (22, 163,  74),   # verde
    "municipal": (249, 115,  22),  # laranja
    "privada":   (124,  58, 237),  # roxo
}


# ─── autenticação ─────────────────────────────────────────────────────────────

def auth_params(extra: dict = None) -> dict:
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
            log.info(f"Autenticado como: {r.json().get('name', '?')}")
            return True
        log.error(f"Auth falhou: {r.status_code} — {r.text[:200]}")
        return False
    except Exception as e:
        log.error(f"Conexão falhou: {e}")
        return False


# ─── taxonomias ──────────────────────────────────────────────────────────────

def get_or_create_categories(session, base: str) -> dict[str, int]:
    """
    Retorna mapeamento slug → term_id para as categorias do Directorist.
    Cria as categorias que não existirem.
    """
    # Buscar existentes
    existing: dict[str, int] = {}
    try:
        r = session.get(
            f"{base}/wp/v2/{CATEGORY_TAX}",
            params=auth_params({"per_page": 100, "_fields": "id,slug,name"}),
            timeout=15,
        )
        if r.status_code == 200:
            for term in r.json():
                existing[term["slug"]] = term["id"]
            log.info(f"Categorias existentes: {existing}")
        else:
            log.warning(f"  Erro ao buscar categorias: {r.status_code} — {r.text[:100]}")
    except Exception as e:
        log.warning(f"  Exceção ao buscar categorias: {e}")

    # Criar as que faltam
    mapping: dict[str, int] = {}
    for slug, nome in CATEGORIAS.items():
        if slug in existing:
            mapping[slug] = existing[slug]
            log.info(f"  Categoria '{slug}' já existe: ID {existing[slug]}")
        else:
            if DRY_RUN:
                log.info(f"  [DRY] Criaria categoria '{slug}' ({nome})")
                mapping[slug] = 0
                continue
            try:
                r = session.post(
                    f"{base}/wp/v2/{CATEGORY_TAX}",
                    json={"name": nome, "slug": slug},
                    params=auth_params(),
                    timeout=15,
                )
                if r.status_code in (200, 201):
                    tid = r.json()["id"]
                    mapping[slug] = tid
                    log.info(f"  Categoria '{slug}' criada: ID {tid}")
                else:
                    log.warning(f"  Erro ao criar '{slug}': {r.status_code} — {r.text[:100]}")
                    mapping[slug] = 0
            except Exception as e:
                log.warning(f"  Exceção ao criar categoria '{slug}': {e}")
                mapping[slug] = 0

    return mapping


# ─── imagens placeholder ─────────────────────────────────────────────────────

def generate_placeholder_png(dep_slug: str) -> bytes:
    """
    Gera PNG 400×300 com cor de fundo da dependência e iniciais do tipo.
    Não requer fontes externas — usa PIL ImageDraw para texto simples.
    """
    try:
        from PIL import Image, ImageDraw
    except ImportError:
        log.warning("  Pillow não instalado — placeholder PNG não gerado")
        return b""

    color = DEP_COLORS.get(dep_slug, (107, 114, 128))
    label = dep_slug[:3].upper()  # "FED", "EST", "MUN", "PRI"

    img = Image.new("RGB", (400, 300), color)
    draw = ImageDraw.Draw(img)

    # Tenta fonte padrão grande; fallback para load_default
    try:
        from PIL import ImageFont
        font_large = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 96)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 28)
    except Exception:
        try:
            from PIL import ImageFont
            font_large = ImageFont.load_default(size=96)
            font_small = ImageFont.load_default(size=28)
        except Exception:
            from PIL import ImageFont
            font_large = ImageFont.load_default()
            font_small = font_large

    # Texto centrado
    bbox = draw.textbbox((0, 0), label, font=font_large)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text(((400 - tw) // 2, (300 - th) // 2 - 20), label, fill=(255, 255, 255), font=font_large)

    subtitle = CATEGORIAS.get(dep_slug, dep_slug).upper()
    bbox2 = draw.textbbox((0, 0), subtitle, font=font_small)
    tw2 = bbox2[2] - bbox2[0]
    draw.text(((400 - tw2) // 2, 240), subtitle, fill=(255, 255, 255, 180), font=font_small)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def upload_placeholder_images(session, base: str) -> dict[str, int]:
    """
    Gera e faz upload de 4 imagens placeholder (uma por tipo de dependência).
    Retorna mapeamento dep_slug → media_id.
    """
    if DRY_RUN:
        log.info("  [DRY] Pulando upload de placeholders")
        return {}

    placeholder_ids: dict[str, int] = {}
    for dep_slug in DEP_COLORS:
        png_bytes = generate_placeholder_png(dep_slug)
        if not png_bytes:
            continue
        filename = f"escola-placeholder-{dep_slug}.png"
        try:
            r = session.post(
                f"{base}/wp/v2/media",
                headers={
                    "Content-Type":        "image/png",
                    "Content-Disposition": f'attachment; filename="{filename}"',
                },
                params=auth_params(),
                data=png_bytes,
                timeout=30,
            )
            if r.status_code in (200, 201):
                mid = r.json()["id"]
                placeholder_ids[dep_slug] = mid
                log.info(f"  Placeholder '{dep_slug}' enviado: media ID {mid}")
            else:
                log.warning(f"  Erro ao enviar placeholder '{dep_slug}': {r.status_code} — {r.text[:80]}")
        except Exception as e:
            log.warning(f"  Exceção no upload placeholder '{dep_slug}': {e}")

    return placeholder_ids


# ─── deduplicação ─────────────────────────────────────────────────────────────

def get_existing_codes(session, base: str) -> set[str]:
    existing, page = set(), 1
    while True:
        try:
            r = session.get(
                f"{base}/wp/v2/{LISTING_TYPE}",
                params=auth_params({"per_page": 100, "page": page,
                                    "_fields": "id,meta", "status": "any"}),
                timeout=30,
            )
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
            if len(items) < 100:
                break
            page += 1
        except Exception as e:
            log.warning(f"  Erro pág {page}: {e}")
            break
    log.info(f"Já importados: {len(existing)} códigos INEP")
    return existing


# ─── criação de listing ────────────────────────────────────────────────────────

def create_listing(
    session,
    base: str,
    escola: dict,
    category_ids: dict[str, int],
    placeholder_ids: dict[str, int],
):
    if DRY_RUN:
        log.debug(f"  [DRY] {escola['listing_title']}")
        return -1

    dep_slug = escola.get("listing_category", "municipal")
    cat_id   = category_ids.get(dep_slug, 0)
    media_id = placeholder_ids.get(dep_slug, 0)

    payload: dict = {
        "title":   escola["listing_title"],
        "content": escola.get("listing_content", ""),
        "status":  "publish",
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
            "listing_phone":       escola.get("listing_phone", ""),
            "listing_website":     escola.get("listing_website", ""),
            "listing_email":       escola.get("listing_email", ""),
        },
    }

    # Categoria Directorist via taxonomia
    if cat_id:
        payload[CATEGORY_TAX] = [cat_id]

    # Imagem placeholder
    if media_id:
        payload["featured_media"] = media_id

    r = session.post(
        f"{base}/wp/v2/{LISTING_TYPE}",
        json=payload,
        params=auth_params(),
        timeout=30,
    )
    if r.status_code in (200, 201):
        return r.json().get("id")
    log.warning(f"  ERRO {r.status_code}: {escola['listing_title'][:50]} — {r.text[:100]}")
    return None


# ─── main ─────────────────────────────────────────────────────────────────────

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

    # Configurar taxonomias
    log.info("Configurando taxonomias...")
    category_ids = get_or_create_categories(session, base)

    # Upload de imagens placeholder (apenas na primeira importação ou se explicitamente solicitado)
    placeholder_ids: dict[str, int] = {}
    if UPLOAD_PLACEHOLDERS and not DRY_RUN:
        log.info("Gerando e enviando imagens placeholder...")
        placeholder_ids = upload_placeholder_images(session, base)
        log.info(f"  Placeholders prontos: {placeholder_ids}")

    # Carregar escolas
    with open(INPUT_FILE, encoding="utf-8") as f:
        escolas = json.load(f).get("escolas", [])
    log.info(f"Total para importar: {len(escolas)}")

    # Deduplicação
    existing = get_existing_codes(session, base)
    novas = [e for e in escolas if e.get("_escola_codigo_inep") not in existing]
    log.info(f"Novas (não importadas): {len(novas)}")

    if DRY_RUN:
        log.info("[DRY RUN] Simulação ativa — sem escrita")

    results: dict = {"created": 0, "errors": 0, "batches": 0, "por_uf": defaultdict(int)}
    t0 = datetime.utcnow()

    for i in range(0, len(novas), BATCH_SIZE):
        batch = novas[i:i + BATCH_SIZE]
        results["batches"] += 1
        log.info(f"Lote {results['batches']}: escolas {i + 1}–{i + len(batch)}")
        for escola in batch:
            nid = create_listing(session, base, escola, category_ids, placeholder_ids)
            if nid:
                results["created"] += 1
                results["por_uf"][escola.get("_escola_uf", "?")] += 1
            else:
                results["errors"] += 1
        if i + BATCH_SIZE < len(novas):
            time.sleep(2)

    elapsed = (datetime.utcnow() - t0).total_seconds()
    log.info(f"Criadas: {results['created']} | Erros: {results['errors']} | {elapsed:.1f}s")

    # Log por estado
    if results["por_uf"]:
        uf_summary = " | ".join(
            f"{uf}: {cnt}" for uf, cnt in sorted(results["por_uf"].items())
        )
        log.info(f"Por UF — {uf_summary}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                **{k: v for k, v in results.items() if k != "por_uf"},
                "por_uf":     dict(results["por_uf"]),
                "elapsed_s":  elapsed,
                "finished_at": datetime.utcnow().isoformat(),
            },
            f,
            indent=2,
        )

    return results["errors"] == 0


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
