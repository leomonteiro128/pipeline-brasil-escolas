#!/usr/bin/env python3
"""
Brasil Escolas — Script 1: Buscar escolas do Censo Escolar INEP
Cadeia de fontes (em ordem de prioridade):
  1. QEdu API          — se QEDU_API_KEY estiver definido
  2. dados.gov.br CKAN — catálogo público, sem chave
  3. ZIP microdados INEP — download direto (bloqueado no GitHub Actions)
  4. Arquivo estático   — data/escolas_inep_static.json (fallback confiável)
  NUNCA: dados fictícios de sample_schools()

Variáveis de ambiente:
  DATA_DIR        Diretório de saída (padrão: data)
  CENSO_YEAR      Ano do Censo (padrão: 2023)
  LIMIT_PER_UF    Máx. escolas por estado (padrão: 200; 0 = sem limite)
  QEDU_API_KEY    Bearer token da QEdu API (opcional)
  INEP_ZIP_CACHE  Caminho para cache do ZIP INEP (opcional)
"""
import csv
import io
import json
import logging
import os
import sys
import tempfile
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR     = Path(os.environ.get("DATA_DIR", "data"))
OUTPUT_FILE  = DATA_DIR / "escolas_raw.json"
STATIC_FILE  = Path("data/escolas_inep_static.json")
ZIP_CACHE    = Path(os.environ.get("INEP_ZIP_CACHE", "")) if os.environ.get("INEP_ZIP_CACHE") else None
CENSO_YEAR   = int(os.environ.get("CENSO_YEAR", "2023"))
LIMIT_PER_UF = int(os.environ.get("LIMIT_PER_UF", "200"))
QEDU_API_KEY = os.environ.get("QEDU_API_KEY", "")

INEP_ZIP_URLS = [
    "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{year}.zip",
    "https://download.inep.gov.br/dados_abertos/microdados_educacao_basica_{year}.zip",
    "https://download.inep.gov.br/microdados/microdados_educacao_basica_{year}.zip",
    "https://download.inep.gov.br/microdados/microdados_censo_escolar_{year}.zip",
]

ESTADOS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]

UF_CODES = {
    "AC": 12, "AL": 27, "AM": 13, "AP": 16, "BA": 29, "CE": 23,
    "DF": 53, "ES": 32, "GO": 52, "MA": 21, "MG": 31, "MS": 50,
    "MT": 51, "PA": 15, "PB": 25, "PE": 26, "PI": 22, "PR": 41,
    "RJ": 33, "RN": 24, "RO": 11, "RR": 14, "RS": 43, "SC": 42,
    "SE": 28, "SP": 35, "TO": 17,
}
CODE_TO_UF = {v: k for k, v in UF_CODES.items()}

KEEP_FIELDS = {
    "SG_UF", "CO_UF", "NO_ENTIDADE", "CO_ENTIDADE", "NO_MUNICIPIO",
    "TP_DEPENDENCIA", "TP_LOCALIZACAO", "DS_ENDERECO", "NU_CEP", "CO_CEP",
    "IN_AEE", "TP_AEE", "IN_EDUCACAO_ESPECIAL", "IN_NECESSIDADES_ESPECIAIS",
    "IN_ACESSIBILIDADE_RAMPAS", "IN_SALA_RECURSOS_MULTIFUNCIONAIS_TIPO_I",
    "IN_SALA_ATENDIMENTO_ESPECIAL",
}


# ─── validação ───────────────────────────────────────────────────────────────

def validate_escola(escola: dict, expected_uf: str | None = None) -> bool:
    """Retorna False se a escola tiver dados obrigatórios ausentes ou UF errada."""
    if not escola.get("NO_ENTIDADE", "").strip():
        return False
    if not escola.get("CO_ENTIDADE", "").strip():
        return False
    uf = escola.get("SG_UF", "").strip()
    if uf not in UF_CODES:
        return False
    if expected_uf and uf != expected_uf:
        log.debug(f"  Descartada UF errada: {uf} (esperado {expected_uf}) — {escola['NO_ENTIDADE']}")
        return False
    return True


def deduplicate(escolas: list[dict]) -> list[dict]:
    """Remove duplicatas por CO_ENTIDADE, mantendo a primeira ocorrência."""
    seen: set[str] = set()
    result = []
    for e in escolas:
        code = e.get("CO_ENTIDADE", "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        result.append(e)
    return result


# ─── fonte 1: QEdu API ────────────────────────────────────────────────────────

def fetch_qedu_uf(uf: str, limit: int) -> list[dict]:
    """
    Busca escolas da QEdu API para um estado.
    Requer QEDU_API_KEY (Bearer token).
    Endpoint: GET https://api.qedu.org.br/v1/escolas?uf=UF&per_page=100&page=N
    """
    if not QEDU_API_KEY:
        return []
    headers = {
        "Authorization": f"Bearer {QEDU_API_KEY}",
        "Accept": "application/json",
    }
    escolas = []
    page = 1
    while True:
        try:
            r = requests.get(
                "https://api.qedu.org.br/v1/escolas",
                params={"uf": uf, "per_page": 100, "page": page},
                headers=headers,
                timeout=30,
            )
            if r.status_code == 401:
                log.warning("  QEdu: chave inválida ou expirada")
                return []
            if r.status_code == 404 or r.status_code == 400:
                break
            r.raise_for_status()
            data = r.json()
            # QEdu pode retornar {"data": [...], "meta": {...}} ou lista direta
            items = data.get("data", data) if isinstance(data, dict) else data
            if not items:
                break
            for item in items:
                # Mapear campos QEdu → schema interno
                escola = {
                    "NO_ENTIDADE":   str(item.get("name", item.get("nome", ""))).strip(),
                    "CO_ENTIDADE":   str(item.get("inep_id", item.get("codigo_inep", item.get("id", "")))).strip(),
                    "SG_UF":         str(item.get("uf", item.get("estado", uf))).strip().upper(),
                    "NO_MUNICIPIO":  str(item.get("city", item.get("municipio", item.get("cidade", "")))).strip(),
                    "TP_DEPENDENCIA": int(item.get("dependence", item.get("dependencia", 3))),
                    "TP_LOCALIZACAO": int(item.get("location", item.get("localizacao", 1))),
                    "DS_ENDERECO":   str(item.get("address", item.get("endereco", ""))).strip(),
                    "NU_CEP":        str(item.get("zip", item.get("cep", ""))).replace("-", "").strip(),
                    "NU_TELEFONE":   str(item.get("phone", item.get("telefone", ""))).strip(),
                    "DS_SITE":       str(item.get("website", item.get("site", ""))).strip(),
                    "DS_EMAIL":      str(item.get("email", "")).strip(),
                    "IN_AEE":        False,
                    "IN_EDUCACAO_ESPECIAL": False,
                    "IN_NECESSIDADES_ESPECIAIS": False,
                    "IN_ACESSIBILIDADE_RAMPAS": bool(item.get("accessibility", item.get("acessibilidade", False))),
                    "IN_SALA_RECURSOS_MULTIFUNCIONAIS_TIPO_I": False,
                }
                escolas.append(escola)
            if limit and len(escolas) >= limit:
                escolas = escolas[:limit]
                break
            if len(items) < 100:
                break
            page += 1
        except Exception as e:
            log.warning(f"  QEdu {uf} pág {page}: {e}")
            break
    log.info(f"  QEdu {uf}: {len(escolas)} escolas")
    return escolas


def fetch_qedu(limit: int) -> list[dict]:
    """Busca todas as escolas via QEdu API (por UF)."""
    if not QEDU_API_KEY:
        log.info("QEdu API: QEDU_API_KEY não definida — pulando")
        return []
    log.info("=== Fonte 1: QEdu API ===")
    all_escolas = []
    for uf in ESTADOS:
        escolas = fetch_qedu_uf(uf, limit)
        # Filtro estrito de UF
        escolas = [e for e in escolas if e.get("SG_UF") == uf]
        all_escolas.extend(escolas)
    log.info(f"  QEdu total: {len(all_escolas)}")
    return all_escolas


# ─── fonte 2: dados.gov.br CKAN ──────────────────────────────────────────────

# Resource IDs conhecidos para o Catálogo de Escolas do INEP no CKAN
CKAN_RESOURCE_IDS = [
    os.environ.get("CKAN_RESOURCE_ID", ""),  # override por env var
    "d7a86cc1-5e28-4e40-87e0-4d6da1d4f4a0",  # Catálogo de Escolas - possível ID
    "8e30b0c6-c2e8-4e35-913a-d0b6a48e7d58",  # ID alternativo
]
CKAN_BASE = "https://dados.gov.br/api/action/datastore_search"


def fetch_ckan_uf(resource_id: str, uf: str, limit: int) -> list[dict]:
    """Busca escolas do CKAN dados.gov.br para um estado."""
    escolas = []
    offset = 0
    per_page = 100
    while True:
        try:
            r = requests.get(
                CKAN_BASE,
                params={
                    "resource_id": resource_id,
                    "limit": per_page,
                    "offset": offset,
                    "filters": json.dumps({"SG_UF": uf}),
                },
                timeout=30,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            if not data.get("success"):
                return []
            records = data.get("result", {}).get("records", [])
            if not records:
                break
            for rec in records:
                cep_raw = str(rec.get("NU_CEP", rec.get("CO_CEP", ""))).replace("-", "").strip()
                escola = {
                    "NO_ENTIDADE":   str(rec.get("NO_ENTIDADE", "")).strip(),
                    "CO_ENTIDADE":   str(rec.get("CO_ENTIDADE", "")).strip(),
                    "SG_UF":         str(rec.get("SG_UF", uf)).strip().upper(),
                    "NO_MUNICIPIO":  str(rec.get("NO_MUNICIPIO", "")).strip(),
                    "TP_DEPENDENCIA": int(rec.get("TP_DEPENDENCIA", 3)),
                    "TP_LOCALIZACAO": int(rec.get("TP_LOCALIZACAO", 1)),
                    "DS_ENDERECO":   str(rec.get("DS_ENDERECO", "")).strip(),
                    "NU_CEP":        cep_raw,
                    "NU_TELEFONE":   str(rec.get("NU_TELEFONE", "")).strip(),
                    "DS_SITE":       str(rec.get("DS_SITE", "")).strip(),
                    "DS_EMAIL":      str(rec.get("DS_EMAIL", "")).strip(),
                    "IN_AEE":        str(rec.get("IN_AEE", "0")).strip() in ("1", "S", "Sim"),
                    "IN_EDUCACAO_ESPECIAL": str(rec.get("IN_EDUCACAO_ESPECIAL", "0")).strip() in ("1", "S"),
                    "IN_NECESSIDADES_ESPECIAIS": str(rec.get("IN_NECESSIDADES_ESPECIAIS", "0")).strip() in ("1", "S"),
                    "IN_ACESSIBILIDADE_RAMPAS": str(rec.get("IN_ACESSIBILIDADE_RAMPAS", "0")).strip() in ("1", "S"),
                    "IN_SALA_RECURSOS_MULTIFUNCIONAIS_TIPO_I": str(rec.get("IN_SALA_RECURSOS_MULTIFUNCIONAIS_TIPO_I", "0")).strip() in ("1", "S"),
                }
                escolas.append(escola)
            if limit and len(escolas) >= limit:
                escolas = escolas[:limit]
                break
            if len(records) < per_page:
                break
            offset += per_page
        except Exception as e:
            log.warning(f"  CKAN {uf} offset {offset}: {e}")
            break
    return escolas


def fetch_ckan(limit: int) -> list[dict]:
    """Tenta buscar escolas do CKAN dados.gov.br."""
    log.info("=== Fonte 2: dados.gov.br CKAN ===")
    # Descobrir resource_id válido
    working_rid = None
    for rid in CKAN_RESOURCE_IDS:
        if not rid:
            continue
        try:
            r = requests.get(CKAN_BASE, params={"resource_id": rid, "limit": 1}, timeout=10)
            if r.status_code == 200 and r.json().get("success"):
                working_rid = rid
                log.info(f"  CKAN resource_id válido: {rid}")
                break
        except Exception:
            continue
    if not working_rid:
        log.warning("  CKAN: nenhum resource_id válido encontrado — pulando")
        return []

    all_escolas = []
    for uf in ESTADOS:
        escolas = fetch_ckan_uf(working_rid, uf, limit)
        # Filtro estrito de UF
        escolas = [e for e in escolas if e.get("SG_UF") == uf]
        all_escolas.extend(escolas)
        log.info(f"  CKAN {uf}: {len(escolas)}")
    log.info(f"  CKAN total: {len(all_escolas)}")
    return all_escolas


# ─── fonte 3: ZIP microdados INEP ────────────────────────────────────────────

def probe_url(url: str, timeout: int = 20) -> bool:
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        try:
            r = requests.get(url, timeout=timeout, stream=True)
            r.close()
            return r.status_code == 200
        except Exception:
            return False


def download_zip(url: str, dest: Path) -> bool:
    try:
        log.info(f"  Baixando {url}")
        r = requests.get(url, timeout=600, stream=True)
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        if total:
            log.info(f"  Tamanho: {total / 1024 / 1024:.0f} MB")
        downloaded = 0
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):
                fh.write(chunk)
                downloaded += len(chunk)
                if total and downloaded % (100 * 1024 * 1024) < 4 * 1024 * 1024:
                    pct = 100 * downloaded // total
                    log.info(f"  {downloaded // 1024 // 1024} MB / {total // 1024 // 1024} MB ({pct}%)")
        log.info(f"  Download completo: {downloaded // 1024 // 1024} MB")
        return True
    except Exception as e:
        log.warning(f"  Download falhou: {e}")
        return False


def find_escolas_entry(zf: zipfile.ZipFile) -> str | None:
    names = zf.namelist()
    for candidate in names:
        upper = candidate.upper()
        if "ESCOLAS" in upper and upper.endswith(".CSV"):
            if "/DADOS/" in upper or "\\DADOS\\" in upper:
                return candidate
    for candidate in names:
        lower = candidate.lower()
        if "microdados_ed_basica" in lower and lower.endswith(".csv"):
            return candidate
    for candidate in names:
        upper = candidate.upper()
        if upper.endswith(".CSV") and ("/DADOS/" in upper or "\\DADOS\\" in upper):
            return candidate
    for candidate in names:
        if "ESCOLAS" in candidate.upper() and candidate.upper().endswith(".CSV"):
            return candidate
    return None


def bool_field(row: dict, key: str) -> bool:
    return str(row.get(key, "0")).strip() in ("1", "S", "Sim", "sim", "true", "True")


def get_uf(row: dict) -> str:
    sg = (row.get("SG_UF") or "").strip()
    if sg:
        return sg
    try:
        co = int(row.get("CO_UF") or 0)
        return CODE_TO_UF.get(co, "")
    except (ValueError, TypeError):
        return ""


def normalize_zip_row(row: dict) -> dict:
    uf = get_uf(row)
    cep = (row.get("NU_CEP") or row.get("CO_CEP") or "").replace("-", "").strip()
    aee = bool_field(row, "IN_AEE") or (str(row.get("TP_AEE") or "0").strip() not in ("0", ""))
    sala_recursos = (
        bool_field(row, "IN_SALA_RECURSOS_MULTIFUNCIONAIS_TIPO_I")
        or bool_field(row, "IN_SALA_ATENDIMENTO_ESPECIAL")
    )
    educacao_especial = (
        bool_field(row, "IN_EDUCACAO_ESPECIAL")
        or bool_field(row, "IN_NECESSIDADES_ESPECIAIS")
    )
    return {
        "NO_ENTIDADE":   (row.get("NO_ENTIDADE") or "").strip(),
        "CO_ENTIDADE":   (row.get("CO_ENTIDADE") or "").strip(),
        "SG_UF":         uf,
        "NO_MUNICIPIO":  (row.get("NO_MUNICIPIO") or "").strip(),
        "TP_DEPENDENCIA": int(row.get("TP_DEPENDENCIA") or 3),
        "TP_LOCALIZACAO": int(row.get("TP_LOCALIZACAO") or 1),
        "DS_ENDERECO":   (row.get("DS_ENDERECO") or "").strip(),
        "NU_CEP":        cep,
        "NU_TELEFONE":   "",
        "DS_SITE":       "",
        "DS_EMAIL":      "",
        "IN_AEE":                                  aee,
        "IN_EDUCACAO_ESPECIAL":                    educacao_especial,
        "IN_NECESSIDADES_ESPECIAIS":               bool_field(row, "IN_NECESSIDADES_ESPECIAIS"),
        "IN_ACESSIBILIDADE_RAMPAS":                bool_field(row, "IN_ACESSIBILIDADE_RAMPAS"),
        "IN_SALA_RECURSOS_MULTIFUNCIONAIS_TIPO_I": sala_recursos,
    }


def parse_csv_bytes(raw: bytes) -> list[dict]:
    for enc in ("latin-1", "utf-8-sig", "utf-8", "cp1252"):
        try:
            text = raw.decode(enc)
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            rows = [
                {k: v for k, v in row.items() if k in KEEP_FIELDS}
                for row in reader
            ]
            log.info(f"  {len(rows):,} registros (encoding: {enc})")
            return rows
        except UnicodeDecodeError:
            continue
    log.error("  Nenhum encoding funcionou")
    return []


def extract_escolas(zip_path: Path) -> list[dict]:
    try:
        with zipfile.ZipFile(zip_path) as zf:
            entry = find_escolas_entry(zf)
            if not entry:
                log.error("ESCOLAS.CSV não encontrado no ZIP")
                return []
            log.info(f"  Extraindo: {entry}")
            raw = zf.read(entry)
            return parse_csv_bytes(raw)
    except zipfile.BadZipFile as e:
        log.error(f"ZIP inválido: {e}")
        return []


def group_and_sample(rows: list[dict], limit: int) -> list[dict]:
    by_uf: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        uf = get_uf(row)
        if uf in UF_CODES:
            by_uf[uf].append(row)

    result = []
    for uf in ESTADOS:
        pool = by_uf.get(uf, [])
        chosen = pool if limit == 0 else pool[:limit]
        result.extend(normalize_zip_row(r) for r in chosen)
        log.info(f"  {uf}: {len(chosen):,} / {len(pool):,} escolas")
    return result


def fetch_inep_zip(limit: int) -> list[dict]:
    log.info("=== Fonte 3: ZIP Microdados INEP ===")
    _tmp_ctx = None
    if ZIP_CACHE:
        zip_path = ZIP_CACHE
        zip_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        _tmp_ctx = tempfile.TemporaryDirectory()
        zip_path = Path(_tmp_ctx.name) / f"censo_{CENSO_YEAR}.zip"

    try:
        # Cache local
        if zip_path.exists() and zip_path.stat().st_size > 1_000_000:
            log.info(f"  Usando ZIP em cache: {zip_path}")
            raw_rows = extract_escolas(zip_path)
            if raw_rows:
                return group_and_sample(raw_rows, limit)

        # Tentar download
        for url_tpl in INEP_ZIP_URLS:
            url = url_tpl.format(year=CENSO_YEAR)
            log.info(f"  Verificando: {url}")
            if not probe_url(url):
                log.warning("  Inacessível, próxima URL...")
                continue
            if download_zip(url, zip_path):
                raw_rows = extract_escolas(zip_path)
                if raw_rows:
                    return group_and_sample(raw_rows, limit)
                log.warning("  Nenhuma escola extraída, próxima URL...")
            zip_path.unlink(missing_ok=True)
    finally:
        if _tmp_ctx:
            _tmp_ctx.cleanup()

    log.warning("  ZIP INEP: todas as URLs falharam")
    return []


# ─── fonte 4: arquivo estático ───────────────────────────────────────────────

def fetch_static(limit: int) -> list[dict]:
    log.info("=== Fonte 4: Arquivo estático pré-gerado ===")
    if not STATIC_FILE.exists():
        log.warning(f"  {STATIC_FILE} não encontrado")
        return []

    with open(STATIC_FILE, encoding="utf-8") as fh:
        static = json.load(fh)

    raw_escolas = static.get("escolas", [])
    source_meta = static.get("metadata", {}).get("source", "static_file")

    # Agrupar por UF e aplicar limite
    by_uf: dict[str, list[dict]] = defaultdict(list)
    for e in raw_escolas:
        uf = e.get("SG_UF", "").strip()
        if uf in UF_CODES:
            by_uf[uf].append(e)

    result = []
    for uf in ESTADOS:
        pool = by_uf.get(uf, [])
        chosen = pool if limit == 0 else pool[:limit]
        # Garantir campos novos presentes
        for e in chosen:
            e.setdefault("NU_TELEFONE", "")
            e.setdefault("DS_SITE", "")
            e.setdefault("DS_EMAIL", "")
        result.extend(chosen)
        log.info(f"  {uf}: {len(chosen):,} / {len(pool):,}")

    log.info(f"  Estático total: {len(result)} escolas ({source_meta})")
    return result


# ─── validação final ─────────────────────────────────────────────────────────

def validate_and_filter(escolas: list[dict]) -> list[dict]:
    """Filtra escolas inválidas e valida estritamente por UF."""
    valid = []
    discarded_no_name = 0
    discarded_no_code = 0
    discarded_bad_uf = 0
    uf_counts: dict[str, int] = defaultdict(int)

    for e in escolas:
        nome = e.get("NO_ENTIDADE", "").strip()
        code = e.get("CO_ENTIDADE", "").strip()
        uf = e.get("SG_UF", "").strip()

        if not nome:
            discarded_no_name += 1
            continue
        if not code:
            discarded_no_code += 1
            continue
        if uf not in UF_CODES:
            discarded_bad_uf += 1
            continue

        valid.append(e)
        uf_counts[uf] += 1

    if discarded_no_name or discarded_no_code or discarded_bad_uf:
        log.warning(
            f"  Descartadas — sem nome: {discarded_no_name}, "
            f"sem código: {discarded_no_code}, UF inválida: {discarded_bad_uf}"
        )

    # Verificar se algum estado tem muito mais que o esperado (indício de mistura de UFs)
    for uf, count in uf_counts.items():
        if LIMIT_PER_UF > 0 and count > LIMIT_PER_UF * 1.1:
            log.warning(f"  {uf}: {count} escolas (acima do limite {LIMIT_PER_UF}) — verifique filtro de UF")

    return valid


# ─── main ────────────────────────────────────────────────────────────────────

def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"=== Censo Escolar INEP {CENSO_YEAR} | limite/UF: {LIMIT_PER_UF or 'ilimitado'} ===")

    source = f"INEP Microdados Censo Escolar {CENSO_YEAR}"
    escolas: list[dict] = []

    # 1. QEdu API
    if not escolas:
        escolas = fetch_qedu(LIMIT_PER_UF)
        if escolas:
            source = f"QEdu API (Censo {CENSO_YEAR})"

    # 2. dados.gov.br CKAN
    if not escolas:
        escolas = fetch_ckan(LIMIT_PER_UF)
        if escolas:
            source = f"dados.gov.br CKAN (Censo {CENSO_YEAR})"

    # 3. ZIP INEP
    if not escolas:
        escolas = fetch_inep_zip(LIMIT_PER_UF)
        if escolas:
            source = f"INEP Microdados Censo Escolar {CENSO_YEAR}"

    # 4. Arquivo estático
    if not escolas:
        escolas = fetch_static(LIMIT_PER_UF)
        if escolas:
            source = "INEP Microdados 2023 (arquivo estático)"

    # NUNCA usar dados fictícios — falhar se não houver dados reais
    if not escolas:
        log.error(
            "ERRO FATAL: Nenhuma fonte de dados retornou escolas reais.\n"
            "Verifique: QEDU_API_KEY, acesso ao CKAN, acesso ao ZIP INEP,\n"
            f"ou adicione o arquivo {STATIC_FILE} ao repositório."
        )
        sys.exit(1)

    # Validação e deduplicação
    log.info(f"Antes da validação: {len(escolas):,} escolas")
    escolas = validate_and_filter(escolas)
    escolas = deduplicate(escolas)
    log.info(f"Após validação/dedup: {len(escolas):,} escolas")

    # Estatísticas por UF
    uf_stats = defaultdict(int)
    for e in escolas:
        uf_stats[e.get("SG_UF", "?")] += 1
    log.info("UF: " + ", ".join(f"{uf}:{uf_stats[uf]}" for uf in ESTADOS if uf in uf_stats))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        json.dump({
            "metadata": {
                "source":       source,
                "fetched_at":   datetime.utcnow().isoformat(),
                "total":        len(escolas),
                "estados":      ESTADOS,
                "limit_per_uf": LIMIT_PER_UF,
                "censo_year":   CENSO_YEAR,
            },
            "escolas": escolas,
        }, fh, ensure_ascii=False, indent=2)

    log.info(f"Salvo em {OUTPUT_FILE}")
    return len(escolas)


if __name__ == "__main__":
    sys.exit(0 if main() > 0 else 1)
