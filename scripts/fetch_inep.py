#!/usr/bin/env python3
"""
Brasil Escolas — Script 1: Fetch escolas do Censo Escolar INEP
Baixa o ZIP de microdados do INEP (ESCOLAS.CSV) e salva em JSON.

Variáveis de ambiente:
  DATA_DIR       Diretório de dados (padrão: data)
  CENSO_YEAR     Ano do censo (padrão: 2023)
  LIMIT_PER_UF   Limite de escolas por estado (padrão: 200; 0 = sem limite)
"""
import csv
import io
import json
import logging
import os
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR     = Path(os.environ.get("DATA_DIR", "data"))
OUTPUT_FILE  = DATA_DIR / "escolas_raw.json"
# Se definido, o ZIP é salvo/lido deste caminho (permite cache no CI)
ZIP_CACHE    = Path(os.environ.get("INEP_ZIP_CACHE", "")) if os.environ.get("INEP_ZIP_CACHE") else None
CENSO_YEAR   = int(os.environ.get("CENSO_YEAR", "2023"))
LIMIT_PER_UF = int(os.environ.get("LIMIT_PER_UF", "200"))

# Candidatos de URL — tenta em ordem até um funcionar
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


# ─── download ────────────────────────────────────────────────────────────────

def probe_url(url: str, timeout: int = 20) -> bool:
    """Retorna True se a URL responde com 200."""
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
    """Baixa a URL em streaming para dest. Retorna True se OK."""
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


# ─── extração ────────────────────────────────────────────────────────────────

def find_escolas_entry(zf: zipfile.ZipFile) -> str | None:
    """Localiza o arquivo ESCOLAS.CSV dentro do ZIP."""
    names = zf.namelist()
    # Prioridade: arquivo com ESCOLAS no nome dentro de pasta DADOS
    for candidate in names:
        upper = candidate.upper()
        if "ESCOLAS" in upper and upper.endswith(".CSV"):
            if "/DADOS/" in upper or "\\DADOS\\" in upper:
                return candidate
    # Fallback: qualquer arquivo com ESCOLAS no nome
    for candidate in names:
        if "ESCOLAS" in candidate.upper() and candidate.upper().endswith(".CSV"):
            return candidate
    return None


def parse_csv_bytes(raw: bytes) -> list[dict]:
    """Tenta decodificar e parsear o CSV com diferentes encodings."""
    for enc in ("latin-1", "utf-8-sig", "utf-8", "cp1252"):
        try:
            text = raw.decode(enc)
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            rows = list(reader)
            log.info(f"  {len(rows):,} registros (encoding: {enc})")
            return rows
        except UnicodeDecodeError:
            continue
    log.error("  Nenhum encoding funcionou")
    return []


def extract_escolas(zip_path: Path) -> list[dict]:
    """Abre o ZIP e retorna os registros de ESCOLAS.CSV."""
    try:
        with zipfile.ZipFile(zip_path) as zf:
            entry = find_escolas_entry(zf)
            if not entry:
                log.error(f"ESCOLAS.CSV não encontrado. Arquivos disponíveis:")
                for n in zf.namelist()[:30]:
                    log.error(f"  {n}")
                return []
            log.info(f"  Extraindo: {entry}")
            raw = zf.read(entry)
            return parse_csv_bytes(raw)
    except zipfile.BadZipFile as e:
        log.error(f"ZIP inválido: {e}")
        return []


# ─── normalização ────────────────────────────────────────────────────────────

def get_uf(row: dict) -> str:
    """Extrai a sigla UF do registro (microdata usa CO_UF numérico)."""
    sg = (row.get("SG_UF") or "").strip()
    if sg:
        return sg
    try:
        co = int(row.get("CO_UF") or 0)
        return CODE_TO_UF.get(co, "")
    except (ValueError, TypeError):
        return ""


def bool_field(row: dict, key: str) -> bool:
    return str(row.get(key, "0")).strip() in ("1", "S", "Sim", "sim", "true", "True")


def normalize_row(row: dict) -> dict:
    uf = get_uf(row)
    return {
        "NO_ENTIDADE":  (row.get("NO_ENTIDADE") or "").strip(),
        "CO_ENTIDADE":  (row.get("CO_ENTIDADE") or "").strip(),
        "SG_UF":        uf,
        "NO_MUNICIPIO": (row.get("NO_MUNICIPIO") or "").strip(),
        "TP_DEPENDENCIA": int(row.get("TP_DEPENDENCIA") or 3),
        "TP_LOCALIZACAO": int(row.get("TP_LOCALIZACAO") or 1),
        "DS_ENDERECO":  (row.get("DS_ENDERECO") or "").strip(),
        "NU_CEP":       (row.get("NU_CEP") or "").replace("-", "").strip(),
        "IN_AEE":                                    bool_field(row, "IN_AEE"),
        "IN_EDUCACAO_ESPECIAL":                      bool_field(row, "IN_EDUCACAO_ESPECIAL"),
        "IN_NECESSIDADES_ESPECIAIS":                 bool_field(row, "IN_NECESSIDADES_ESPECIAIS"),
        "IN_ACESSIBILIDADE_RAMPAS":                  bool_field(row, "IN_ACESSIBILIDADE_RAMPAS"),
        "IN_SALA_RECURSOS_MULTIFUNCIONAIS_TIPO_I":   bool_field(row, "IN_SALA_RECURSOS_MULTIFUNCIONAIS_TIPO_I"),
    }


# ─── agrupamento / amostragem ────────────────────────────────────────────────

def group_and_sample(rows: list[dict], limit: int) -> list[dict]:
    """Agrupa por UF, aplica limite e normaliza."""
    by_uf: dict[str, list[dict]] = {}
    for row in rows:
        uf = get_uf(row)
        if uf in UF_CODES:
            by_uf.setdefault(uf, []).append(row)

    result = []
    for uf in ESTADOS:
        pool = by_uf.get(uf, [])
        chosen = pool if limit == 0 else pool[:limit]
        result.extend(normalize_row(r) for r in chosen)
        log.info(f"  {uf}: {len(chosen):,} / {len(pool):,} escolas")

    return result


# ─── dados de exemplo ────────────────────────────────────────────────────────

def sample_schools(n_per_uf: int = 5) -> list[dict]:
    escolas = []
    for uf in ESTADOS:
        for i in range(1, n_per_uf + 1):
            escolas.append({
                "NO_ENTIDADE":       f"Escola Estadual {uf} {i:04d}",
                "CO_ENTIDADE":       f"{UF_CODES[uf]}{i:06d}",
                "SG_UF":             uf,
                "NO_MUNICIPIO":      f"Municipio {uf}-{i}",
                "TP_DEPENDENCIA":    2,
                "TP_LOCALIZACAO":    1,
                "DS_ENDERECO":       f"Rua Principal, {i * 100}",
                "NU_CEP":            f"{UF_CODES[uf]:02d}{i:06d}",
                "IN_NECESSIDADES_ESPECIAIS": i % 3 == 0,
                "IN_AEE": False,
                "IN_EDUCACAO_ESPECIAL": False,
                "IN_ACESSIBILIDADE_RAMPAS": False,
                "IN_SALA_RECURSOS_MULTIFUNCIONAIS_TIPO_I": False,
            })
    return escolas


# ─── main ────────────────────────────────────────────────────────────────────

def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"=== Censo Escolar INEP {CENSO_YEAR} | limite/UF: {LIMIT_PER_UF or 'ilimitado'} ===")

    source = f"INEP Microdados Censo Escolar {CENSO_YEAR}"
    escolas: list[dict] = []

    # Usa cache persistente (para CI) ou diretório temporário
    _tmp_ctx = None
    if ZIP_CACHE:
        zip_path = ZIP_CACHE
        zip_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        _tmp_ctx = tempfile.TemporaryDirectory()
        zip_path = Path(_tmp_ctx.name) / f"censo_{CENSO_YEAR}.zip"

    try:
        # Se já existe no cache, tenta usá-lo direto
        if zip_path.exists() and zip_path.stat().st_size > 1_000_000:
            log.info(f"Usando ZIP em cache: {zip_path} ({zip_path.stat().st_size // 1024 // 1024} MB)")
            raw_rows = extract_escolas(zip_path)
            if raw_rows:
                log.info(f"Total bruto: {len(raw_rows):,} registros")
                escolas = group_and_sample(raw_rows, LIMIT_PER_UF)

        # Tentar cada URL candidata
        if not escolas:
            for url_tpl in INEP_ZIP_URLS:
                url = url_tpl.format(year=CENSO_YEAR)
                log.info(f"Verificando: {url}")
                if not probe_url(url):
                    log.warning("  Inacessível, tentando próxima URL...")
                    continue
                if download_zip(url, zip_path):
                    raw_rows = extract_escolas(zip_path)
                    if raw_rows:
                        log.info(f"Total bruto: {len(raw_rows):,} registros")
                        escolas = group_and_sample(raw_rows, LIMIT_PER_UF)
                        break
                    else:
                        log.warning("  Nenhuma escola extraída, tentando próxima URL...")
                zip_path.unlink(missing_ok=True)
    finally:
        if _tmp_ctx:
            _tmp_ctx.cleanup()

    if not escolas:
        log.warning("Download do INEP falhou em todas as URLs. Usando dados de exemplo.")
        escolas = sample_schools(5)
        source = "sample_data"

    log.info(f"Total final: {len(escolas):,} escolas ({source})")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        json.dump({
            "metadata": {
                "source":        source,
                "fetched_at":    datetime.utcnow().isoformat(),
                "total":         len(escolas),
                "estados":       ESTADOS,
                "limit_per_uf":  LIMIT_PER_UF,
                "censo_year":    CENSO_YEAR,
            },
            "escolas": escolas,
        }, fh, ensure_ascii=False, indent=2)

    log.info(f"Salvo em {OUTPUT_FILE}")
    return len(escolas)


if __name__ == "__main__":
    sys.exit(0 if main() > 0 else 1)
