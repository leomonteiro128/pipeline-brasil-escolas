#!/usr/bin/env python3
"""
enriquecer_escolas_rfb.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Enriquece dados de escolas no Supabase cruzando duas fontes oficiais:

  • INEP — Censo Escolar 2023 (nome, município, UF, CEP)
  • Receita Federal — Cadastro de CNPJs (endereço completo, telefone)

via Base dos Dados (BigQuery público) + fuzzy matching por nome/município.

Variáveis de ambiente obrigatórias:
  SUPABASE_URL            URL do projeto Supabase
  SUPABASE_SERVICE_KEY    chave service_role (para PATCH)
  GCP_PROJECT_ID          ID do projeto Google Cloud
  GOOGLE_CREDENTIALS_JSON JSON da service account GCP (conteúdo do arquivo)

Variáveis opcionais:
  UF          processa apenas este estado (ex: SP). Vazio = todos
  FUZZY_MIN   pontuação mínima de similaridade 0-100 (default: 72)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import os, sys, json, unicodedata, tempfile, time
from pathlib import Path

import requests

# ── utilidades do próprio pipeline ──────────────────────────────────
SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPTS_DIR))
from utils import (
    formatar_cep,
    formatar_telefone,
    atualizar_supabase,
)

# ── configurações ────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
GCP_PROJECT  = os.environ["GCP_PROJECT_ID"]
BATCH_UF     = os.environ.get("UF", "").strip().upper()
FUZZY_MIN    = int(os.environ.get("FUZZY_MIN", "72"))

HEADERS_SB = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

# prefixos a ignorar no fuzzy match
PREFIXOS = [
    "ESCOLA ESTADUAL ", "ESCOLA MUNICIPAL ", "ESCOLA PUBLICA ",
    "ESCOLA ", "COLEGIO ", "CENTRO EDUCACIONAL ", "CENTRO DE ENSINO ",
    "INSTITUTO ", "EDUCANDARIO ", "GRUPO ESCOLAR ",
]


# ════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════

def setup_gcp():
    """Escreve as credenciais GCP em arquivo temporário e configura env."""
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json:
        raise EnvironmentError("GOOGLE_CREDENTIALS_JSON não está definido.")
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    tmp.write(creds_json)
    tmp.close()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name
    return tmp.name


def normalizar(texto: str) -> str:
    """Remove acentos, padroniza maiúsculas e remove prefixos comuns."""
    t = str(texto or "").upper().strip()
    # remover acentos
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    # remover prefixos
    for p in PREFIXOS:
        if t.startswith(p):
            t = t[len(p):]
            break
    return t.strip()


def fuzzy_score(a: str, b: str) -> int:
    """token_sort_ratio entre dois strings normalizados."""
    from rapidfuzz.fuzz import token_sort_ratio
    return int(token_sort_ratio(a, b))


def fone(ddd, tel) -> str | None:
    if not ddd or not tel:
        return None
    ddd = str(ddd).strip().zfill(2)
    tel = str(tel).strip().replace("-", "").replace(" ", "")
    if len(tel) == 8:
        tel = f"{tel[:4]}-{tel[4:]}"
    elif len(tel) == 9:
        tel = f"{tel[:5]}-{tel[5:]}"
    else:
        return None
    return f"({ddd}) {tel}"


def cep_fmt(cep) -> str | None:
    if not cep:
        return None
    c = str(cep).strip().replace("-", "").replace(".", "").zfill(8)
    return f"{c[:5]}-{c[5:]}" if len(c) == 8 else None


def cnpj_fmt(cnpj) -> str | None:
    if not cnpj:
        return None
    c = str(cnpj).strip().replace(".", "").replace("/", "").replace("-", "").zfill(14)
    if len(c) != 14:
        return None
    return f"{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:]}"


# ════════════════════════════════════════════════════════════════════
# BIGQUERY — download de dados
# ════════════════════════════════════════════════════════════════════

def baixar_inep(bd, ufs: list | None = None):
    filtro_uf = (
        f"AND sigla_uf IN ({', '.join(repr(u) for u in ufs)})"
        if ufs else ""
    )
    sql = f"""
    SELECT
        id_escola,
        nome,
        sigla_uf,
        id_municipio,
        rede,
        cep
    FROM `basedosdados.br_inep_censo_escolar.escola`
    WHERE ano = 2023
      AND tipo_situacao_funcionamento = '1'
      {filtro_uf}
    """
    print("📥 Baixando INEP Censo Escolar 2023…")
    df = bd.read_sql(query=sql, billing_project_id=GCP_PROJECT)
    print(f"   → {len(df):,} escolas carregadas")
    return df


def baixar_rfb(bd, ufs: list | None = None):
    filtro_uf = (
        f"AND sigla_uf IN ({', '.join(repr(u) for u in ufs)})"
        if ufs else ""
    )
    sql = f"""
    SELECT
        cnpj,
        id_municipio,
        sigla_uf,
        nome_fantasia,
        razao_social,
        logradouro,
        numero,
        complemento,
        bairro,
        cep,
        ddd_1,
        telefone_1,
        ddd_2,
        telefone_2,
        cnae_fiscal_principal
    FROM `basedosdados.br_me_cnpj.estabelecimentos`
    WHERE cnae_fiscal_principal LIKE '85%'
      AND situacao_cadastral = '2'
      {filtro_uf}
    """
    print("📥 Baixando CNPJs Receita Federal (CNAE 85%)…")
    df = bd.read_sql(query=sql, billing_project_id=GCP_PROJECT)
    print(f"   → {len(df):,} registros carregados")
    return df


# ════════════════════════════════════════════════════════════════════
# SUPABASE
# ════════════════════════════════════════════════════════════════════

def buscar_escolas_incompletas(uf: str | None = None) -> list:
    """Retorna escolas ativas com telefone OU endereço NULL."""
    params = {
        "select": "id,nome,municipio,uf,cnpj,telefone,site,endereco,bairro,cep,email",
        "or":     "(telefone.is.null,endereco.is.null)",
        "status": "eq.ativo",
        "limit":  "5000",
        "order":  "id.asc",
    }
    if uf:
        params["uf"] = f"eq.{uf}"

    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/escolas",
        headers=HEADERS_SB,
        params=params,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def atualizar(escola_id: int, patch: dict) -> int:
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/escolas?id=eq.{escola_id}",
        headers=HEADERS_SB,
        json=patch,
        timeout=15,
    )
    return r.status_code


# ════════════════════════════════════════════════════════════════════
# MATCHING
# ════════════════════════════════════════════════════════════════════

def construir_indices(df_inep, df_rfb):
    """Pré-indexa os DataFrames para busca rápida por município."""
    # INEP: (nome_norm, uf) → id_municipio  (exato primeiro)
    inep_exato = {}
    for _, row in df_inep.iterrows():
        key = (normalizar(row["nome"]), str(row["sigla_uf"]))
        inep_exato[key] = str(row["id_municipio"])

    # INEP por UF para fuzzy fallback
    inep_por_uf = df_inep.groupby("sigla_uf")

    # RFB por município
    rfb_por_mun = df_rfb.groupby("id_municipio")

    return inep_exato, inep_por_uf, rfb_por_mun


def encontrar_mun_id(nome_escola, uf, inep_exato, inep_por_uf) -> str | None:
    # 1. Busca exata
    chave = (normalizar(nome_escola), uf)
    if chave in inep_exato:
        return inep_exato[chave]

    # 2. Fuzzy dentro do mesmo UF
    if uf not in inep_por_uf.groups:
        return None

    df_uf = inep_por_uf.get_group(uf)
    nome_n = normalizar(nome_escola)
    melhor_score, melhor_mun = 0, None

    for _, row in df_uf.iterrows():
        s = fuzzy_score(nome_n, normalizar(row["nome"]))
        if s > melhor_score:
            melhor_score = s
            melhor_mun   = str(row["id_municipio"])

    return melhor_mun if melhor_score >= FUZZY_MIN else None


def encontrar_cnpj(nome_escola, mun_id, rfb_por_mun):
    """Retorna o melhor registro RFB para a escola, ou (None, 0)."""
    if mun_id not in rfb_por_mun.groups:
        return None, 0

    df_mun = rfb_por_mun.get_group(mun_id)
    nome_n = normalizar(nome_escola)
    melhor_score, melhor_row = 0, None

    for _, row in df_mun.iterrows():
        candidato = normalizar(row.get("nome_fantasia") or row.get("razao_social") or "")
        s = fuzzy_score(nome_n, candidato)
        if s > melhor_score:
            melhor_score = s
            melhor_row   = row

    return melhor_row, melhor_score


def montar_patch(escola: dict, rfb_row) -> dict:
    """Retorna dict com campos a atualizar (só os que estão vazios no DB)."""
    patch = {}

    # telefone
    if not escola.get("telefone"):
        t = fone(rfb_row.get("ddd_1"), rfb_row.get("telefone_1"))
        if not t:
            t = fone(rfb_row.get("ddd_2"), rfb_row.get("telefone_2"))
        if t:
            patch["telefone"] = t

    # endereço
    if not escola.get("endereco"):
        partes = [
            str(rfb_row.get("logradouro") or "").strip(),
            str(rfb_row.get("numero")     or "").strip(),
            str(rfb_row.get("complemento") or "").strip(),
        ]
        end = ", ".join(p for p in partes if p and p.upper() not in ("", "S/N", "SN"))
        if end:
            patch["endereco"] = end

    if not escola.get("bairro") and rfb_row.get("bairro"):
        patch["bairro"] = str(rfb_row["bairro"]).strip().title()

    if not escola.get("cep"):
        c = cep_fmt(rfb_row.get("cep"))
        if c:
            patch["cep"] = c

    if not escola.get("cnpj"):
        c = cnpj_fmt(rfb_row.get("cnpj"))
        if c:
            patch["cnpj"] = c

    return patch


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════

def main():
    try:
        import basedosdados as bd
        from rapidfuzz import fuzz  # noqa: F401 (só para garantir import)
        import pandas as pd
    except ImportError as e:
        sys.exit(f"❌ Dependência faltando: {e}\n"
                 f"   pip install basedosdados rapidfuzz pandas")

    creds_file = setup_gcp()
    print(f"✅ GCP configurado | projeto: {GCP_PROJECT}")

    ufs = [BATCH_UF] if BATCH_UF else None
    label = BATCH_UF or "todos os estados"
    print(f"🗺  Escopo: {label}\n{'─' * 60}")

    # ── 1. Baixar dados externos ──────────────────────────────────
    df_inep = baixar_inep(bd, ufs)
    df_rfb  = baixar_rfb(bd, ufs)

    # ── 2. Buscar escolas do Supabase ─────────────────────────────
    escolas = buscar_escolas_incompletas(BATCH_UF or None)
    print(f"\n📋 Supabase: {len(escolas)} escolas para enriquecer\n{'─' * 60}")

    if not escolas:
        print("✅ Nenhuma escola incompleta encontrada. Nada a fazer.")
        os.unlink(creds_file)
        return

    # ── 3. Construir índices ──────────────────────────────────────
    print("🔧 Construindo índices de busca…")
    inep_exato, inep_por_uf, rfb_por_mun = construir_indices(df_inep, df_rfb)
    print("   Índices prontos.\n")

    # ── 4. Enriquecer ─────────────────────────────────────────────
    atualizadas = sem_match = ja_completas = 0

    for i, escola in enumerate(escolas, 1):
        nome = escola["nome"]
        uf   = escola["uf"]

        print(f"[{i:04d}/{len(escolas):04d}] {nome} — {escola['municipio']}/{uf}")

        # Achar município no INEP
        mun_id = encontrar_mun_id(nome, uf, inep_exato, inep_por_uf)
        if not mun_id:
            print("         ⚠ Sem match no INEP")
            sem_match += 1
            continue

        # Achar CNPJ no RFB
        rfb_row, score = encontrar_cnpj(nome, mun_id, rfb_por_mun)
        if rfb_row is None or score < FUZZY_MIN:
            print(f"         ℹ Sem match RFB (melhor score={score})")
            sem_match += 1
            continue

        rfb_nome = str(rfb_row.get("nome_fantasia") or rfb_row.get("razao_social") or "")
        print(f"         ✓ Match: {rfb_nome[:50]} (score={score})")

        patch = montar_patch(escola, rfb_row)
        if not patch:
            print("         ℹ Campos já preenchidos")
            ja_completas += 1
            continue

        status = atualizar(escola["id"], patch)
        print(f"         ✅ {list(patch.keys())} → HTTP {status}")
        atualizadas += 1

    # ── 5. Resumo ─────────────────────────────────────────────────
    print()
    print("═" * 60)
    print(f"✅  Enriquecidas : {atualizadas}")
    print(f"ℹ️   Já completas : {ja_completas}")
    print(f"⚠️   Sem match   : {sem_match}")
    print(f"📋  Total        : {len(escolas)}")
    print("═" * 60)

    try:
        os.unlink(creds_file)
    except OSError:
        pass


if __name__ == "__main__":
    main()
